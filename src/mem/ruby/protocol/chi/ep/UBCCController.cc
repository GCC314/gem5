#include "mem/ruby/protocol/chi/ep/UBCCController.hh"

#include <cstdio>
#include <cstring>
#include <sstream>

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "debug/RubyEP.hh"
#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"
#include "mem/ruby/system/RubySystem.hh"
#include "sim/cur_tick.hh"

namespace gem5
{

namespace ruby
{

// Static registry for cross-node UBCC routing
std::map<int, UBCCController*> UBCCController::_instances;

void
UBCCController::registerInstance(int node_id, UBCCController *ubcc)
{
    _instances[node_id] = ubcc;
}

UBCCController*
UBCCController::getInstance(int node_id)
{
    auto it = _instances.find(node_id);
    return (it != _instances.end()) ? it->second : nullptr;
}

UBCCController::UBCCController(int node_id, RubySystem *ruby_system)
  : _nodeId(node_id),
    _interconnectLatency(200),
    _recallCount(0),
    _recallResponseCount(0),
    _writebackCount(0),
    _evictCount(0),
    _staleRejectedCount(0),
    _ownerMismatchRejectedCount(0),
    _invalidationCount(0),
    _invalidationAckCount(0),
    _epRnfSnoopCount(0),
    _dsmLocalBase(0),
    _dsmSegSize(0)
{
    // Precompute DSM local range for isDsmAddr()
    // Hardcoded prototype constants: num_nodes=3, segSize=128MB, NODE_ADDR_SHIFT=40
    constexpr uint64_t kSegSize = 128ULL * 1024 * 1024;
    constexpr int kNodeAddrShift = 40;
    uint64_t nodeBase = static_cast<uint64_t>(node_id) << kNodeAddrShift;
    _dsmLocalBase = nodeBase + 2 * kSegSize + node_id * kSegSize;
    _dsmSegSize = kSegSize;

    registerInstance(node_id, this);
}

UBCCController::~UBCCController()
{
    _instances.erase(_nodeId);
}

void
UBCCController::wakeup()
{
    // v4: Clean up expired tombstones on each wakeup
    cleanupTombstones();
}

// ---- isDsmAddr (pure computation, no SentinelHelper) ----

bool
UBCCController::isDsmAddr(uint64_t pa) const
{
    return pa >= _dsmLocalBase && pa < _dsmLocalBase + _dsmSegSize;
}

// ---- M5: Home UBCC MESI Grant Decision ----

void
UBCCController::ensureDirEntry(uint64_t line_pa)
{
    if (_directory.find(line_pa) == _directory.end()) {
        DirEntry entry;
        entry.lineAddr = line_pa;
        _directory[line_pa] = entry;
    }
}

const char*
UBCCController::mesiStateName(MESIState s) const
{
    switch (s) {
        case MESIState::G_I: return "G_I";
        case MESIState::G_S: return "G_S";
        case MESIState::G_E: return "G_E";
        case MESIState::G_M: return "G_M";
        default: return "UNKNOWN";
    }
}

UBCC_OuterGrantType
UBCCController::processOuterRequest(
    uint64_t line_pa, UBCC_OuterReqType reqType, bool writeIntent,
    int requesterNode,
    uint64_t baseEpoch, uint64_t reqId,
    Tick *outGrantVisibleTick, Tick *outSentinelVisibleTick,
    bool *outRecallNeeded, int *outRecallOwnerNode,
    GrantDataSource *outDataSource)
{
    DPRINTF(RubyCHIGeneric,
            "UBCC node_id=%d: processOuterRequest PA=0x%lx req=%d write=%d "
            "requesterNode=%d baseEpoch=%lu reqId=%lu\n",
            _nodeId, line_pa, static_cast<int>(reqType), writeIntent,
            requesterNode, baseEpoch, reqId);

    // Initialize M6 recall outputs and F3 dataSource output
    if (outRecallNeeded)   *outRecallNeeded = false;
    if (outRecallOwnerNode) *outRecallOwnerNode = -1;
    if (outDataSource) *outDataSource = GrantDataSource::HomeMemory;

    // Validate: only DSM addresses for this home node
    if (!isDsmAddr(line_pa)) {
        fatal("UBCC node_id=%d: non-home-DSM address PA=0x%lx in outer request\n",
              _nodeId, line_pa);
    }

    // Validate: Shared + true is illegal
    if (reqType == UBCC_OuterReqType::GlobalReadShared && writeIntent) {
        fatal("UBCC node_id=%d: illegal Shared+writeIntent=true for PA=0x%lx\n",
              _nodeId, line_pa);
    }

    // Validate requesterNode
    if (requesterNode < -1 || requesterNode >= 64) {
        fatal("UBCC node_id=%d: requesterNode=%d out of range\n",
              _nodeId, requesterNode);
    }

    ensureDirEntry(line_pa);
    DirEntry &entry = _directory[line_pa];

    // v4: Check for existing outstanding — if active, return BUSY
    OutstandingRequest *existing = findOutstanding(line_pa);
    if (existing) {
        // v4 D-19: Keep DONE outstanding for recall→grant transition.
        // Do NOT remove here — let G_E/G_M retry path detect and convert it.
        if (existing->stage != OpStage::DONE &&
            existing->stage != OpStage::CANCELLED &&
            existing->stage != OpStage::TIMED_OUT) {
            DPRINTF(RubyEP,
                    "UBCC node_id=%d: existing outstanding PA=0x%lx "
                    "opType=%d stage=%d — BUSY\n",
                    _nodeId, line_pa,
                    static_cast<int>(existing->opType),
                    static_cast<int>(existing->stage));
            return static_cast<UBCC_OuterGrantType>(-1);
        }
        // RECALL.DONE or other terminal — keep in map, let case blocks handle transition
    }

    // v4: Check tombstone for duplicate Clear within window W
    bool tsAccepted = false;
    if (checkTombstone(line_pa, entry.epoch, reqId, tsAccepted)) {
        // Already committed — return idempotent grant
        DPRINTF(RubyEP,
                "UBCC node_id=%d: tombstone HIT for PA=0x%lx — idempotent grant\n",
                _nodeId, line_pa);
        Tick now = curTick();
        if (outGrantVisibleTick) *outGrantVisibleTick = now;
        if (outSentinelVisibleTick) *outSentinelVisibleTick = now;
        if (outDataSource) *outDataSource = GrantDataSource::HomeMemory; // F3: conservative
        return UBCC_OuterGrantType::GlobalGrantShared; // conservative
    }

    // Record grant-visible tick
    Tick grantVisibleTick = curTick();
    Tick sentinelVisibleTick = curTick();

    // v4: Allocate reserved epoch (committed epoch + 1, NOT committed yet)
    uint64_t reservedEpoch = allocateReservedEpoch(entry);

    UBCC_OuterGrantType grant = UBCC_OuterGrantType::GlobalGrantShared;
    MESIState prevState = entry.state;

    // Determine intended result based on current committed state + request
    OutstandingRequest *oreq = nullptr;

    switch (entry.state) {
        case MESIState::G_I: {
            if (reqType == UBCC_OuterReqType::GlobalReadShared) {
                grant = UBCC_OuterGrantType::GlobalGrantShared;
                // Intended: G_S, sharers+=req, no owner
                oreq = createOutstanding(line_pa, OpType::GRANT_HANDSHAKE,
                                         requesterNode, -1);
                if (oreq) {
                    oreq->reservedEpoch = reservedEpoch;
                    oreq->reqId = reqId;
                    oreq->baseEpoch = baseEpoch;
                    oreq->stage = OpStage::WAITING_CLEAR;
                    oreq->intendedState = MESIState::G_S;
                    oreq->intendedSharersMask = (1ULL << requesterNode);
                    oreq->intendedOwnerNode = -1;
                    oreq->intendedDirty = false;
                    oreq->dataSource = GrantDataSource::HomeMemory;
                    if (outDataSource) *outDataSource = GrantDataSource::HomeMemory;
                }
            } else { // GlobalReadUnique
                if (!writeIntent) {
                    grant = UBCC_OuterGrantType::GlobalGrantExclusive;
                    oreq = createOutstanding(line_pa, OpType::GRANT_HANDSHAKE,
                                             requesterNode, -1);
                    if (oreq) {
                        oreq->reservedEpoch = reservedEpoch;
                        oreq->reqId = reqId;
                        oreq->baseEpoch = baseEpoch;
                        oreq->stage = OpStage::WAITING_CLEAR;
                        oreq->intendedState = MESIState::G_E;
                        oreq->intendedSharersMask = 0;
                        oreq->intendedOwnerNode = requesterNode;
                        oreq->intendedDirty = false;
                        oreq->dataSource = GrantDataSource::HomeMemory;
                        if (outDataSource) *outDataSource = GrantDataSource::HomeMemory;
                    }
                } else {
                    grant = UBCC_OuterGrantType::GlobalGrantModified;
                    oreq = createOutstanding(line_pa, OpType::GRANT_HANDSHAKE,
                                             requesterNode, -1);
                    if (oreq) {
                        oreq->reservedEpoch = reservedEpoch;
                        oreq->reqId = reqId;
                        oreq->baseEpoch = baseEpoch;
                        oreq->stage = OpStage::WAITING_CLEAR;
                        oreq->intendedState = MESIState::G_M;
                        oreq->intendedSharersMask = 0;
                        oreq->intendedOwnerNode = requesterNode;
                        oreq->intendedDirty = true;
                        oreq->dataSource = GrantDataSource::HomeMemory;
                        if (outDataSource) *outDataSource = GrantDataSource::HomeMemory;
                    }
                }
            }
            break;
        }

        case MESIState::G_S: {
            if (reqType == UBCC_OuterReqType::GlobalReadShared) {
                grant = UBCC_OuterGrantType::GlobalGrantShared;
                oreq = createOutstanding(line_pa, OpType::GRANT_HANDSHAKE,
                                         requesterNode, -1);
                if (oreq) {
                    oreq->reservedEpoch = reservedEpoch;
                    oreq->reqId = reqId;
                    oreq->baseEpoch = baseEpoch;
                    oreq->stage = OpStage::WAITING_CLEAR;
                    oreq->intendedState = MESIState::G_S;
                    oreq->intendedSharersMask = entry.sharersMask | (1ULL << requesterNode);
                    oreq->intendedOwnerNode = -1;
                    oreq->intendedDirty = false;
                    oreq->dataSource = GrantDataSource::HomeMemory;
                    if (outDataSource) *outDataSource = GrantDataSource::HomeMemory;
                }
            } else {
                // Unique request — invalidation needed for non-requester sharers
                uint64_t otherSharers = entry.sharersMask;
                if (requesterNode >= 0)
                    otherSharers &= ~(1ULL << requesterNode);

                if (otherSharers != 0) {
                    // v4: Create INVALIDATE + GRANT_HANDSHAKE
                    // INVALIDATE outstanding
                    OutstandingRequest *invOreq = createOutstanding(
                        line_pa, OpType::INVALIDATE, requesterNode, -1);
                    if (invOreq) {
                        invOreq->reservedEpoch = reservedEpoch;
                        invOreq->reqId = reqId;
                        invOreq->baseEpoch = baseEpoch;
                        invOreq->stage = OpStage::WAITING_ALL_ACKS;
                        invOreq->targetMask = otherSharers;
                        invOreq->totalMask = otherSharers;
                        invOreq->pendingAckCount = __builtin_popcountll(otherSharers);
                        invOreq->ackMask = 0;
                        invOreq->writeIntent = writeIntent;
                        invOreq->intendedState = writeIntent ? MESIState::G_M : MESIState::G_E;
                        invOreq->intendedOwnerNode = requesterNode;
                        invOreq->intendedSharersMask = 0;
                        invOreq->intendedDirty = writeIntent;
                        invOreq->dataSource = GrantDataSource::HomeMemory; // F3
                    }
                    _invalidationCount++;
                    // Return BUSY — invalidation must complete before grant
                    return static_cast<UBCC_OuterGrantType>(-1);
                } else {
                    // No other sharers — immediate upgrade (self-upgrade)
                    // v4: This should use UPGRADE_PENDING path (§4.1.3 G_S row)
                    // For now, create GRANT_HANDSHAKE for the upgrade
                    grant = writeIntent
                        ? UBCC_OuterGrantType::GlobalGrantModified
                        : UBCC_OuterGrantType::GlobalGrantExclusive;
                    oreq = createOutstanding(line_pa, OpType::GRANT_HANDSHAKE,
                                             requesterNode, -1);
                    if (oreq) {
                        oreq->reservedEpoch = reservedEpoch;
                        oreq->reqId = reqId;
                        oreq->baseEpoch = baseEpoch;
                        oreq->stage = OpStage::WAITING_CLEAR;
                        oreq->intendedState = writeIntent ? MESIState::G_M : MESIState::G_E;
                        oreq->intendedSharersMask = 0;
                        oreq->intendedOwnerNode = requesterNode;
                        oreq->intendedDirty = writeIntent;
                        oreq->dataSource = GrantDataSource::HomeMemory;
                        if (outDataSource) *outDataSource = GrantDataSource::HomeMemory;
                    }
                }
            }
            break;
        }

        case MESIState::G_E:
        case MESIState::G_M: {
            int existingOwner = entry.ownerNode;
            bool wasDirty = (entry.state == MESIState::G_M);

            // v4: Check if there's an already-completed RECALL for this requester
            bool recallAlreadyDone = false;
            auto rit = _outstandingReqs.find(line_pa);
            if (rit != _outstandingReqs.end() &&
                rit->second.opType == OpType::RECALL &&
                rit->second.requesterNode == requesterNode &&
                rit->second.stage == OpStage::DONE) {
                recallAlreadyDone = true;

                // F2: RECALL and GRANT_HANDSHAKE are two separate lifecycle
                // objects.  Remove the terminal RECALL first, then create a
                // new GRANT_HANDSHAKE.  DO NOT mutate opType in place.
                OutstandingRequest recallData = rit->second;  // capture fields
                removeOutstanding(line_pa);  // free the PA slot

                uint64_t newSharers = (1ULL << requesterNode);
                if (existingOwner >= 0)
                    newSharers |= (1ULL << existingOwner);

                OutstandingRequest *grantOreq = createOutstanding(
                    line_pa, OpType::GRANT_HANDSHAKE,
                    requesterNode, -1);
                if (grantOreq) {
                    grantOreq->reservedEpoch = recallData.reservedEpoch;
                    grantOreq->reqId = recallData.reqId;
                    grantOreq->baseEpoch = recallData.baseEpoch;
                    grantOreq->stage = OpStage::WAITING_CLEAR;
                    grantOreq->recallBarrierDone = true;
                    // F2: Copy recall data from RECALL → GRANT_HANDSHAKE
                    grantOreq->dataValid = recallData.dataValid;
                    if (recallData.dataValid) {
                        memcpy(grantOreq->dataBuf, recallData.dataBuf, 64);
                    }
                    // F3: Data source is RecallBuffer since data came from recall
                    grantOreq->dataSource = GrantDataSource::RecallBuffer;
                    if (outDataSource) *outDataSource = GrantDataSource::RecallBuffer;
                    if (reqType == UBCC_OuterReqType::GlobalReadShared) {
                        grant = UBCC_OuterGrantType::GlobalGrantShared;
                        grantOreq->intendedState = MESIState::G_S;
                        grantOreq->intendedSharersMask = newSharers;
                        grantOreq->intendedOwnerNode = -1;
                        grantOreq->intendedDirty = false;
                    } else {
                        grant = writeIntent
                            ? UBCC_OuterGrantType::GlobalGrantModified
                            : UBCC_OuterGrantType::GlobalGrantExclusive;
                        grantOreq->intendedState = writeIntent
                            ? MESIState::G_M : MESIState::G_E;
                        grantOreq->intendedOwnerNode = requesterNode;
                        grantOreq->intendedSharersMask = 0;
                        grantOreq->intendedDirty = writeIntent;
                    }
                } else {
                    // Failed to create GRANT_HANDSHAKE — restore RECALL
                    // (should not happen since PA was freed above)
                    fatal("UBCC node_id=%d: failed to create GRANT_HANDSHAKE "
                          "after removing DONE RECALL PA=0x%lx\n",
                          _nodeId, line_pa);
                }
                DPRINTF(RubyEP,
                        "UBCC node_id=%d: RECALL→GRANT_HANDSHAKE transition "
                        "PA=0x%lx requester=%d intended=%s dataSource=RecallBuffer (NEW object)\n",
                        _nodeId, line_pa, requesterNode,
                        grantOreq ? mesiStateName(grantOreq->intendedState)
                                  : "none");
                return grant;
            }

            if (existingOwner >= 0 && existingOwner != requesterNode
                && !recallAlreadyDone) {
                // v4: Recall needed — create RECALL + GRANT_HANDSHAKE
                bool recallStarted = initiateRecall(
                    line_pa, entry, reqType, writeIntent, requesterNode);

                DPRINTF(RubyEP,
                        "UBCC node_id=%d: v4 recall initiated PA=0x%lx "
                        "existingOwner=%d requester=%d recallStarted=%d\n",
                        _nodeId, line_pa, existingOwner,
                        requesterNode, recallStarted);

                if (recallStarted) {
                    _recallCount++;
                    if (outRecallNeeded) *outRecallNeeded = true;
                    if (outRecallOwnerNode) *outRecallOwnerNode = existingOwner;
                    // F3: Data source for recall path is RecallBuffer
                    if (outDataSource) *outDataSource = GrantDataSource::RecallBuffer;

                    // Create RECALL outstanding
                    OutstandingRequest *recallOreq = createOutstanding(
                        line_pa, OpType::RECALL, requesterNode, existingOwner);
                    if (recallOreq) {
                        recallOreq->reservedEpoch = reservedEpoch;
                        recallOreq->reqId = reqId;
                        recallOreq->baseEpoch = baseEpoch;
                        recallOreq->stage = OpStage::WAITING_TARGET_RESP;
                        recallOreq->reqType = reqType;
                        recallOreq->writeIntent = writeIntent;
                        recallOreq->dataSource = GrantDataSource::RecallBuffer;
                    }
                    // Return BUSY — recall must complete before grant
                    return static_cast<UBCC_OuterGrantType>(-1);
                }
            }

            // Same owner or no recall — immediate grant
            if (reqType == UBCC_OuterReqType::GlobalReadShared) {
                grant = UBCC_OuterGrantType::GlobalGrantShared;
                oreq = createOutstanding(line_pa, OpType::GRANT_HANDSHAKE,
                                         requesterNode, -1);
                if (oreq) {
                    oreq->reservedEpoch = reservedEpoch;
                    oreq->reqId = reqId;
                    oreq->baseEpoch = baseEpoch;
                    oreq->stage = OpStage::WAITING_CLEAR;
                    oreq->intendedState = MESIState::G_S;
                    uint64_t newSharers = (1ULL << requesterNode);
                    if (existingOwner >= 0)
                        newSharers |= (1ULL << existingOwner);
                    oreq->intendedSharersMask = newSharers;
                    oreq->intendedOwnerNode = -1;
                    oreq->intendedDirty = false;
                    oreq->dataSource = GrantDataSource::HomeMemory;
                    if (outDataSource) *outDataSource = GrantDataSource::HomeMemory;
                }
            } else {
                grant = writeIntent
                    ? UBCC_OuterGrantType::GlobalGrantModified
                    : UBCC_OuterGrantType::GlobalGrantExclusive;
                oreq = createOutstanding(line_pa, OpType::GRANT_HANDSHAKE,
                                         requesterNode, -1);
                if (oreq) {
                    oreq->reservedEpoch = reservedEpoch;
                    oreq->reqId = reqId;
                    oreq->baseEpoch = baseEpoch;
                    oreq->stage = OpStage::WAITING_CLEAR;
                    oreq->intendedState = writeIntent ? MESIState::G_M : MESIState::G_E;
                    oreq->intendedSharersMask = 0;
                    oreq->intendedOwnerNode = requesterNode;
                    oreq->intendedDirty = writeIntent;
                    oreq->dataSource = GrantDataSource::HomeMemory;
                    if (outDataSource) *outDataSource = GrantDataSource::HomeMemory;
                }
            }
            break;
        }
    }

    // v4: §4.1.3 — SHALL NOT modify committed DirEntry here.
    // Committed DirEntry stays as-is until matching Clear is accepted.

    DPRINTF(RubyEP,
            "UBCC node_id=%d: v4 grant decision PA=0x%lx "
            "prev=%s intended_state=%s grant=%d reservedEpoch=%lu "
            "(committed DirEntry NOT modified)\n",
            _nodeId, line_pa,
            mesiStateName(prevState),
            oreq ? mesiStateName(oreq->intendedState) : "none",
            static_cast<int>(grant), reservedEpoch);

    if (outGrantVisibleTick)
        *outGrantVisibleTick = grantVisibleTick;
    if (outSentinelVisibleTick)
        *outSentinelVisibleTick = sentinelVisibleTick;

    return grant;
}

std::string
UBCCController::inspectUbccDirForTest(uint64_t line_pa)
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end()) {
        return "{\"error\": \"entry not found\"}";
    }

    const DirEntry &e = it->second;
    std::ostringstream oss;
    oss << "{"
        << "\"lineAddr\":\"0x" << std::hex << e.lineAddr << std::dec << "\","
        << "\"state\":\"" << mesiStateName(e.state) << "\","
        << "\"sharersMask\":" << e.sharersMask << ","
        << "\"ownerNode\":" << e.ownerNode << ","
        << "\"dirty\":" << (e.dirty ? "true" : "false") << ","
        << "\"epoch\":" << e.epoch << ","
        << "\"nextReqId\":" << e.nextReqId;

    // v4: Outstanding state sourced from OutstandingRequest
    auto oit = _outstandingReqs.find(line_pa);
    if (oit != _outstandingReqs.end()) {
        const auto &ost = oit->second;
        oss << ","
            << "\"ostOpType\":" << static_cast<int>(ost.opType) << ","
            << "\"ostStage\":" << static_cast<int>(ost.stage) << ","
            << "\"ostRequester\":" << ost.requesterNode << ","
            << "\"ostTarget\":" << ost.targetNode << ","
            << "\"ostReservedEpoch\":" << ost.reservedEpoch << ","
            << "\"ostReqId\":" << ost.reqId << ","
            << "\"ostDataSource\":" << static_cast<int>(ost.dataSource);  // F3
        if (ost.opType == OpType::INVALIDATE) {
            oss << ","
                << "\"pendingInvalidationCount\":" << ost.pendingAckCount << ","
                << "\"pendingInvalidationMask\":" << ost.totalMask << ","
                << "\"invalidatedAckMask\":" << ost.ackMask;
        }
    } else {
        oss << ","
            << "\"ostOpType\":-1";
    }

    // Counters for test observation
    oss << ","
        << "\"writebackCount\":" << _writebackCount << ","
        << "\"evictCount\":" << _evictCount << ","
        << "\"staleRejectedCount\":" << _staleRejectedCount << ","
        << "\"ownerMismatchRejectedCount\":" << _ownerMismatchRejectedCount << ","
        << "\"invalidationCount\":" << _invalidationCount << ","
        << "\"invalidationAckCount\":" << _invalidationAckCount;
    oss << "}";
    return oss.str();
}

bool
UBCCController::getUbccDirFieldsForTest(uint64_t line_pa,
    MESIState &outState, int &outOwnerNode,
    uint64_t &outSharersMask, bool &outDirty) const
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end()) {
        return false;
    }
    const DirEntry &e = it->second;
    outState = e.state;
    outOwnerNode = e.ownerNode;
    outSharersMask = e.sharersMask;
    outDirty = e.dirty;
    return true;
}

// ---- M6: Extended Directory Field Access (includes recall context) ----

bool
UBCCController::getUbccDirFieldsExtendedForTest(uint64_t line_pa,
    MESIState &outState, int &outOwnerNode,
    uint64_t &outSharersMask, bool &outDirty, bool &outBusy,
    int &outPendingRequester, int &outPendingRecallTarget) const
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end()) {
        return false;
    }
    const DirEntry &e = it->second;
    outState = e.state;
    outOwnerNode = e.ownerNode;
    outSharersMask = e.sharersMask;
    outDirty = e.dirty;
    outBusy = isLineBusy(line_pa);
    outPendingRequester = getPendingRequester(line_pa);
    outPendingRecallTarget = getPendingRecallTarget(line_pa);
    return true;
}

// ---- M6: Recall Management ----

bool
UBCCController::initiateRecall(uint64_t line_pa, DirEntry &entry,
    UBCC_OuterReqType reqType, bool writeIntent, int requesterNode)
{
    // v4: OutstandingRequest handles all recall state.
    // DirEntry remains unmodified until Clear/UpgradeDone.
    DPRINTF(RubyEP,
            "UBCC node_id=%d: initiateRecall PA=0x%lx "
            "ownerNode=%d requester=%d state=%s dirty=%d\n",
            _nodeId, line_pa, entry.ownerNode, requesterNode,
            mesiStateName(entry.state), entry.dirty);

    return true;
}

bool
UBCCController::processRecallResponse(uint64_t line_pa, int ownerNode,
                                       bool dataReceived, uint64_t responseEpoch,
                                       uint64_t reqId,
                                       const DataBlock *dataBlk)
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end()) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processRecallResponse PA=0x%lx "
                "entry not found\n", _nodeId, line_pa);
        return false;
    }

    DirEntry &entry = it->second;

    // v4: Half-range epoch check
    if (!checkEpochForLine(line_pa, responseEpoch)) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processRecallResponse PA=0x%lx "
                "STALE epoch: response=%lu directory=%lu — REJECTED\n",
                _nodeId, line_pa, responseEpoch, entry.epoch);
        _staleRejectedCount++;
        return false;
    }

    // v4: Verify pending recall via OutstandingRequest
    OutstandingRequest *ost = findOutstanding(line_pa);
    if (!ost || ost->opType != OpType::RECALL) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processRecallResponse PA=0x%lx "
                "no pending recall\n", _nodeId, line_pa);
        return false;
    }

    // Verify the recall target matches
    if (ost->targetNode >= 0 && ost->targetNode != ownerNode) {
        warn("UBCC node_id=%d: recall owner mismatch PA=0x%lx "
             "expected=%d got=%d — rejecting\n",
             _nodeId, line_pa, ost->targetNode, ownerNode);
        return false;
    }

    // Verify reqId matches
    if (ost->reqId != 0 && ost->reqId != reqId) {
        warn("UBCC node_id=%d: recall reqId mismatch PA=0x%lx "
             "expected=%lu got=%lu — rejecting\n",
             _nodeId, line_pa, ost->reqId, reqId);
        return false;
    }

    int requesterNode = ost->requesterNode;
    UBCC_OuterReqType reqType = ost->reqType;
    bool writeIntent = ost->writeIntent;

    DPRINTF(RubyEP,
            "UBCC node_id=%d: processRecallResponse PA=0x%lx "
            "ownerNode=%d dataReceived=%d requester=%d reqType=%d "
            "prevState=%s dirty=%d\n",
            _nodeId, line_pa, ownerNode, dataReceived,
            requesterNode, static_cast<int>(reqType),
            mesiStateName(entry.state), entry.dirty);

    // v4: Release recall barrier
    ost->recallBarrierDone = true;
    ost->stage = OpStage::DONE;
    ost->respTick = curTick();
    ost->dataValid = dataReceived;

    // F2: Preserve terminal tuple for GRANT_HANDSHAKE creation on retry:
    // (linePa, requesterNode, baseEpoch, reservedEpoch, reqId, opType=RECALL)
    // All fields except stage/dataValid/recallBarrierDone/respTick already
    // hold their terminal values and MUST NOT be mutated further.

    // F2: Store actual recall data in outstanding's dataBuf for later
    // transfer to GRANT_HANDSHAKE when it's created.
    if (dataBlk && dataReceived) {
        memcpy(ost->dataBuf, dataBlk->getData(0, 64), 64);
        ost->dataValid = true;
    }

    // v4: The directory transition occurs ONLY when the GRANT_HANDSHAKE
    // Clear arrives. Here we only release the recall barrier.
    _recallResponseCount++;

    DPRINTF(RubyEP,
            "UBCC node_id=%d: recall barrier released PA=0x%lx "
            "state=%s ownerNode=%d (DirEntry NOT modified — "
            "waiting for Clear to commit intended result)\n",
            _nodeId, line_pa,
            mesiStateName(entry.state), entry.ownerNode);

    return true;
}

bool
UBCCController::isLineBusy(uint64_t line_pa) const
{
    // v4: Check outstanding requests for non-terminal stages
    auto oit = _outstandingReqs.find(line_pa);
    if (oit != _outstandingReqs.end()) {
        switch (oit->second.stage) {
            case OpStage::DONE:
            case OpStage::CANCELLED:
            case OpStage::TIMED_OUT:
                break;  // Terminal stages — not busy
            default:
                return true;
        }
    }
    return false;
}

int
UBCCController::getPendingRequester(uint64_t line_pa) const
{
    auto oit = _outstandingReqs.find(line_pa);
    if (oit != _outstandingReqs.end())
        return oit->second.requesterNode;
    return -1;
}

int
UBCCController::getPendingRecallTarget(uint64_t line_pa) const
{
    auto oit = _outstandingReqs.find(line_pa);
    if (oit != _outstandingReqs.end() &&
        oit->second.opType == OpType::RECALL)
        return oit->second.targetNode;
    return -1;
}

// ---- M8: Global Invalidation Management ----

bool
UBCCController::processInvalidationAck(uint64_t line_pa, int ackNode,
                                        uint64_t responseEpoch,
                                        uint64_t reqId)
{
    // Validate ackNode boundaries
    if (ackNode < 0 || ackNode >= 64) {
        warn("UBCC node_id=%d: processInvalidationAck PA=0x%lx "
             "ackNode=%d out of range — REJECTED\n",
             _nodeId, line_pa, ackNode);
        return false;
    }

    auto it = _directory.find(line_pa);
    if (it == _directory.end()) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processInvalidationAck PA=0x%lx "
                "entry not found\n", _nodeId, line_pa);
        return false;
    }

    DirEntry &entry = it->second;

    // v4: Half-range epoch check
    if (!checkEpochForLine(line_pa, responseEpoch)) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processInvalidationAck PA=0x%lx "
                "STALE epoch: response=%lu directory=%lu — REJECTED\n",
                _nodeId, line_pa, responseEpoch, entry.epoch);
        _staleRejectedCount++;
        return false;
    }

    // v4: Verify pending invalidation via OutstandingRequest
    OutstandingRequest *ost = findOutstanding(line_pa);
    if (!ost || ost->opType != OpType::INVALIDATE) {
        // Already completed — idempotent
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processInvalidationAck PA=0x%lx "
                "no pending invalidation — idempotent\n",
                _nodeId, line_pa);
        return true;
    }

    // Check for duplicate ack
    uint64_t nodeBit = (1ULL << ackNode);
    if (!(ost->totalMask & nodeBit)) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processInvalidationAck PA=0x%lx "
                "ackNode=%d not in targetMask=0x%lx\n",
                _nodeId, line_pa, ackNode, ost->totalMask);
        return false;
    }

    if (ost->ackMask & nodeBit) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processInvalidationAck PA=0x%lx "
                "duplicate ack from node %d — ignoring\n",
                _nodeId, line_pa, ackNode);
        return true;
    }

    // Record the ack
    ost->ackMask |= nodeBit;
    ost->pendingAckCount--;
    // Clear the sharer bit from committed entry
    entry.sharersMask &= ~nodeBit;

    DPRINTF(RubyEP,
            "UBCC node_id=%d: invalidation ack PA=0x%lx ackNode=%d "
            "remaining=%d ackMask=0x%lx totalMask=0x%lx\n",
            _nodeId, line_pa, ackNode,
            ost->pendingAckCount, ost->ackMask, ost->totalMask);

    _invalidationAckCount++;

    // Check if all invalidations are complete
    if (ost->pendingAckCount == 0) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: all invalidations complete PA=0x%lx\n",
                _nodeId, line_pa);

        // v4: Release invalidate barrier
        ost->invalidateBarrierDone = true;
        ost->stage = OpStage::DONE;
        ost->respTick = curTick();

        // v4: Create GRANT_HANDSHAKE for the intended result.
        // Convert the INVALIDATE outstanding in-place to GRANT_HANDSHAKE
        // to avoid the create-then-remove race on the same linePa key.
        ost->opType = OpType::GRANT_HANDSHAKE;
        ost->stage = OpStage::WAITING_CLEAR;
        // intendedState, intendedOwnerNode, intendedSharersMask, intendedDirty
        // are already set from when the INVALIDATE was created.
        ost->recallBarrierDone = false;
        ost->invalidateBarrierDone = true;  // INVALIDATE is now DONE
    }

    return true;
}

int
UBCCController::getPendingInvalidationCount(uint64_t line_pa) const
{
    auto oit = _outstandingReqs.find(line_pa);
    if (oit != _outstandingReqs.end() &&
        oit->second.opType == OpType::INVALIDATE)
        return oit->second.pendingAckCount;
    return -1;
}

uint64_t
UBCCController::getPendingInvalidationMask(uint64_t line_pa) const
{
    auto oit = _outstandingReqs.find(line_pa);
    if (oit != _outstandingReqs.end() &&
        oit->second.opType == OpType::INVALIDATE)
        return oit->second.totalMask & ~oit->second.ackMask;
    return 0;
}

// ---- M7: Epoch / Stale Protection ----

bool
UBCCController::checkEpochForLine(uint64_t line_pa, uint64_t responseEpoch) const
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end())
        return true; // No entry yet — accept (first miss creates entry)

    // v4: Half-range epoch comparison (§3.1.2).
    // Reject if responseEpoch is older than committed epoch.
    // Accept if responseEpoch >= committed epoch (within half-range).
    // This handles wrap-around correctly.
    if (isNewerEpoch(it->second.epoch, responseEpoch)) {
        // committed epoch is newer than response → stale
        return false;
    }
    return true;
}

uint64_t
UBCCController::getEpochForLine(uint64_t line_pa) const
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end())
        return 0;
    return it->second.epoch;
}

// ---- M7: GlobalWriteback ----

bool
UBCCController::processWriteback(uint64_t line_pa, int requesterNode,
                                  uint64_t epochVal, bool keepAsClean)
{
    DPRINTF(RubyEP,
            "UBCC node_id=%d: processWriteback PA=0x%lx "
            "requesterNode=%d epoch=%lu keepAsClean=%d\n",
            _nodeId, line_pa, requesterNode, epochVal, keepAsClean);

    auto it = _directory.find(line_pa);
    if (it == _directory.end()) {
        // No directory entry — accept writeback (first registration)
        // This handles the case where data was cached with stale/incomplete metadata.
        ensureDirEntry(line_pa);
        it = _directory.find(line_pa);
    }

    DirEntry &entry = it->second;

    // v4: Outstanding-aware BUSY check (§4.6.2)
    if (isLineBusy(line_pa)) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processWriteback PA=0x%lx "
                "line busy (outstanding active) — BUSY/RETRY\n",
                _nodeId, line_pa);
        return false;
    }

    // ---- M7: Stale epoch check ----
    if (!checkEpochForLine(line_pa, epochVal)) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processWriteback PA=0x%lx "
                "STALE epoch: msg=%lu directory=%lu — REJECTED\n",
                _nodeId, line_pa, epochVal, entry.epoch);
        _staleRejectedCount++;
        return false;
    }

    // ---- M7: Owner match check ----
    // Writeback must come from the current owner (or -1 if no entry).
    // Reject if the requesting node is not the current owner.
    if (entry.ownerNode >= 0 && entry.ownerNode != requesterNode) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processWriteback PA=0x%lx "
                "OWNER MISMATCH: requesterNode=%d != ownerNode=%d — REJECTED\n",
                _nodeId, line_pa, requesterNode, entry.ownerNode);
        _ownerMismatchRejectedCount++;
        return false;
    }

    // ---- M7: Process writeback ----
    // Dirty data is written back — clear dirty flag.
    // If keepAsClean, the owner retains exclusive clean ownership (G_E).
    // Otherwise, the owner drops the line entirely (G_I).
    if (keepAsClean && requesterNode >= 0) {
        // Owner writes back but retains clean exclusive
        entry.state = MESIState::G_E;
        entry.ownerNode = requesterNode;
        entry.sharersMask = 0;
        entry.dirty = false;
    } else {
        // Owner drops the line completely
        entry.state = MESIState::G_I;
        entry.ownerNode = -1;
        entry.sharersMask = 0;
        entry.dirty = false;
    }
    // v4: DirEntry.pendingOp removed — no-op here

    _writebackCount++;

    DPRINTF(RubyEP,
            "UBCC node_id=%d: processWriteback PA=0x%lx complete "
            "newState=%s ownerNode=%d dirty=%d\n",
            _nodeId, line_pa, mesiStateName(entry.state),
            entry.ownerNode, entry.dirty);

    return true;
}

// ---- M7: GlobalEvict (Clean Evict) ----

bool
UBCCController::processEvict(uint64_t line_pa, int evictingNode,
                              uint64_t epochVal)
{
    DPRINTF(RubyEP,
            "UBCC node_id=%d: processEvict PA=0x%lx "
            "evictingNode=%d epoch=%lu\n",
            _nodeId, line_pa, evictingNode, epochVal);

    auto it = _directory.find(line_pa);
    if (it == _directory.end()) {
        // No entry — nothing to evict, accept as no-op
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processEvict PA=0x%lx "
                "no entry — no-op\n", _nodeId, line_pa);
        return true;
    }

    DirEntry &entry = it->second;

    // ---- M7: Stale epoch check ----
    if (!checkEpochForLine(line_pa, epochVal)) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processEvict PA=0x%lx "
                "STALE epoch: msg=%lu directory=%lu — REJECTED\n",
                _nodeId, line_pa, epochVal, entry.epoch);
        _staleRejectedCount++;
        return false;
    }

    // Phase 2: Line busy check unified to OutstandingRequest-aware
    if (isLineBusy(line_pa)) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processEvict PA=0x%lx "
                "line busy — rejected\n",
                _nodeId, line_pa);
        return false;
    }

    // ---- M7: Process evict based on current state ----
    bool removedFromSharer = false;
    bool removedFromOwner = false;

    // Remove from sharer mask if present
    if (evictingNode >= 0) {
        uint64_t nodeBit = (1ULL << evictingNode);
        if (entry.sharersMask & nodeBit) {
            entry.sharersMask &= ~nodeBit;
            removedFromSharer = true;
        }
    }

    // If the evicting node is the current owner (clean owner, G_E),
    // clear ownership.
    if (entry.ownerNode >= 0 && entry.ownerNode == evictingNode) {
        // Only clean owners (G_E) can evict without writeback.
        // Dirty owners (G_M) must writeback first.
        if (entry.dirty) {
            DPRINTF(RubyEP,
                    "UBCC node_id=%d: processEvict PA=0x%lx "
                    "dirty owner evict not allowed — must writeback first\n",
                    _nodeId, line_pa);
            return false;
        }
        entry.ownerNode = -1;
        entry.sharersMask = 0; // Exclusive owner has no sharers
        removedFromOwner = true;
    }

    // ---- M7 P0-3: Reject evict if node is neither owner nor sharer ----
    if (!removedFromSharer && !removedFromOwner) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processEvict PA=0x%lx "
                "evictingNode=%d is neither owner (ownerNode=%d) nor sharer "
                "(sharersMask=0x%lx) — REJECTED\n",
                _nodeId, line_pa, evictingNode,
                entry.ownerNode, entry.sharersMask);
        return false;
    }

    // ---- Determine new state ----
    if (entry.sharersMask == 0 && entry.ownerNode < 0) {
        // No sharers, no owner → G_I
        entry.state = MESIState::G_I;
    } else if (entry.ownerNode >= 0) {
        // Exclusive owner remains (different from evicting node)
        // State stays G_E or G_M — unchanged
    } else {
        // Share-only line
        entry.state = MESIState::G_S;
    }

    // M7 P0-2: Only clear dirty if we removed a clean owner.
    // Sharer-only eviction must not touch dirty (owner's dirty state preserved).
    // Dirty owner eviction was already rejected above.
    if (removedFromOwner) {
        entry.dirty = false;
    }
    _evictCount++;

    DPRINTF(RubyEP,
            "UBCC node_id=%d: processEvict PA=0x%lx complete "
            "removedSharer=%d removedOwner=%d newState=%s "
            "sharersMask=0x%lx ownerNode=%d\n",
            _nodeId, line_pa, removedFromSharer, removedFromOwner,
            mesiStateName(entry.state),
            entry.sharersMask, entry.ownerNode);

    return true;
}

// ---- v4: Local Upgrade Management (§4.1.4) ----

bool
UBCCController::processOuterUpgradeReq(
    uint64_t line_pa, int requesterNode,
    uint64_t epoch, uint64_t reqId,
    int desiredPerm, UBCC_UpgradeCause cause)
{
    DPRINTF(RubyEP,
            "UBCC node_id=%d: processOuterUpgradeReq PA=0x%lx "
            "requesterNode=%d epoch=%lu reqId=%lu desiredPerm=%d\n",
            _nodeId, line_pa, requesterNode, epoch, reqId, desiredPerm);

    ensureDirEntry(line_pa);
    DirEntry &entry = _directory[line_pa];

    // Check if requester is a committed sharer
    if (requesterNode >= 0) {
        uint64_t reqBit = (1ULL << requesterNode);
        if (!(entry.sharersMask & reqBit)) {
            DPRINTF(RubyEP,
                    "UBCC node_id=%d: upgrade rejected — "
                    "requesterNode=%d not in sharersMask=0x%lx\n",
                    _nodeId, line_pa, requesterNode, entry.sharersMask);
            return false;
        }
    }

    // Check existing outstanding — if any, reject
    if (findOutstanding(line_pa)) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: upgrade rejected — "
                "existing outstanding for PA=0x%lx\n",
                _nodeId, line_pa);
        return false;
    }

    // v4: Allocate reserved epoch (committed epoch + 1)
    uint64_t reservedEpoch = allocateReservedEpoch(entry);

    // Create UPGRADE_PENDING outstanding
    OutstandingRequest *oreq = createOutstanding(
        line_pa, OpType::UPGRADE_PENDING, requesterNode, -1);
    if (!oreq) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: upgrade rejected — "
                "failed to create outstanding for PA=0x%lx\n",
                _nodeId, line_pa);
        return false;
    }

    oreq->reservedEpoch = reservedEpoch;
    oreq->reqId = reqId;
    oreq->baseEpoch = epoch;
    oreq->stage = OpStage::WAITING_LOCAL_DONE;
    oreq->upgradeCause = cause;
    oreq->accepted = true;

    // Determine intended state
    bool writeIntent = (desiredPerm == 1);  // Unique
    oreq->intendedState = writeIntent ? MESIState::G_M : MESIState::G_E;
    oreq->intendedOwnerNode = requesterNode;
    oreq->intendedSharersMask = entry.sharersMask & ~(1ULL << requesterNode);
    oreq->intendedDirty = writeIntent;

    DPRINTF(RubyEP,
            "UBCC node_id=%d: upgrade accepted PA=0x%lx "
            "reservedEpoch=%lu reqId=%lu — irrevocable-after-ack\n",
            _nodeId, line_pa, reservedEpoch, reqId);

    // v4: §4.1.4 step 2-3 — DirEntry NOT modified; committed stays as-is.
    // irrevocable-after-ack: once accepted, can only be DONE or PERSISTENT_BUSY.
    return true;
}

bool
UBCCController::processOuterUpgradeDone(
    uint64_t line_pa, int requesterNode,
    uint64_t epoch, uint64_t reqId)
{
    DPRINTF(RubyEP,
            "UBCC node_id=%d: processOuterUpgradeDone PA=0x%lx "
            "requesterNode=%d epoch=%lu reqId=%lu\n",
            _nodeId, line_pa, requesterNode, epoch, reqId);

    auto it = _directory.find(line_pa);
    if (it == _directory.end()) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processOuterUpgradeDone PA=0x%lx "
                "entry not found\n", _nodeId, line_pa);
        return false;
    }

    // Verify UPGRADE_PENDING outstanding
    OutstandingRequest *ost = findOutstanding(line_pa);
    if (!ost || ost->opType != OpType::UPGRADE_PENDING) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processOuterUpgradeDone PA=0x%lx "
                "no UPGRADE_PENDING outstanding\n", _nodeId, line_pa);
        return false;
    }

    // Verify matching tuple
    if (ost->requesterNode != requesterNode) {
        warn("UBCC node_id=%d: UpgradeDone requester mismatch PA=0x%lx\n",
             _nodeId, line_pa);
        return false;
    }

    // v4: §4.1.4 step 5 — commit intended result to DirEntry
    DirEntry &entry = it->second;
    commitIntendedResult(entry, *ost);

    // Retire UPGRADE_PENDING
    ost->stage = OpStage::DONE;
    ost->respTick = curTick();
    removeOutstanding(line_pa);

    DPRINTF(RubyEP,
            "UBCC node_id=%d: upgrade committed PA=0x%lx "
            "newState=%s owner=%d epoch=%lu\n",
            _nodeId, line_pa, mesiStateName(entry.state),
            entry.ownerNode, entry.epoch);

    return true;
}

// ---- v4: Clear / ClearAck (§3.5) ----

bool
UBCCController::processClear(
    uint64_t line_pa, int srcNode,
    uint64_t epoch, uint64_t reqId)
{
    DPRINTF(RubyEP,
            "UBCC node_id=%d: processClear PA=0x%lx "
            "srcNode=%d epoch=%lu reqId=%lu\n",
            _nodeId, line_pa, srcNode, epoch, reqId);

    // Check tombstone first (duplicate Clear within window W)
    bool tsAccepted = false;
    if (checkTombstone(line_pa, epoch, reqId, tsAccepted)) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: tombstone replay PA=0x%lx "
                "epoch=%lu reqId=%lu accepted=%d\n",
                _nodeId, line_pa, epoch, reqId, tsAccepted);
        return tsAccepted;
    }

    auto it = _directory.find(line_pa);
    if (it == _directory.end()) {
        // Stale Clear for unknown line — log and drop (§3.5)
        warn("UBCC node_id=%d: stale Clear for unknown PA=0x%lx — dropped\n",
             _nodeId, line_pa);
        return false;
    }

    // Verify GRANT_HANDSHAKE outstanding
    OutstandingRequest *ost = findOutstanding(line_pa);
    if (!ost || ost->opType != OpType::GRANT_HANDSHAKE) {
        // No active GRANT_HANDSHAKE — check for already-completed
        // (might be tombstone already cleaned up)
        warn("UBCC node_id=%d: processClear PA=0x%lx "
             "no GRANT_HANDSHAKE outstanding — dropped\n",
             _nodeId, line_pa);
        return false;
    }

    // v4: Verify epoch — the Clear carries the base epoch observed by
    // requester; the GRANT_HANDSHAKE's reservedEpoch = baseEpoch + 1.
    // Compare against baseEpoch (not reservedEpoch) for matching.
    if (ost->baseEpoch != epoch) {
        warn("UBCC node_id=%d: processClear PA=0x%lx "
             "epoch mismatch: ost_base=%lu clear=%lu — dropping, "
             "retiring stale GRANT_HANDSHAKE\n",
             _nodeId, line_pa, ost->baseEpoch, epoch);
        // v4 D-18: Retire stale GRANT_HANDSHAKE so it doesn't block
        // future RECALL/INVALIDATE creation for this PA.
        retireToTombstone(*ost, false);
        removeOutstanding(line_pa);
        return false;
    }

    // Verify reqId match
    if (ost->reqId != reqId) {
        warn("UBCC node_id=%d: processClear PA=0x%lx "
             "reqId mismatch: ost=%lu clear=%lu — dropped\n",
             _nodeId, line_pa, ost->reqId, reqId);
        return false;
    }

    // F2: Strong validation — requesterNode must match srcNode
    if (ost->requesterNode >= 0 && ost->requesterNode != srcNode) {
        warn("UBCC node_id=%d: processClear PA=0x%lx "
             "requesterNode mismatch: ost=%d clear=%d — dropped\n",
             _nodeId, line_pa, ost->requesterNode, srcNode);
        return false;
    }

    // F2: Stage must be WAITING_CLEAR — only accept Clear for an active
    // GRANT_HANDSHAKE that is actually expecting a Clear commit.
    if (ost->stage != OpStage::WAITING_CLEAR) {
        warn("UBCC node_id=%d: processClear PA=0x%lx "
             "stage mismatch: expected WAITING_CLEAR got %d — dropped\n",
             _nodeId, line_pa, static_cast<int>(ost->stage));
        return false;
    }

    // v4: GRANT_HANDSHAKE existence + correct stage implies prerequisites DONE.
    // The upstream processOuterRequest / processInvalidationAck only creates
    // GRANT_HANDSHAKE after all barriers (RECALL/INVALIDATE) have completed.

    // v4: §3.3, §3.5 — commit intended result to committed DirEntry
    DirEntry &entry = it->second;
    commitIntendedResult(entry, *ost);

    // Retire GRANT_HANDSHAKE to tombstone(W) for duplicate Clear replay
    retireToTombstone(*ost, true);
    removeOutstanding(line_pa);

    // Order log audit (§3.6)
    printf("[UBCC-ORDER] pa=0x%lx epoch=%lu reqId=%lu op=ClearGrantHandshake "
           "requester=%d state=%s\n",
           line_pa, epoch, reqId, srcNode,
           mesiStateName(entry.state));

    return true;
}

// ---- v4: Private helpers ----

// Half-range epoch comparison (§3.1.2)
bool
UBCCController::isNewerEpoch(uint64_t a, uint64_t b)
{
    return ((a - b) & 0xffffffffffffffffULL) < (1ULL << 63);
}

uint64_t
UBCCController::allocateReservedEpoch(DirEntry &entry)
{
    // reservedEpoch = committed epoch + 1; committed epoch is NOT modified here
    return entry.epoch + 1;
}

uint64_t
UBCCController::allocateReqId(DirEntry &entry)
{
    return entry.nextReqId++;
}

void
UBCCController::commitIntendedResult(DirEntry &entry, const OutstandingRequest &ost)
{
    entry.state = ost.intendedState;
    entry.sharersMask = ost.intendedSharersMask;
    entry.ownerNode = ost.intendedOwnerNode;
    entry.dirty = ost.intendedDirty;
    entry.epoch = ost.reservedEpoch;

    DPRINTF(RubyEP,
            "UBCC node_id=%d: commitIntendedResult PA=0x%lx "
            "state=%s owner=%d sharers=0x%lx dirty=%d epoch=%lu\n",
            _nodeId, ost.linePa,
            mesiStateName(entry.state), entry.ownerNode,
            entry.sharersMask, entry.dirty, entry.epoch);
}

void
UBCCController::retireToTombstone(const OutstandingRequest &ost, bool accepted)
{
    GrantHandshakeTombstone ts;
    ts.linePa = ost.linePa;
    ts.epoch = ost.reservedEpoch;
    ts.reqId = ost.reqId;
    ts.opType = OpType::GRANT_HANDSHAKE;
    ts.accepted = accepted;
    ts.expireTick = curTick() + _tombstoneWindowW;
    _tombstones[ost.linePa] = ts;

    DPRINTF(RubyEP,
            "UBCC node_id=%d: retireToTombstone PA=0x%lx "
            "epoch=%lu reqId=%lu expireTick=%lu\n",
            _nodeId, ost.linePa, ost.reservedEpoch, ost.reqId, ts.expireTick);
}

bool
UBCCController::checkTombstone(uint64_t linePa, uint64_t epoch, uint64_t reqId,
                                bool &outAccepted)
{
    cleanupTombstones();
    auto it = _tombstones.find(linePa);
    if (it == _tombstones.end())
        return false;

    GrantHandshakeTombstone &ts = it->second;
    if (ts.epoch == epoch && ts.reqId == reqId) {
        outAccepted = ts.accepted;
        DPRINTF(RubyEP,
                "UBCC node_id=%d: checkTombstone HIT PA=0x%lx "
                "epoch=%lu reqId=%lu accepted=%d\n",
                _nodeId, linePa, epoch, reqId, ts.accepted);
        return true;
    }
    return false;
}

void
UBCCController::cleanupTombstones()
{
    Tick now = curTick();
    for (auto it = _tombstones.begin(); it != _tombstones.end(); ) {
        if (it->second.expireTick <= now) {
            DPRINTF(RubyEP,
                    "UBCC node_id=%d: tombstone expired PA=0x%lx "
                    "epoch=%lu reqId=%lu\n",
                    _nodeId, it->second.linePa,
                    it->second.epoch, it->second.reqId);
            it = _tombstones.erase(it);
        } else {
            ++it;
        }
    }
}

// ---- v4: Outstanding request API ----
OutstandingRequest*
UBCCController::findOutstanding(uint64_t linePa)
{
    auto it = _outstandingReqs.find(linePa);
    if (it != _outstandingReqs.end())
        return &it->second;
    return nullptr;
}

OutstandingRequest*
UBCCController::createOutstanding(uint64_t linePa, OpType opType,
                                  int requesterNode, int targetNode)
{
    // v4: Keep single outstanding per line
    if (_outstandingReqs.count(linePa))
        return nullptr;
    OutstandingRequest req;
    req.linePa = linePa;
    req.baseEpoch = getEpochForLine(linePa);
    req.reservedEpoch = 0;   // filled in by caller
    req.reqId = 0;           // filled in by caller
    req.opType = opType;
    req.stage = OpStage::CREATED;
    req.requesterNode = requesterNode;
    req.targetNode = targetNode;
    req.targetMask = 0;
    req.intendedState = MESIState::G_I;
    req.intendedSharersMask = 0;
    req.intendedOwnerNode = -1;
    req.intendedDirty = false;
    req.recallBarrierDone = false;
    req.invalidateBarrierDone = false;
    req.clearAckCached = false;
    req.createTick = curTick();
    req.respTick = 0;
    req.deadlineTick = curTick() + _interconnectLatency * 10;
    req.accepted = false;
    req.dataValid = false;
    req.dataSource = GrantDataSource::HomeMemory;  // F3: default
    req.pendingAckCount = 0;
    req.ackMask = 0;
    req.totalMask = 0;
    _outstandingReqs[linePa] = req;
    return &_outstandingReqs[linePa];
}

void
UBCCController::removeOutstanding(uint64_t linePa)
{
    _outstandingReqs.erase(linePa);
}

} // namespace ruby
} // namespace gem5
