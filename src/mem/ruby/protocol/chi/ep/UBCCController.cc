#include "mem/ruby/protocol/chi/ep/UBCCController.hh"

#include <cstdio>
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
    const Tick cur_tick = curTick();

    while (!_outerQueue.empty()) {
        const auto &entry = _outerQueue.front();
        if (entry.tick + entry.latency <= cur_tick) {
            _outerQueue.pop();
        } else {
            break;
        }
    }
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
    Tick *outGrantVisibleTick, Tick *outSentinelVisibleTick,
    bool *outRecallNeeded, int *outRecallOwnerNode)
{
    DPRINTF(RubyCHIGeneric,
            "UBCC node_id=%d: processOuterRequest PA=0x%lx req=%d write=%d "
            "requesterNode=%d\n",
            _nodeId, line_pa, static_cast<int>(reqType), writeIntent,
            requesterNode);

    // Initialize M6 recall outputs
    if (outRecallNeeded)   *outRecallNeeded = false;
    if (outRecallOwnerNode) *outRecallOwnerNode = -1;

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

    // Validate: requesterNode must fit within sharersMask bit width (64 bits).
    // Allow requesterNode=-1 (callers may not know the node); the internal
    // "if (requesterNode >= 0)" guards already prevent invalid shifts.
    // Negative values below -1 are also invalid.
    if (requesterNode < -1 || requesterNode >= 64) {
        fatal("UBCC node_id=%d: requesterNode=%d out of range [-1, 63] "
              "for PA=0x%lx\n",
              _nodeId, requesterNode, line_pa);
    }

    ensureDirEntry(line_pa);
    DirEntry &entry = _directory[line_pa];

    // ---- M6/M8/Q3: Busy check ----
    if (entry.pendingOp > 0) {
        // Q3: grant handshake in progress — check timer
        if (entry.pendingOp == 3) {
            Tick elapsed = curTick() - entry.grantTick;
            if (elapsed > 2000000 || entry.pendingRequester == requesterNode) {
                entry.pendingOp = 0;
            } else {
                // Block different requester during handshake window
                if (outRecallNeeded) *outRecallNeeded = false;
                if (outRecallOwnerNode) *outRecallOwnerNode = -1;
                return UBCC_OuterGrantType::GlobalGrantShared;
            }
        }
        // M8: invalidation in progress, same requester reentry
        else if (entry.pendingOp == 2 && entry.pendingRequester == requesterNode) {
            DPRINTF(RubyEP,
                    "UBCC node_id=%d: M8 reentry no-op PA=0x%lx "
                    "pendingOp=invalidation requesterNode=%d matches — "
                    "returning current grant without state mutation\n",
                    _nodeId, line_pa, requesterNode);

            UBCC_OuterGrantType currentGrant =
                UBCC_OuterGrantType::GlobalGrantShared;
            switch (entry.state) {
                case MESIState::G_E:
                    currentGrant = UBCC_OuterGrantType::GlobalGrantExclusive;
                    break;
                case MESIState::G_M:
                    currentGrant = UBCC_OuterGrantType::GlobalGrantModified;
                    break;
                case MESIState::G_S:
                    currentGrant = UBCC_OuterGrantType::GlobalGrantShared;
                    break;
                default:
                    currentGrant = UBCC_OuterGrantType::GlobalGrantShared;
                    break;
            }

            Tick now = curTick();
            if (outGrantVisibleTick) *outGrantVisibleTick = now;
            if (outSentinelVisibleTick) *outSentinelVisibleTick = now;

            return currentGrant;
        }
        // All other busy cases: strict rejection
        else {
            fatal("UBCC node_id=%d: M6/M8/Q3 busy-check PA=0x%lx pendingOp=%d "
                  "pendingRequester=%d pendingRecallTarget=%d "
                  "requesterNode=%d — strict rejection\n",
                  _nodeId, line_pa, entry.pendingOp,
                  entry.pendingRequester, entry.pendingRecallTarget,
                  requesterNode);
        }
    }

    // Record grant-visible tick BEFORE sentinel install,
    // so we can assert sentinel_visible_tick <= grant_visible_tick
    Tick grantVisibleTick = curTick();

    // Increment epoch
    entry.epoch++;

    UBCC_OuterGrantType grant = UBCC_OuterGrantType::GlobalGrantShared;
    MESIState prevState = entry.state;

    switch (entry.state) {
        case MESIState::G_I: {
            // First miss: no sharers, no owner
            if (reqType == UBCC_OuterReqType::GlobalReadShared) {
                // Shared read → GrantShared, enter G_S
                grant = UBCC_OuterGrantType::GlobalGrantShared;
                entry.state = MESIState::G_S;
                // Set sharer bit for requesterNode
                if (requesterNode >= 0)
                    entry.sharersMask |= (1ULL << requesterNode);
                entry.ownerNode = -1;
                entry.dirty = false;
            } else { // GlobalReadUnique
                if (!writeIntent) {
                    // Unique, no write intent → GrantExclusive, enter G_E
                    grant = UBCC_OuterGrantType::GlobalGrantExclusive;
                    entry.state = MESIState::G_E;
                    entry.ownerNode = requesterNode;
                    entry.sharersMask = 0;
                    entry.dirty = false;
                } else {
                    // Unique, write intent → GrantModified, enter G_M
                    grant = UBCC_OuterGrantType::GlobalGrantModified;
                    entry.state = MESIState::G_M;
                    entry.ownerNode = requesterNode;
                    entry.sharersMask = 0;
                    entry.dirty = true;
                }
            }
            break;
        }

        case MESIState::G_S: {
            // Line already has sharers
            if (reqType == UBCC_OuterReqType::GlobalReadShared) {
                // Additional sharer → still G_S, add to sharers
                grant = UBCC_OuterGrantType::GlobalGrantShared;
                // entry.state stays G_S
                if (requesterNode >= 0)
                    entry.sharersMask |= (1ULL << requesterNode);
                entry.dirty = false;
            } else {
                // ---- M8: Unique request on shared line → invalidation flow ----
                // Identify external sharers that must be invalidated.
                // The requester (if already a sharer upgrading to unique)
                // is kept; all other sharers must be wiped.
                uint64_t otherSharers = entry.sharersMask;
                if (requesterNode >= 0)
                    otherSharers &= ~(1ULL << requesterNode);

                if (otherSharers != 0) {
                    // ---- M8: Pending invalidation flow ----
                    // There are other sharers that must be invalidated
                    // before the unique grant is truly valid.
                    // Mark the line busy with invalidation-in-progress (pendingOp=2).
                    DPRINTF(RubyEP,
                            "UBCC node_id=%d: M8 initiating invalidation "
                            "PA=0x%lx otherSharers=0x%lx requester=%d\n",
                            _nodeId, line_pa, otherSharers, requesterNode);

                    entry.pendingOp = 2; // invalidation-in-progress
                    entry.pendingInvalidationMask = otherSharers;
                    entry.invalidatedAckMask = 0;
                    // Count bits in otherSharers
                    entry.pendingInvalidationCount = __builtin_popcountll(otherSharers);
                    entry.pendingRequester = requesterNode;
                    entry.pendingReqType = reqType;
                    entry.pendingWriteIntent = writeIntent;
                    entry.pendingRecallTarget = -1;

                    // Advance state to G_E/G_M immediately (grant is prepared),
                    // but sharersMask is cleared (the requester is now the
                    // exclusive owner, not a sharer).
                    entry.sharersMask = 0;

                    if (!writeIntent) {
                        grant = UBCC_OuterGrantType::GlobalGrantExclusive;
                        entry.state = MESIState::G_E;
                        entry.ownerNode = requesterNode;
                        entry.dirty = false;
                    } else {
                        grant = UBCC_OuterGrantType::GlobalGrantModified;
                        entry.state = MESIState::G_M;
                        entry.ownerNode = requesterNode;
                        entry.dirty = true;
                    }

                    _invalidationCount++;
                } else {
                    // No other sharers — immediate upgrade without invalidation
                    if (!writeIntent) {
                        grant = UBCC_OuterGrantType::GlobalGrantExclusive;
                        entry.state = MESIState::G_E;
                        entry.sharersMask = 0; // exclusive owner, not a sharer
                        entry.ownerNode = requesterNode;
                        entry.dirty = false;
                    } else {
                        grant = UBCC_OuterGrantType::GlobalGrantModified;
                        entry.state = MESIState::G_M;
                        entry.sharersMask = 0;
                        entry.ownerNode = requesterNode;
                        entry.dirty = true;
                    }
                }
            }
            break;
        }

        case MESIState::G_E:
        case MESIState::G_M: {
            // ---- M6: Line has an owner → recall path ----
            // If the current owner is a remote node (not the requester),
            // we need to recall the owner first.
            // In single-gem5 prototype, ownerNode is always different
            // from requesterNode when the state is G_E or G_M (unless
            // requester is re-requesting -- self-request path for testing).
            int existingOwner = entry.ownerNode;

            if (existingOwner >= 0 &&
                existingOwner != requesterNode) {
                // ---- Recall Needed ----
                // Initiate recall instead of immediately resolving.
                bool recallStarted = initiateRecall(
                    line_pa, entry, reqType, writeIntent, requesterNode);

                DPRINTF(RubyEP,
                        "UBCC node_id=%d: M6 recall initiated PA=0x%lx "
                        "existingOwner=%d requester=%d recallStarted=%d\n",
                        _nodeId, line_pa, existingOwner,
                        requesterNode, recallStarted);

                if (recallStarted) {
                    _recallCount++;
                    // Signal to caller that recall is needed
                    if (outRecallNeeded)
                        *outRecallNeeded = true;
                    if (outRecallOwnerNode)
                        *outRecallOwnerNode = existingOwner;

                    // Return a provisional grant type based on the request
                    // (the final grant is determined after recall completes).
                    if (reqType == UBCC_OuterReqType::GlobalReadShared) {
                        grant = UBCC_OuterGrantType::GlobalGrantShared;
                    } else if (!writeIntent) {
                        grant = UBCC_OuterGrantType::GlobalGrantExclusive;
                    } else {
                        grant = UBCC_OuterGrantType::GlobalGrantModified;
                    }
                } else {
                    // Recall failed to initiate, fall back to direct state
                    // change (backward compatibility with M5 behavior).
                    DPRINTF(RubyEP,
                            "UBCC node_id=%d: recall initiation failed, "
                            "falling back to direct state change\n", _nodeId);
                    if (reqType == UBCC_OuterReqType::GlobalReadShared) {
                        grant = UBCC_OuterGrantType::GlobalGrantShared;
                        entry.state = MESIState::G_S;
                        if (requesterNode >= 0)
                            entry.sharersMask |= (1ULL << requesterNode);
                        entry.ownerNode = -1;
                        entry.dirty = false;
                        entry.pendingOp = 0;
                    } else if (!writeIntent) {
                        grant = UBCC_OuterGrantType::GlobalGrantExclusive;
                        entry.state = MESIState::G_E;
                        entry.ownerNode = requesterNode;
                        entry.dirty = false;
                        entry.pendingOp = 0;
                    } else {
                        grant = UBCC_OuterGrantType::GlobalGrantModified;
                        entry.state = MESIState::G_M;
                        entry.ownerNode = requesterNode;
                        entry.dirty = true;
                        entry.pendingOp = 0;
                    }
                }
            } else {
                // Same owner or no existing owner → direct state change
                if (reqType == UBCC_OuterReqType::GlobalReadShared) {
                    // Read on owned line → downgrade to shared
                    grant = UBCC_OuterGrantType::GlobalGrantShared;
                    entry.state = MESIState::G_S;
                    if (requesterNode >= 0)
                        entry.sharersMask |= (1ULL << requesterNode);
                    entry.ownerNode = -1;
                    entry.dirty = false;
                } else {
                    // Unique on owned line → owner transfer or keep
                    if (!writeIntent) {
                        grant = UBCC_OuterGrantType::GlobalGrantExclusive;
                        entry.state = MESIState::G_E;
                        entry.ownerNode = requesterNode;
                        entry.dirty = false;
                    } else {
                        grant = UBCC_OuterGrantType::GlobalGrantModified;
                        entry.state = MESIState::G_M;
                        entry.ownerNode = requesterNode;
                        entry.dirty = true;
                    }
                }
            }
            break;
        }
    }

    // ---- Grant timing ----
    // UBCC directory (sharersMask/ownerNode) is the authoritative
    // registration — no separate sentinel install needed.
    // sentinelVisibleTick == grantVisibleTick by construction.
    Tick sentinelVisibleTick = curTick();

    DPRINTF(RubyEP,
            "UBCC node_id=%d: grant decision PA=0x%lx "
            "prev=%s next=%s grant=%d ownerNode=%d\n",
            _nodeId, line_pa,
            mesiStateName(prevState), mesiStateName(entry.state),
            static_cast<int>(grant), entry.ownerNode);

    // ---- Q3: Mark grant CHI handshake in progress ----
    // Short delay to prevent ReadShared from HN-F reaching L2 while
    // the previous ReadUnique TBE is being freed.  1500 ticks is
    // sufficient to avoid the SC_RSC crash without blocking invalidation.
    if (curTick() > 0 && entry.pendingOp == 0) {
        entry.pendingOp = 3;
        entry.pendingRequester = requesterNode;
        entry.grantTick = curTick();
    }

    // Propagate tick values to caller
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
        << "\"pendingOp\":" << e.pendingOp << ","
        << "\"pendingRequester\":" << e.pendingRequester << ","
        << "\"pendingRecallTarget\":" << e.pendingRecallTarget;
    // M6: Only include if available
    if (e.pendingOp > 0) {
        oss << ","
            << "\"pendingReqType\":" << static_cast<int>(e.pendingReqType) << ","
            << "\"pendingWriteIntent\":" << (e.pendingWriteIntent ? "true" : "false");
    }
    // M7: Counters for test observation
    oss << ","
        << "\"writebackCount\":" << _writebackCount << ","
        << "\"evictCount\":" << _evictCount << ","
        << "\"staleRejectedCount\":" << _staleRejectedCount << ","
        << "\"ownerMismatchRejectedCount\":" << _ownerMismatchRejectedCount;
    // M8: Invalidation fields
    if (e.pendingOp == 2) {
        oss << ","
            << "\"pendingInvalidationCount\":" << e.pendingInvalidationCount << ","
            << "\"pendingInvalidationMask\":" << e.pendingInvalidationMask << ","
            << "\"invalidatedAckMask\":" << e.invalidatedAckMask;
    }
    oss << ","
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
    outBusy = (e.pendingOp > 0);
    outPendingRequester = e.pendingRequester;
    outPendingRecallTarget = e.pendingRecallTarget;
    return true;
}

// ---- M6: Recall Management ----

bool
UBCCController::initiateRecall(uint64_t line_pa, DirEntry &entry,
    UBCC_OuterReqType reqType, bool writeIntent, int requesterNode)
{
    // Mark the line as busy (pendingOp = 1 = recall-in-progress)
    entry.pendingOp = 1;
    entry.pendingRequester = requesterNode;
    entry.pendingRecallTarget = entry.ownerNode;
    entry.pendingReqType = reqType;
    entry.pendingWriteIntent = writeIntent;

    DPRINTF(RubyEP,
            "UBCC node_id=%d: initiateRecall PA=0x%lx "
            "ownerNode=%d requester=%d state=%s dirty=%d\n",
            _nodeId, line_pa, entry.ownerNode, requesterNode,
            mesiStateName(entry.state), entry.dirty);

    return true;
}

bool
UBCCController::processRecallResponse(uint64_t line_pa, int ownerNode,
                                       bool dataReceived, uint64_t responseEpoch)
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end()) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processRecallResponse PA=0x%lx "
                "entry not found\n", _nodeId, line_pa);
        return false;
    }

    DirEntry &entry = it->second;

    // ---- M7: Stale epoch check ----
    // M7 P1-5: Remove epoch==0 bypass — all paths must pass epoch check.
    if (!checkEpochForLine(line_pa, responseEpoch)) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processRecallResponse PA=0x%lx "
                "STALE epoch: response=%lu directory=%lu — REJECTED\n",
                _nodeId, line_pa, responseEpoch, entry.epoch);
        _staleRejectedCount++;
        return false;
    }

    // Verify this is a pending recall
    if (entry.pendingOp != 1) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processRecallResponse PA=0x%lx "
                "no pending recall (pendingOp=%d)\n",
                _nodeId, line_pa, entry.pendingOp);
        return false;
    }

    // Verify the recall target matches — strict protocol check.
    if (entry.pendingRecallTarget >= 0 &&
        entry.pendingRecallTarget != ownerNode) {
        fatal("UBCC node_id=%d: recall owner mismatch PA=0x%lx "
              "expected=%d got=%d — protocol violation\n",
              _nodeId, line_pa, entry.pendingRecallTarget, ownerNode);
    }

    int requesterNode = entry.pendingRequester;
    UBCC_OuterReqType reqType = entry.pendingReqType;
    bool writeIntent = entry.pendingWriteIntent;
    MESIState prevState = entry.state;
    bool wasDirty = entry.dirty;

    DPRINTF(RubyEP,
            "UBCC node_id=%d: processRecallResponse PA=0x%lx "
            "ownerNode=%d dataReceived=%d requester=%d reqType=%d "
            "prevState=%s dirty=%d\n",
            _nodeId, line_pa, ownerNode, dataReceived,
            requesterNode, static_cast<int>(reqType),
            mesiStateName(prevState), wasDirty);

    // ---- Complete the directory transition ----
    // Based on the recall result and request type, determine the new state.
    if (reqType == UBCC_OuterReqType::GlobalReadShared) {
        // Read on owned line → old owner downgraded to shared
        // Add requester and existing owner to sharers
        entry.state = MESIState::G_S;
        if (requesterNode >= 0)
            entry.sharersMask |= (1ULL << requesterNode);
        if (ownerNode >= 0)
            entry.sharersMask |= (1ULL << ownerNode);
        entry.ownerNode = -1;
        if (dataReceived && wasDirty) {
            // Dirty data was written back through recall
            // No persistent data storage here (metadata-only)
        }
        entry.dirty = false;
    } else {
        // Unique/write request → new requester becomes owner
        if (!writeIntent) {
            entry.state = MESIState::G_E;
            entry.dirty = false;
        } else {
            entry.state = MESIState::G_M;
            entry.dirty = true;
        }
        entry.ownerNode = requesterNode;
        entry.sharersMask = 0; // old owner invalidated
    }

    // Clear pending recall context
    entry.pendingOp = 0;
    entry.pendingRequester = -1;
    entry.pendingRecallTarget = -1;

    _recallResponseCount++;

    DPRINTF(RubyEP,
            "UBCC node_id=%d: recall completed PA=0x%lx "
            "prevState=%s newState=%s newOwner=%d\n",
            _nodeId, line_pa,
            mesiStateName(prevState), mesiStateName(entry.state),
            entry.ownerNode);

    return true;
}

bool
UBCCController::isLineBusy(uint64_t line_pa) const
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end())
        return false;
    return it->second.pendingOp > 0;
}

int
UBCCController::getPendingRequester(uint64_t line_pa) const
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end())
        return -1;
    return it->second.pendingRequester;
}

int
UBCCController::getPendingRecallTarget(uint64_t line_pa) const
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end())
        return -1;
    return it->second.pendingRecallTarget;
}

// ---- M8: Global Invalidation Management ----

bool
UBCCController::processInvalidationAck(uint64_t line_pa, int ackNode,
                                        uint64_t responseEpoch)
{
    // Validate ackNode boundaries BEFORE any directory access or early-return.
    // ackNode must be in [0, 63] to safely compute (1ULL << ackNode).
    if (ackNode < 0 || ackNode >= 64) {
        warn("UBCC node_id=%d: processInvalidationAck PA=0x%lx "
             "ackNode=%d out of range [0, 63] — REJECTED\n",
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

    // ---- Stale epoch check ----
    if (!checkEpochForLine(line_pa, responseEpoch)) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processInvalidationAck PA=0x%lx "
                "STALE epoch: response=%lu directory=%lu — REJECTED\n",
                _nodeId, line_pa, responseEpoch, entry.epoch);
        _staleRejectedCount++;
        return false;
    }

    // Verify this is a pending invalidation
    if (entry.pendingOp != 2) {
        // If invalidation already completed (pendingOp==0), accept
        // as idempotent duplicate ack without error.
        if (entry.pendingOp == 0) {
            DPRINTF(RubyEP,
                    "UBCC node_id=%d: processInvalidationAck PA=0x%lx "
                    "invalidation already completed — idempotent, ignoring\n",
                    _nodeId, line_pa);
            return true;
        }
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processInvalidationAck PA=0x%lx "
                "no pending invalidation (pendingOp=%d)\n",
                _nodeId, line_pa, entry.pendingOp);
        return false;
    }

    // Verify the ack node is in the pending invalidation mask
    uint64_t nodeBit = (1ULL << ackNode);
    if (!(entry.pendingInvalidationMask & nodeBit)) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processInvalidationAck PA=0x%lx "
                "ackNode=%d not in pendingInvalidationMask=0x%lx\n",
                _nodeId, line_pa, ackNode, entry.pendingInvalidationMask);
        return false;
    }

    // Check for duplicate ack
    if (entry.invalidatedAckMask & nodeBit) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processInvalidationAck PA=0x%lx "
                "duplicate ack from node %d — ignoring\n",
                _nodeId, line_pa, ackNode);
        return true; // Idempotent: accept duplicate without error
    }

    // Record the ack
    entry.invalidatedAckMask |= nodeBit;
    entry.pendingInvalidationCount--;
    // Clear the sharer bit now that invalidation is confirmed
    entry.sharersMask &= ~nodeBit;

    DPRINTF(RubyEP,
            "UBCC node_id=%d: invalidation ack PA=0x%lx ackNode=%d "
            "remaining=%d ackMask=0x%lx pendingMask=0x%lx\n",
            _nodeId, line_pa, ackNode,
            entry.pendingInvalidationCount,
            entry.invalidatedAckMask, entry.pendingInvalidationMask);

    _invalidationAckCount++;

    // Check if all invalidations are complete
    if (entry.pendingInvalidationCount == 0) {
        // All sharers have acked — clear pending invalidation state
        DPRINTF(RubyEP,
                "UBCC node_id=%d: all invalidations complete PA=0x%lx "
                "state=%s ownerNode=%d\n",
                _nodeId, line_pa,
                mesiStateName(entry.state), entry.ownerNode);

        entry.pendingOp = 0;
        entry.pendingInvalidationMask = 0;
        entry.invalidatedAckMask = 0;
        entry.pendingRequester = -1;
    }

    return true;
}

int
UBCCController::getPendingInvalidationCount(uint64_t line_pa) const
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end())
        return -1;
    return (it->second.pendingOp == 2)
        ? it->second.pendingInvalidationCount
        : -1;
}

uint64_t
UBCCController::getPendingInvalidationMask(uint64_t line_pa) const
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end())
        return 0;
    return it->second.pendingInvalidationMask;
}

// ---- M7: Epoch / Stale Protection ----

bool
UBCCController::checkEpochForLine(uint64_t line_pa, uint64_t responseEpoch) const
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end())
        return true; // No entry yet — accept (first miss creates entry)
    return responseEpoch == it->second.epoch;
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
    entry.pendingOp = 0; // Clear any pending ops

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

    // ---- M7: Line busy check ----
    // Cannot evict while line is busy with in-flight transaction.
    if (entry.pendingOp > 0) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processEvict PA=0x%lx "
                "line busy (pendingOp=%d) — rejected\n",
                _nodeId, line_pa, entry.pendingOp);
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

// ---- Q3: Grant CHI handshake completion callback ----
void
UBCCController::grantHandshakeComplete(uint64_t line_pa)
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end())
        return;

    if (it->second.pendingOp == 3) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: Q3 grantHandshakeComplete PA=0x%lx "
                "-- releasing pendingOp\n",
                _nodeId, line_pa);
        it->second.pendingOp = 0;
        it->second.pendingRequester = -1;
    }
}

} // namespace ruby
} // namespace gem5
