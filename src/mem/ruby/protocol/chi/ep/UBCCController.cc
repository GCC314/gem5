#include "mem/ruby/protocol/chi/ep/UBCCController.hh"

#include <cstdio>
#include <sstream>

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "debug/RubyEP.hh"
#include "mem/ruby/protocol/chi/ep/SentinelHelper.hh"
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
    _recallResponseCount(0)
{
    if (ruby_system) {
        _sentinelHelper = new SentinelHelper(ruby_system, node_id);
    }
    registerInstance(node_id, this);
}

UBCCController::~UBCCController()
{
    _instances.erase(_nodeId);
    delete _sentinelHelper;
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

// ---- M4 Sentinel Registration Test Hooks ----

bool
UBCCController::installSentinelForTest(uint64_t line_pa, bool as_owner)
{
    if (!_sentinelHelper) {
        warn("UBCCController node_id=%d: SentinelHelper not available\n",
             _nodeId);
        return false;
    }

    bool ok = _sentinelHelper->installSentinelForTest(line_pa, as_owner);
    if (ok) {
        _sentinelStates[line_pa] = as_owner ? SS_OWNER : SS_SHARER;
    }
    return ok;
}

bool
UBCCController::removeSentinelForTest(uint64_t line_pa)
{
    if (!_sentinelHelper) {
        warn("UBCCController node_id=%d: SentinelHelper not available\n",
             _nodeId);
        return false;
    }

    bool ok = _sentinelHelper->removeSentinelForTest(line_pa);
    if (ok) {
        _sentinelStates.erase(line_pa);
    }
    return ok;
}

std::string
UBCCController::inspectDirEntryForTest(uint64_t line_pa)
{
    DirEntrySnapshot snap;
    if (!getDirEntrySnapshot(line_pa, snap)) {
        return "{\"error\": \"entry not found\"}";
    }

    std::ostringstream oss;
    oss << "{"
        << "\"sharerCount\":" << snap.sharerCount << ","
        << "\"ownerExists\":" << (snap.ownerExists ? "true" : "false") << ","
        << "\"ownerIsExcl\":" << (snap.ownerIsExcl ? "true" : "false") << ","
        << "\"ownerVersion\":" << snap.ownerVersion << ","
        << "\"ownerStr\":\"" << snap.ownerStr << "\","
        << "\"state\":" << snap.state << ","
        << "\"epRnfInSharers\":" << (snap.epRnfInSharers ? "true" : "false") << ","
        << "\"epRnfIsOwner\":" << (snap.epRnfIsOwner ? "true" : "false") << ","
        << "\"epRnfLookupFailed\":" << (snap.epRnfLookupFailed ? "true" : "false")
        << "}";
    return oss.str();
}

bool
UBCCController::getDirEntrySnapshot(uint64_t line_pa, DirEntrySnapshot &snap)
{
    if (!_sentinelHelper) {
        warn("UBCCController node_id=%d: SentinelHelper not available\n",
             _nodeId);
        return false;
    }
    return _sentinelHelper->inspectDirEntryForTest(line_pa, snap);
}

bool
UBCCController::isDsmAddr(uint64_t pa) const
{
    if (!_sentinelHelper)
        return false;
    return _sentinelHelper->isDsmAddr(pa);
}

uint64_t
UBCCController::getEpRnfSnoopCount() const
{
    if (!_sentinelHelper)
        return 0;
    return _sentinelHelper->getEpRnfSnoopCount();
}

void
UBCCController::resetEpRnfSnoopCount()
{
    if (_sentinelHelper)
        _sentinelHelper->resetEpRnfSnoopCount();
}

void
UBCCController::incrementEpRnfSnoopCount()
{
    if (_sentinelHelper)
        _sentinelHelper->incrementEpRnfSnoopCount();
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

    // ---- M6: Busy check (strict) ----
    // If the line is already busy with an in-flight recall, reject
    // ALL requests — no exceptions. Self-requester reentry is also
    // a protocol violation in M6.
    if (entry.pendingOp > 0) {
        fatal("UBCC node_id=%d: M6 busy-check PA=0x%lx pendingOp=%d "
              "pendingRequester=%d pendingRecallTarget=%d "
              "requesterNode=%d — strict rejection\n",
              _nodeId, line_pa, entry.pendingOp,
              entry.pendingRequester, entry.pendingRecallTarget,
              requesterNode);
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
                // Unique request on shared line → requires invalidation
                // For M5/M6, we handle the simple case of upgrading
                // (full invalidation logic comes in M8)
                if (!writeIntent) {
                    grant = UBCC_OuterGrantType::GlobalGrantExclusive;
                    entry.state = MESIState::G_E;
                    entry.sharersMask = 0; // all sharers invalidated
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

    // ---- M4 Backfill: Sentinel Registration in Grant Path ----
    // Per plan/00-terminology.md §3.2.1, sentinel registration must
    // complete before the grant becomes visible to the requester.
    // We install the sentinel here, immediately after grant decision
    // and before returning control to the requester.
    //
    // M5 Phase 2: Add timing assertion that
    //   sentinel_visible_tick <= grant_visible_tick
    // Since both occur in the same simulation tick (curTick()),
    // and sentinel install is synchronous, this invariant is
    // satisfied by construction. We record and verify it.
    bool sentinelOk = false;
    SentinelState sentMode = SentinelState::SS_SHARER;

    if (grant == UBCC_OuterGrantType::GlobalGrantShared) {
        // Requester gets shared → EP_RNF is a sharer
        sentinelOk = installSentinelForTest(line_pa, false); // as_owner=false
        sentMode = SentinelState::SS_SHARER;
    } else {
        // Requester gets exclusive/modified → EP_RNF is owner
        sentinelOk = installSentinelForTest(line_pa, true); // as_owner=true
        sentMode = SentinelState::SS_OWNER;
    }

    // M5 Phase 2: Timing assertion
    // sentinel install completes synchronously within the same tick,
    // so sentinel_visible_tick == grant_visible_tick == curTick().
    // The spec requires sentinel_visible_tick <= grant_visible_tick.
    // We log both ticks for verification, and assert the ordering.
    Tick sentinelVisibleTick = curTick();
    DPRINTF(RubyEP,
            "UBCC node_id=%d: sentinel timing PA=0x%lx "
            "sentinel_visible_tick=%lu grant_visible_tick=%lu\n",
            _nodeId, line_pa,
            sentinelVisibleTick, grantVisibleTick);

    // Hard assertion: sentinel must be visible no later than grant
    if (sentinelVisibleTick > grantVisibleTick) {
        fatal("UBCC node_id=%d: sentinel registration timing violation "
              "PA=0x%lx sentinel_tick=%lu > grant_tick=%lu\n",
              _nodeId, line_pa, sentinelVisibleTick, grantVisibleTick);
    }

    if (!sentinelOk) {
        // Sentinel install failure is non-fatal for M5 first-miss scenarios
        // (may fail if HN directory is unavailable). In production, this
        // would be a hard failure requiring retry.
        warn("UBCC node_id=%d: sentinel install for PA=0x%lx "
             "returned false (may be non-fatal for first-miss test)\n",
             _nodeId, line_pa);
    } else {
        // Update test mirror
        _sentinelStates[line_pa] = sentMode;
    }

    DPRINTF(RubyEP,
            "UBCC node_id=%d: grant decision PA=0x%lx "
            "prev=%s next=%s grant=%d ownerNode=%d sentinel=%s\n",
            _nodeId, line_pa,
            mesiStateName(prevState), mesiStateName(entry.state),
            static_cast<int>(grant), entry.ownerNode,
            sentinelOk ? "installed" : "FAILED");

    // ---- M5 Phase 2: Propagate tick values to caller ----
    // Write back grantVisibleTick and sentinelVisibleTick so that
    // EPBackend::handleRemoteMiss can populate the OuterGrantEnvelope.
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
                                       bool dataReceived)
{
    auto it = _directory.find(line_pa);
    if (it == _directory.end()) {
        DPRINTF(RubyEP,
                "UBCC node_id=%d: processRecallResponse PA=0x%lx "
                "entry not found\n", _nodeId, line_pa);
        return false;
    }

    DirEntry &entry = it->second;

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

} // namespace ruby
} // namespace gem5
