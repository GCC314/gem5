#include "mem/ruby/protocol/chi/ep/EPBackend.hh"

#include <sstream>

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "debug/RubyEP.hh"
#include "mem/ruby/protocol/chi/ep/EPRNFController.hh"
#include "mem/ruby/protocol/chi/ep/UBCCController.hh"
#include "mem/ruby/system/RubySystem.hh"
#include "params/EPBackend.hh"

namespace gem5
{

namespace ruby
{

// Static registry for cross-node EPBackend routing (M6)
std::map<int, EPBackend*> EPBackend::_backendInstances;

EPBackend* EPBackend::getBackendInstance(int node_id)
{
    auto it = _backendInstances.find(node_id);
    return (it != _backendInstances.end()) ? it->second : nullptr;
}

EPBackend::EPBackend(const Params &p)
  : SimObject(p),
    _nodeId(p.node_id),
    _addrMap(3, 128ULL * 1024 * 1024),
    _lastSideband{false, 0, 0, false, -1, -1, -1},
    _recallReceivedCount(0),
    _recallResponseSentCount(0),
    _writebackCount(0),
    _evictCount(0),
    _invalidationReceivedCount(0),
    _invalidationAckSentCount(0)
{
    // Pass RubySystem to UBCCController for SentinelHelper init
    // RubySystem is available via the params
    auto *ruby_system = p.ruby_system;
    _ubcc = new UBCCController(_nodeId, ruby_system);

    // M6: Register this EPBackend in the static cross-node routing registry
    _backendInstances[_nodeId] = this;
}

EPBackend::~EPBackend()
{
    // M6: Deregister from cross-node routing registry
    _backendInstances.erase(_nodeId);
    delete _ubcc;
}

/**
 * Forward declare the M4 self-test entry point (defined in M4SelfTest.cc).
 */
void m4SelfTest_run(EPBackend*);

/**
 * Forward declare the M5 self-test entry point (defined in M5SelfTest.cc).
 */
void m5SelfTest_run(EPBackend*);

/**
 * Forward declare the M6 self-test entry point (defined in M6SelfTest.cc).
 */
void m6SelfTest_run(EPBackend*);

/**
 * Forward declare the M7 self-test entry point (defined in M7SelfTest.cc).
 */
void m7SelfTest_run(EPBackend*);

/**
 * Forward declare the M8 self-test entry point (defined in M8SelfTest.cc).
 */
void m8SelfTest_run(EPBackend*);


void
EPBackend::init()
{
    SimObject::init();

    // ---- M4 Sentinel Registration Self-Test ----
    // ---- M5 Sideband Self-Test ----
    // ---- M6 UBCC Directory + EP_RNF Self-Test ----
    // Runs during instantiation; results printed to stdout.
    // Python test harness parses the output.
    // Only one node (node 0) runs the self-tests to avoid duplicate output.
    if (_nodeId == 0 && _ubcc) {
        m4SelfTest_run(this);
        m5SelfTest_run(this);
        m6SelfTest_run(this);
        m7SelfTest_run(this);
        m8SelfTest_run(this);
    }
}

void
EPBackend::wakeup()
{
    if (_ubcc)
        _ubcc->wakeup();
}

bool
EPBackend::checkAddr(uint64_t pa) const
{
    if (_addrMap.isDsm(_nodeId, pa)) {
        int h = _addrMap.homeNode(_nodeId, pa);
        if (h == _nodeId) {
            return true;
        } else {
            fatal("EPBackend node_id=%d: cross-node DSM access "
                  "PA=0x%lx src=%d home_node=%d",
                  _nodeId, pa, _addrMap.srcNodeId(pa), h);
        }
    }
    fatal("EPBackend node_id=%d: forbidden non-DSM access PA=0x%lx",
          _nodeId, pa);
    return false;
}

bool
EPBackend::checkDsmAddr(uint64_t pa) const
{
    if (_addrMap.isDsm(_nodeId, pa))
        return true;

    fatal("EPBackend node_id=%d: non-DSM address on EP path PA=0x%lx",
          _nodeId, pa);
    return false;
}

// ---- M4 Sentinel Registration Test Hooks ----

bool
EPBackend::installSentinelForTest(uint64_t line_pa, bool as_owner)
{
    if (!_ubcc) {
        warn("EPBackend node_id=%d: UBCC not available\n", _nodeId);
        return false;
    }
    return _ubcc->installSentinelForTest(line_pa, as_owner);
}

bool
EPBackend::removeSentinelForTest(uint64_t line_pa)
{
    if (!_ubcc) {
        warn("EPBackend node_id=%d: UBCC not available\n", _nodeId);
        return false;
    }
    return _ubcc->removeSentinelForTest(line_pa);
}

std::string
EPBackend::inspectDirEntryForTest(uint64_t line_pa)
{
    if (!_ubcc) {
        return "{\"error\": \"UBCC not available\"}";
    }
    return _ubcc->inspectDirEntryForTest(line_pa);
}

bool
EPBackend::isDsmAddr(uint64_t pa) const
{
    if (!_ubcc)
        return false;
    return _ubcc->isDsmAddr(pa);
}

uint64_t
EPBackend::getEpRnfSnoopCount() const
{
    if (!_ubcc)
        return 0;
    return _ubcc->getEpRnfSnoopCount();
}

void
EPBackend::resetEpRnfSnoopCount()
{
    if (_ubcc)
        _ubcc->resetEpRnfSnoopCount();
}

void
EPBackend::incrementEpRnfSnoopCount()
{
    if (_ubcc)
        _ubcc->incrementEpRnfSnoopCount();
}

// ---- M5: Remote Miss Request Dispatch ----

int
EPBackend::handleRemoteMiss(uint64_t line_pa, int neededPerm, bool writeIntent,
                             int& outHomeNode)
{
    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: handleRemoteMiss PA=0x%lx "
            "neededPerm=%d writeIntent=%d\n",
            _nodeId, line_pa, neededPerm, writeIntent);

    // Validate: neededPerm must be 0 (Shared) or 1 (Unique)
    if (neededPerm != 0 && neededPerm != 1) {
        fatal("EPBackend node_id=%d: invalid neededPerm=%d "
              "(must be 0 or 1) for PA=0x%lx\n",
              _nodeId, neededPerm, line_pa);
    }

    // Validate: Shared + true is illegal
    if (neededPerm == 0 && writeIntent) {
        fatal("EPBackend node_id=%d: illegal sideband combination "
              "Shared+writeIntent=true for PA=0x%lx\n",
              _nodeId, line_pa);
    }

    // Validate DSM address
    if (!_addrMap.isDsm(_nodeId, line_pa)) {
        fatal("EPBackend node_id=%d: non-DSM address on remote miss path "
              "PA=0x%lx\n", _nodeId, line_pa);
    }

    int homeNode = _addrMap.homeNode(_nodeId, line_pa);
    if (homeNode < 0 || homeNode == _nodeId) {
        fatal("EPBackend node_id=%d: invalid home node %d for PA=0x%lx\n",
              _nodeId, homeNode, line_pa);
    }
    outHomeNode = homeNode;

    // Translate PA from requester's view to home node's view.
    uint64_t offset = _addrMap.dsmOffset(line_pa);
    uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode, offset);

    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: translating PA 0x%lx -> home PA 0x%lx "
            "homeNode=%d offset=0x%lx\n",
            _nodeId, line_pa, homePa, homeNode, offset);

    // Map sideband to outer request type
    OuterReqType reqType;
    if (neededPerm == 0) {
        // Shared + false → GlobalReadShared
        reqType = OuterReqType::GlobalReadShared;
    } else {
        // Unique + false/true → GlobalReadUnique
        reqType = OuterReqType::GlobalReadUnique;
    }

    // Create requester bookkeeping entry (uses requester's PA view)
    _epochCounter++;
    RequesterLineEntry entry;
    entry.lineAddr = line_pa;
    entry.state = RequesterLineState::R_WAIT_GRANT;
    entry.pendingReq = reqType;
    entry.epoch = _epochCounter;
    entry.writeIntent = writeIntent;
    entry.homeNode = homeNode;
    _requesterLines[line_pa] = entry;

    // Dispatch to home node's UBCC via cross-node registry.
    // Use home PA so the home UBCC sees the line in its own address space.
    UBCCController *homeUbcc = UBCCController::getInstance(homeNode);
    if (!homeUbcc) {
        // Fallback: use our own UBCC (for same-home-node or
        // bootstrap scenarios). This is a temporary M5 simplification.
        DPRINTF(RubyCHIGeneric,
                "EPBackend node_id=%d: home UBCC for node %d not found, "
                "falling back to local UBCC\n",
                _nodeId, homeNode);
        homeUbcc = _ubcc;
    }
    if (!homeUbcc) {
        fatal("EPBackend node_id=%d: no UBCC available for home node %d "
              "PA=0x%lx\n", _nodeId, homeNode, line_pa);
    }

    // ---- M5 Phase 2: Outer Message Envelope ----
    // Build a proper outer request envelope for dispatch, logging,
    // and future network migration.
    OuterReqEnvelope reqEnv;
    reqEnv.linePa = homePa;
    reqEnv.reqType = reqType;
    reqEnv.writeIntent = writeIntent;
    reqEnv.srcNode = _nodeId;
    reqEnv.epoch = entry.epoch;
    _lastReqEnv = reqEnv;

    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: outer request envelope "
            "linePa=0x%lx reqType=%d writeIntent=%d srcNode=%d epoch=%lu\n",
            _nodeId, reqEnv.linePa, static_cast<int>(reqEnv.reqType),
            reqEnv.writeIntent, reqEnv.srcNode, reqEnv.epoch);

    // Convert outer request type to UBCC's internal enum
    UBCC_OuterReqType ubccReq =
        (reqType == OuterReqType::GlobalReadShared)
            ? UBCC_OuterReqType::GlobalReadShared
            : UBCC_OuterReqType::GlobalReadUnique;

    // ---- M6: Mark outer txn pending before dispatching ----
    // Inform local EP_RNF that an outer transaction is in flight
    // for this line, so HN snoop responses are delayed until completion.
    if (_epRnfCtrl) {
        _epRnfCtrl->setOuterTxnPending(line_pa, true);
    }

    // Send to home UBCC using home PA view and requesterNode
    // M5 Phase 2: capture grant/sentinel visible ticks for envelope
    Tick grantVisibleTick = 0;
    Tick sentinelVisibleTick = 0;
    // ---- M6: Recall detection ----
    bool recallNeeded = false;
    int recallOwnerNode = -1;
    UBCC_OuterGrantType ubccGrant =
        homeUbcc->processOuterRequest(homePa, ubccReq, writeIntent, _nodeId,
                                      &grantVisibleTick, &sentinelVisibleTick,
                                      &recallNeeded, &recallOwnerNode);

    // ---- M6: Handle recall path ----
    // If the home UBCC signals that a recall is needed, we must
    // route the recall through the owner node's EPBackend
    // (not bypass it with a direct processRecallResponse call).
    if (recallNeeded && recallOwnerNode >= 0) {
        DPRINTF(RubyEP,
                "EPBackend node_id=%d: M6 recall needed PA=0x%lx "
                "ownerNode=%d requesterNode=%d\n",
                _nodeId, line_pa, recallOwnerNode, _nodeId);

        // Build recall message
        OuterRecallMsg recallMsg;
        recallMsg.linePa = homePa;
        // P1-4: Compute owner's local PA for _requesterLines lookup
        recallMsg.ownerLocalPa = _addrMap.buildDsmPA(
            recallOwnerNode, homeNode, offset);
        recallMsg.ownerNode = recallOwnerNode;
        recallMsg.homeNode = homeNode;
        recallMsg.epoch = entry.epoch;
        // Recall triggered by read: owner downgrades to shared
        // Recall triggered by unique/write: owner invalidates
        recallMsg.isReadRequest = (reqType == OuterReqType::GlobalReadShared);
        // Data is needed if the owner was dirty (G_M state)
        recallMsg.dataNeeded = true; // conservative: always request data

        _lastRecallMsg = recallMsg;

        // M6: Route recall through the owner node's EPBackend.
        // The owner EPBackend processes the recall (handleRecallRequest)
        // and sends the response back to the home UBCC via
        // sendRecallResponse -> processRecallResponse.
        // This eliminates the direct shortcut and ensures proper
        // owner-node recall semantics.
        EPBackend *ownerBackend = EPBackend::getBackendInstance(recallOwnerNode);
        if (ownerBackend) {
            DPRINTF(RubyEP,
                    "EPBackend node_id=%d: routing recall to owner "
                    "EPBackend node %d\n",
                    _nodeId, recallOwnerNode);
            bool recallOk = ownerBackend->handleRecallRequest(recallMsg);
            if (!recallOk) {
                // M6 P0-2: Recall failure must abort the grant.
                // Proceeding to handleGrant after a failed recall
                // violates the protocol (the line may still be owned
                // by the recalled node with conflicting permissions).
                fatal("EPBackend node_id=%d: owner EPBackend node %d "
                      "rejected recall for PA=0x%lx - "
                      "cannot proceed with grant\n",
                      _nodeId, recallOwnerNode, line_pa);
            }
        } else {
            // M6 P0-1: No fallback — owner EPBackend must be in registry.
            // Bypassing the owner EPBackend with a direct
            // processRecallResponse call silently skips the proper
            // recall path and must never happen.
            fatal("EPBackend node_id=%d: owner EPBackend for node %d "
                  "not found in registry for recall PA=0x%lx - "
                  "cannot bypass recall path\n",
                  _nodeId, recallOwnerNode, line_pa);
        }
    }

    // ---- M8: Global Invalidation Routing ----
    // Check if the home UBCC has pending invalidations from this
    // request (e.g., G_S upgrade to unique with external sharers).
    {
        int pendingInvCount = homeUbcc->getPendingInvalidationCount(homePa);
        if (pendingInvCount > 0) {
            uint64_t pendingInvMask = homeUbcc->getPendingInvalidationMask(homePa);
            // P0-1: Use home UBCC's line epoch (not requester's local epoch)
            // so that processInvalidationAck's checkEpochForLine() matches.
            uint64_t homeEpoch = homeUbcc->getEpochForLine(homePa);
            DPRINTF(RubyEP,
                    "EPBackend node_id=%d: M8 routing invalidations "
                    "PA=0x%lx homePa=0x%lx invCount=%d invMask=0x%lx "
                    "homeEpoch=%lu\n",
                    _nodeId, line_pa, homePa,
                    pendingInvCount, pendingInvMask, homeEpoch);

            // For each sharer node in the pending invalidation mask,
            // send an invalidation request through that node's EPBackend.
            for (int s = 0; s < 64 && pendingInvMask != 0; s++) {
                uint64_t sBit = (1ULL << s);
                if (pendingInvMask & sBit) {
                    pendingInvMask &= ~sBit; // Clear as we process

                    OuterInvalidateMsg invMsg;
                    invMsg.linePa = homePa;
                    // Compute sharer's local PA
                    invMsg.sharerLocalPa = _addrMap.buildDsmPA(
                        s, homeNode, offset);
                    invMsg.sharerNode = s;
                    invMsg.homeNode = homeNode;
                    invMsg.epoch = homeEpoch; // P0-1: use home epoch

                    _lastInvalidateMsg = invMsg;

                    // Route invalidation to the sharer node's EPBackend
                    EPBackend *sharerBackend = EPBackend::getBackendInstance(s);
                    if (sharerBackend) {
                        DPRINTF(RubyEP,
                                "EPBackend node_id=%d: routing invalidation "
                                "to node %d\n", _nodeId, s);
                        bool invOk = sharerBackend->handleInvalidationRequest(invMsg);
                        if (!invOk) {
                            fatal("EPBackend node_id=%d: sharer node %d "
                                  "rejected invalidation for PA=0x%lx\n",
                                  _nodeId, s, line_pa);
                        }
                    } else {
                        // In single-gem5 prototype with cross-node EPBackend
                        // registry, all nodes' EPBackends should be registered.
                        // If a sharer's EPBackend is missing (maybe it hasn't
                        // been instantiated yet in a real multi-gem5 scenario),
                        // we can issue a direct ack for prototype purposes.
                        DPRINTF(RubyEP,
                                "EPBackend node_id=%d: sharer EPBackend for "
                                "node %d not found — issuing direct ack\n",
                                _nodeId, s);

                        // Direct ack through home UBCC (use home epoch)
                        homeUbcc->processInvalidationAck(homePa, s, homeEpoch);
                    }
                }
            }
        }
    }

    // ---- M5 Phase 2: Outer Grant Envelope ----
    // Capture grant decision into structured envelope
    OuterGrantEnvelope grantEnv;
    grantEnv.linePa = homePa;
    grantEnv.homeNode = homeNode;
    grantEnv.epoch = entry.epoch;
    grantEnv.grantVisibleTick = grantVisibleTick;
    grantEnv.sentinelVisibleTick = sentinelVisibleTick;

    // Self-test assertion: sentinelVisibleTick <= grantVisibleTick
    if (sentinelVisibleTick > grantVisibleTick) {
        fatal("EPBackend node_id=%d: tick ordering violation "
              "PA=0x%lx sentinelVisibleTick=%lu > grantVisibleTick=%lu\n",
              _nodeId, line_pa, sentinelVisibleTick, grantVisibleTick);
    }

    // Convert UBCC grant back to EPBackend's OuterGrantType
    OuterGrantType grant;
    switch (ubccGrant) {
        case UBCC_OuterGrantType::GlobalGrantShared:
            grant = OuterGrantType::GlobalGrantShared;
            grantEnv.grantType = OuterGrantType::GlobalGrantShared;
            break;
        case UBCC_OuterGrantType::GlobalGrantExclusive:
            grant = OuterGrantType::GlobalGrantExclusive;
            grantEnv.grantType = OuterGrantType::GlobalGrantExclusive;
            break;
        case UBCC_OuterGrantType::GlobalGrantModified:
            grant = OuterGrantType::GlobalGrantModified;
            grantEnv.grantType = OuterGrantType::GlobalGrantModified;
            break;
        default:
            fatal("EPBackend node_id=%d: unknown UBCC grant %d\n",
                  _nodeId, static_cast<int>(ubccGrant));
    }

    _lastGrantEnv = grantEnv;

    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: outer grant envelope "
            "linePa=0x%lx grantType=%d homeNode=%d epoch=%lu\n",
            _nodeId, grantEnv.linePa, static_cast<int>(grantEnv.grantType),
            grantEnv.homeNode, grantEnv.epoch);

    // Handle grant result and update bookkeeping
    OuterGrantType result = handleGrant(line_pa, grant, homeNode);

    // ---- M6: Clear outer txn pending and signal completion ----
    // The outer transaction is now complete; notify local EP_RNF
    // so any delayed HN snoop responses can be sent.
    if (_epRnfCtrl) {
        _epRnfCtrl->setOuterTxnPending(line_pa, false);
        _epRnfCtrl->signalOuterTxnComplete(line_pa);
    }

    return static_cast<int>(result);

    fatal("EPBackend node_id=%d: no UBCC available for remote miss "
          "PA=0x%lx\n", _nodeId, line_pa);
    return -1;
}

OuterGrantType
EPBackend::handleGrant(uint64_t line_pa, OuterGrantType grant, int homeNode)
{
    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: handleGrant PA=0x%lx grant=%d homeNode=%d\n",
            _nodeId, line_pa, static_cast<int>(grant), homeNode);

    auto it = _requesterLines.find(line_pa);
    if (it == _requesterLines.end()) {
        fatal("EPBackend node_id=%d: grant for unknown line PA=0x%lx\n",
              _nodeId, line_pa);
    }

    // Update requester bookkeeping based on grant
    switch (grant) {
        case OuterGrantType::GlobalGrantShared:
            it->second.state = RequesterLineState::R_S;
            DPRINTF(RubyCHIGeneric,
                    "EPBackend node_id=%d: line 0x%lx -> R_S\n",
                    _nodeId, line_pa);
            break;
        case OuterGrantType::GlobalGrantExclusive:
            it->second.state = RequesterLineState::R_E;
            DPRINTF(RubyCHIGeneric,
                    "EPBackend node_id=%d: line 0x%lx -> R_E (GrantExclusive)\n",
                    _nodeId, line_pa);
            break;
        case OuterGrantType::GlobalGrantModified:
            it->second.state = RequesterLineState::R_M;
            DPRINTF(RubyCHIGeneric,
                    "EPBackend node_id=%d: line 0x%lx -> R_M (GrantModified)\n",
                    _nodeId, line_pa);
            break;
        default:
            fatal("EPBackend node_id=%d: unknown grant type %d\n",
                  _nodeId, static_cast<int>(grant));
    }

    return grant;
}

// ---- M5 Inspection API ----

RequesterLineSnapshot
EPBackend::inspectRequesterState(uint64_t line_pa) const
{
    RequesterLineSnapshot snap;
    snap.valid = false;

    auto it = _requesterLines.find(line_pa);
    if (it != _requesterLines.end()) {
        snap.valid = true;
        snap.lineAddr = it->second.lineAddr;
        snap.state = static_cast<int>(it->second.state);
        snap.pendingReq = static_cast<int>(it->second.pendingReq);
        snap.writeIntent = it->second.writeIntent;
        snap.epoch = it->second.epoch;
        snap.homeNode = it->second.homeNode;
    }

    return snap;
}

void
EPBackend::recordSideband(uint64_t line_pa, int neededPerm, bool writeIntent,
                           int outerReqType, int grantResult, int homeNode)
{
    _lastSideband.valid = true;
    _lastSideband.lineAddr = line_pa;
    _lastSideband.neededPerm = neededPerm;
    _lastSideband.writeIntent = writeIntent;
    _lastSideband.outerReqType = outerReqType;
    _lastSideband.grantResult = grantResult;
    _lastSideband.homeNode = homeNode;
}

SidebandSnapshot
EPBackend::inspectLastSideband() const
{
    return _lastSideband;
}

void
EPBackend::clearSidebandSnapshot()
{
    _lastSideband.valid = false;
    _lastSideband.lineAddr = 0;
    _lastSideband.neededPerm = 0;
    _lastSideband.writeIntent = false;
    _lastSideband.outerReqType = -1;
    _lastSideband.grantResult = -1;
    _lastSideband.homeNode = -1;
}

// ---- M5 Phase 2: Diagnose Expected Grant ----

std::string
EPBackend::diagnoseExpectedGrant(int neededPerm, bool writeIntent) const
{
    if (neededPerm == 0) {
        // Shared → always GrantShared
        if (writeIntent) {
            return "ILLEGAL: Shared+writeIntent=true is invalid";
        }
        return "Shared";
    } else {
        // Unique
        if (writeIntent) {
            return "Modified";
        } else {
            return "Exclusive";
        }
    }
}

// ---- M6: Recall Management ----

bool
EPBackend::handleRecallRequest(const OuterRecallMsg &recallMsg)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: handleRecallRequest "
            "PA=0x%lx ownerNode=%d homeNode=%d epoch=%lu "
            "isRead=%d dataNeeded=%d\n",
            _nodeId, recallMsg.linePa, recallMsg.ownerNode,
            recallMsg.homeNode, recallMsg.epoch,
            recallMsg.isReadRequest, recallMsg.dataNeeded);

    // M6 P0-3: Validate — this node must be the recall target.
    // A mismatch indicates a routing bug or stale recall message;
    // silently processing is a correctness hazard.
    if (recallMsg.ownerNode != _nodeId) {
        fatal("EPBackend node_id=%d: recall target mismatch "
              "expected=%d got=%d\n",
              _nodeId, recallMsg.ownerNode, _nodeId);
    }

    // Store recall message for inspection
    _lastRecallMsg = recallMsg;
    _recallReceivedCount++;

    // ---- M7: Update requester-side bookkeeping ----
    // Recall result split:
    //   - Read recall → old owner downgrades to shared (R_S)
    //   - Unique/write recall → old owner invalidates (R_I)
    // P1-4: Use ownerLocalPa (owner's local PA) for _requesterLines lookup.
    // linePa is the home-node PA view and may not match the owner's local PA.
    {
        uint64_t lookupPa = (recallMsg.ownerLocalPa != 0)
                               ? recallMsg.ownerLocalPa
                               : recallMsg.linePa;
        auto it = _requesterLines.find(lookupPa);
        if (it != _requesterLines.end()) {
            if (recallMsg.isReadRequest) {
                // Downgrade to shared
                it->second.state = RequesterLineState::R_S;
            } else {
                // Invalidate
                it->second.state = RequesterLineState::R_I;
            }
        }
    }

    // In the single-gem5 prototype, the recall is processed immediately:
    // The owner node's EPBackend receives the recall and needs to:
    //   1. Trigger local HN coherent access (not yet wired in M6)
    //   2. Gather data from local cache/memory (simulated)
    //   3. Send response back to home UBCC
    //
    // For M6, we simulate the data path:
    //   - If dataNeeded is true (dirty owner), we mark dataReturned=true
    //   - The actual data content is currently dummy (zero) until
    //     the real CHI HN path is integrated (M7+)

    // Build the recall response
    OuterRecallResponse response;
    response.linePa = recallMsg.linePa;
    response.ownerNode = _nodeId;
    response.homeNode = recallMsg.homeNode;
    response.epoch = recallMsg.epoch;
    // For recall triggered by read: owner keeps shared copy
    // (data returned to requester via home)
    response.dataReturned = recallMsg.dataNeeded;
    response.ackReceived = true;

    // Send recall response back to home UBCC
    return sendRecallResponse(response);
}

bool
EPBackend::sendRecallResponse(const OuterRecallResponse &response)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: sendRecallResponse "
            "PA=0x%lx homeNode=%d dataReturned=%d\n",
            _nodeId, response.linePa, response.homeNode,
            response.dataReturned);

    // Store for inspection
    _lastRecallResponse = response;
    _recallResponseSentCount++;

    // Route response to home node's UBCC.
    // M6 P0-1: No fallback — the home UBCC must be registered.
    // Falling back to local _ubcc silently bypasses the home node's
    // directory and must never happen.
    UBCCController *homeUbcc = UBCCController::getInstance(response.homeNode);
    if (!homeUbcc) {
        fatal("EPBackend node_id=%d: home UBCC for node %d not found "
              "for recall response PA=0x%lx - "
              "cannot fall back to local UBCC\n",
              _nodeId, response.homeNode, response.linePa);
    }

    // Complete the recall at the home UBCC
    bool ok = homeUbcc->processRecallResponse(
        response.linePa, response.ownerNode, response.dataReturned,
        response.epoch);

    if (!ok) {
        warn("EPBackend node_id=%d: home UBCC rejected recall response "
             "PA=0x%lx\n", _nodeId, response.linePa);
    }

    return ok;
}

// ---- M7: Writeback / Evict ----

bool
EPBackend::handleWriteback(uint64_t line_pa, bool keepAsClean)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: handleWriteback PA=0x%lx "
            "keepAsClean=%d\n",
            _nodeId, line_pa, keepAsClean);

    if (!_addrMap.isDsm(_nodeId, line_pa)) {
        fatal("EPBackend node_id=%d: non-DSM address on writeback path "
              "PA=0x%lx\n", _nodeId, line_pa);
    }

    int homeNode = _addrMap.homeNode(_nodeId, line_pa);
    if (homeNode < 0) {
        fatal("EPBackend node_id=%d: invalid home node for writeback "
              "PA=0x%lx\n", _nodeId, line_pa);
    }

    // Translate PA to home node's view
    uint64_t offset = _addrMap.dsmOffset(line_pa);
    uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode, offset);

    // Look up requester entry to get epoch
    uint64_t epochVal = 0;
    auto it = _requesterLines.find(line_pa);
    if (it != _requesterLines.end()) {
        epochVal = it->second.epoch;
    } else {
        // Use epoch counter if no entry exists
        _epochCounter++;
        epochVal = _epochCounter;
    }

    // Build writeback message envelope
    _lastWritebackMsg.linePa = homePa;
    _lastWritebackMsg.requesterNode = _nodeId;
    _lastWritebackMsg.homeNode = homeNode;
    _lastWritebackMsg.epoch = epochVal;
    _lastWritebackMsg.keepAsClean = keepAsClean;

    // Route to home UBCC
    UBCCController *homeUbcc = UBCCController::getInstance(homeNode);
    if (!homeUbcc) {
        DPRINTF(RubyEP,
                "EPBackend node_id=%d: home UBCC for node %d not found, "
                "falling back to local UBCC\n", _nodeId, homeNode);
        homeUbcc = _ubcc;
    }
    if (!homeUbcc) {
        fatal("EPBackend node_id=%d: no UBCC available for writeback "
              "PA=0x%lx\n", _nodeId, line_pa);
    }

    bool ok = homeUbcc->processWriteback(homePa, _nodeId, epochVal, keepAsClean);

    // Build ack envelope
    _lastAckMsg.linePa = homePa;
    _lastAckMsg.homeNode = homeNode;
    _lastAckMsg.epoch = epochVal;
    _lastAckMsg.success = ok;

    // Update requester bookkeeping based on result
    if (ok) {
        _writebackCount++;
        if (it != _requesterLines.end()) {
            if (keepAsClean) {
                // Owner retains clean exclusive (G_E)
                it->second.state = RequesterLineState::R_E;
            } else {
                // Owner drops the line (R_I)
                it->second.state = RequesterLineState::R_I;
            }
        }
    }

    DPRINTF(RubyEP,
            "EPBackend node_id=%d: handleWriteback PA=0x%lx complete "
            "ok=%d keepAsClean=%d\n",
            _nodeId, line_pa, ok, keepAsClean);

    return ok;
}

bool
EPBackend::handleEvict(uint64_t line_pa)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: handleEvict PA=0x%lx\n",
            _nodeId, line_pa);

    if (!_addrMap.isDsm(_nodeId, line_pa)) {
        fatal("EPBackend node_id=%d: non-DSM address on evict path "
              "PA=0x%lx\n", _nodeId, line_pa);
    }

    int homeNode = _addrMap.homeNode(_nodeId, line_pa);
    if (homeNode < 0) {
        fatal("EPBackend node_id=%d: invalid home node for evict "
              "PA=0x%lx\n", _nodeId, line_pa);
    }

    // Translate PA to home node's view
    uint64_t offset = _addrMap.dsmOffset(line_pa);
    uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode, offset);

    // Look up requester entry to get epoch
    uint64_t epochVal = 0;
    auto it = _requesterLines.find(line_pa);
    if (it != _requesterLines.end()) {
        epochVal = it->second.epoch;
    } else {
        _epochCounter++;
        epochVal = _epochCounter;
    }

    // Build evict message envelope
    _lastEvictMsg.linePa = homePa;
    _lastEvictMsg.evictingNode = _nodeId;
    _lastEvictMsg.homeNode = homeNode;
    _lastEvictMsg.epoch = epochVal;

    // Route to home UBCC
    UBCCController *homeUbcc = UBCCController::getInstance(homeNode);
    if (!homeUbcc) {
        DPRINTF(RubyEP,
                "EPBackend node_id=%d: home UBCC for node %d not found, "
                "falling back to local UBCC\n", _nodeId, homeNode);
        homeUbcc = _ubcc;
    }
    if (!homeUbcc) {
        fatal("EPBackend node_id=%d: no UBCC available for evict "
              "PA=0x%lx\n", _nodeId, line_pa);
    }

    bool ok = homeUbcc->processEvict(homePa, _nodeId, epochVal);

    // Build ack envelope
    _lastAckMsg.linePa = homePa;
    _lastAckMsg.homeNode = homeNode;
    _lastAckMsg.epoch = epochVal;
    _lastAckMsg.success = ok;

    if (ok) {
        _evictCount++;
        if (it != _requesterLines.end()) {
            // Evict means the line is dropped from this node
            it->second.state = RequesterLineState::R_I;
        }
    }

    DPRINTF(RubyEP,
            "EPBackend node_id=%d: handleEvict PA=0x%lx complete ok=%d\n",
            _nodeId, line_pa, ok);

    return ok;
}

uint64_t
EPBackend::getStaleRejectedCount() const
{
    if (!_ubcc)
        return 0;
    return _ubcc->getStaleEpochRejectedCount();
}

void
EPBackend::resetStaleRejectedCount()
{
    if (_ubcc)
        _ubcc->resetStaleEpochRejectedCount();
}

uint64_t
EPBackend::getOwnerMismatchRejectedCount() const
{
    if (!_ubcc)
        return 0;
    return _ubcc->getOwnerMismatchRejectedCount();
}

void
EPBackend::resetOwnerMismatchRejectedCount()
{
    if (_ubcc)
        _ubcc->resetOwnerMismatchRejectedCount();
}

// ---- M8: Global Invalidation Management ----

bool
EPBackend::handleInvalidationRequest(const OuterInvalidateMsg &invMsg)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: handleInvalidationRequest "
            "PA=0x%lx sharerNode=%d homeNode=%d epoch=%lu\n",
            _nodeId, invMsg.linePa, invMsg.sharerNode,
            invMsg.homeNode, invMsg.epoch);

    // M8 P0-1: Validate — this node must be the invalidation target.
    if (invMsg.sharerNode != _nodeId) {
        fatal("EPBackend node_id=%d: invalidation target mismatch "
              "expected=%d got=%d\n",
              _nodeId, invMsg.sharerNode, _nodeId);
    }

    // Store invalidation message for inspection
    _lastInvalidateMsg = invMsg;
    _invalidationReceivedCount++;

    // In the single-gem5 prototype, invalidate the requester-side
    // bookkeeping immediately.
    // P1-4: Use sharerLocalPa (sharer's local PA) for _requesterLines lookup.
    uint64_t lookupPa = (invMsg.sharerLocalPa != 0)
                           ? invMsg.sharerLocalPa
                           : invMsg.linePa;
    {
        auto it = _requesterLines.find(lookupPa);
        if (it != _requesterLines.end()) {
            // Invalidation: line is downgraded to invalid
            it->second.state = RequesterLineState::R_I;
        }
    }

    // Build invalidation ack
    OuterInvalidationAck ack;
    ack.linePa = invMsg.linePa;
    ack.ackNode = _nodeId;
    ack.homeNode = invMsg.homeNode;
    ack.epoch = invMsg.epoch;
    ack.success = true;

    // Send ack back to home UBCC
    return sendInvalidationAck(ack);
}

bool
EPBackend::sendInvalidationAck(const OuterInvalidationAck &ack)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: sendInvalidationAck "
            "PA=0x%lx ackNode=%d homeNode=%d epoch=%lu\n",
            _nodeId, ack.linePa, ack.ackNode,
            ack.homeNode, ack.epoch);

    // Store for inspection
    _lastInvalidationAck = ack;
    _invalidationAckSentCount++;

    // Route ack to home node's UBCC
    UBCCController *homeUbcc = UBCCController::getInstance(ack.homeNode);
    if (!homeUbcc) {
        fatal("EPBackend node_id=%d: home UBCC for node %d not found "
              "for invalidation ack PA=0x%lx\n",
              _nodeId, ack.homeNode, ack.linePa);
    }

    bool ok = homeUbcc->processInvalidationAck(
        ack.linePa, ack.ackNode, ack.epoch);

    if (!ok) {
        warn("EPBackend node_id=%d: home UBCC rejected invalidation ack "
             "PA=0x%lx\n", _nodeId, ack.linePa);
    }

    return ok;
}

} // namespace ruby
} // namespace gem5
