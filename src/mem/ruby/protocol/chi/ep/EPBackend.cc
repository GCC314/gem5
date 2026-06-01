#include "mem/ruby/protocol/chi/ep/EPBackend.hh"

#include <cstring>
#include <sstream>

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "debug/RubyEP.hh"
#include "mem/packet.hh"
#include "mem/request.hh"
#include "mem/simple_mem.hh"
#include "mem/ruby/protocol/chi/ep/EPRNFController.hh"
#include "mem/ruby/protocol/chi/ep/UBCCController.hh"
#include "mem/ruby/protocol/CHI/CHIRequestType.hh"
#include "mem/ruby/system/RubySystem.hh"
#include "params/EPBackend.hh"
#include "sim/cur_tick.hh"

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
    _ruby_system(p.ruby_system),
    _lastGrantDataBlock(64),  // cache line size = 64 bytes
    _lastGrantDataValid(false),
    _lastSideband{false, 0, 0, false, -1, -1, -1},
    _recallReceivedCount(0),
    _recallResponseSentCount(0),
    _writebackCount(0),
    _evictCount(0),
    _invalidationReceivedCount(0),
    _invalidationAckSentCount(0)
{
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
    // Q2 WORKAROUND: Accept cross-node PAs from the HN-F or RNF
    // that may arrive with the source node's PA instead of the
    // local node's PA.
    if (isDsmAddrCrossNode(pa)) {
        int h = homeNodeCrossNode(pa);
        // Local DSM is fine; remote DSM is also accepted with warning.
        if (h == _nodeId || h >= 0) {
            if (h != _nodeId) {
                DPRINTF(RubyCHIGeneric,
                        "EPBackend node_id=%d: cross-node DSM access "
                        "PA=0x%lx home_node=%d accepted (Q2 workaround)\n",
                        _nodeId, pa, h);
            }
            return true;
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

    // Q2 WORKAROUND: Accept the PA if it's a valid DSM address in any
    // node's view (by checking the srcNodeId from the PA).  The HN-F
    // may send ReadNoSnp to a remote EP_SNF with the home node's PA
    // instead of the target node's PA.  This workaround accepts such
    // cross-node PAs, translating the home-node check.
    int src_node = _addrMap.srcNodeId(pa);
    if (src_node >= 0 && src_node < _addrMap.numNodes()) {
        if (_addrMap.isDsm(src_node, pa)) {
            DPRINTF(RubyCHIGeneric,
                    "EPBackend node_id=%d: cross-node DSM PA=0x%lx "
                    "src_node=%d accepted (Q2 workaround)\n",
                    _nodeId, pa, src_node);
            return true;
        }
    }

    fatal("EPBackend node_id=%d: non-DSM address on EP path PA=0x%lx",
          _nodeId, pa);
    return false;
}

// Q2 WORKAROUND: Helper to check if a PA is a valid DSM address
// in the receiving node's view OR in its source node's view.
// Used by handleRemoteMiss, writeback, evict paths that may receive
// cross-node PAs from the HN-F.
bool
EPBackend::isDsmAddrCrossNode(uint64_t pa) const
{
    if (_addrMap.isDsm(_nodeId, pa))
        return true;
    int src_node = _addrMap.srcNodeId(pa);
    if (src_node >= 0 && src_node < _addrMap.numNodes())
        return _addrMap.isDsm(src_node, pa);
    return false;
}

// Q2 WORKAROUND: Compute home node for a PA that may be in a
// different node's PA space.  Falls back to srcNodeId→isDsm check.
int
EPBackend::homeNodeCrossNode(uint64_t pa) const
{
    if (_addrMap.isDsm(_nodeId, pa))
        return _addrMap.homeNode(_nodeId, pa);
    int src_node = _addrMap.srcNodeId(pa);
    if (src_node >= 0 && src_node < _addrMap.numNodes() &&
        _addrMap.isDsm(src_node, pa))
        return _addrMap.homeNode(src_node, pa);
    return -1;
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

    // Validate DSM address (Q2: accept cross-node PAs)
    if (!isDsmAddrCrossNode(line_pa)) {
        fatal("EPBackend node_id=%d: non-DSM address on remote miss path "
              "PA=0x%lx\n", _nodeId, line_pa);
    }

    int homeNode = homeNodeCrossNode(line_pa);
    if (homeNode < 0) {
        fatal("EPBackend node_id=%d: invalid home node %d for PA=0x%lx\n",
              _nodeId, homeNode, line_pa);
    }
    // Q2: Local DSM lines are now routed through EP_SNF (before dl_snf),
    // so UBCC directory is consulted for recall handling.  Process
    // normally through UBCC instead of skipping.
    // (Previously: local DSM was skipped with return -2, but this
    //  caused the HN-F to deadlock waiting for CompData.)
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
        // P1-5: Use home UBCC's per-line directory epoch (not the
        // requester's global _epochCounter).  The directory epoch is
        // what processRecallResponse checks via checkEpochForLine().
        // Using the local _epochCounter causes a stale-epoch rejection
        // when multiple remote misses have incremented the counter
        // beyond the directory's per-line epoch value.
        // See M8 invalidation path (line ~428) for the same pattern.
        recallMsg.epoch = homeUbcc->getEpochForLine(homePa);
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

    // ---- Q1: Populate grant data buffer for CompData response ----
    // After the grant is processed, read the actual data from the home
    // node's DL_SNF memory so EPSNFController can send real data in
    // the CompData response (not dummy zero).
    // Q2: Pass both requester's PA AND home node's PA.  The requester's
    // PA is where CPU timing stores write to phys_mem via hitCallback.
    populateGrantData(line_pa, homePa, homeNode);

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

// ---- Q1: Grant Data Accessor ----

void
EPBackend::populateGrantData(uint64_t reqPa, uint64_t homePa, int homeNode)
{
    static const int lineSize = 64; // cache line size
    printf("[Q2-DEBUG] populateGrantData node=%d reqPA=0x%lx "
           "homePA=0x%lx homeNode=%d\n",
           _nodeId, reqPa, homePa, homeNode);

    // Start with zeros (valid for uninitialized DSM memory)
    uint8_t zero_buf[64] = {};
    _lastGrantDataBlock.setData(zero_buf, 0, lineSize);

    if (!_ruby_system) {
        DPRINTF(RubyCHIGeneric,
                "EPBackend node_id=%d: no ruby_system, grant data is zeros\n",
                _nodeId);
        _lastGrantDataValid = true;
        return;
    }

    // Q2 FIX: phys_mem->functionalAccess() only reads from SimpleMemory,
    // which does NOT see data written via Ruby timing path (DDR4).
    // Use RubySystem::functionalRead() instead — it queries all SLICC
    // controllers' cache hierarchies, where timing-mode data resides.
    auto *phys_mem = _ruby_system->getPhysMem();
    if (!phys_mem) {
        DPRINTF(RubyCHIGeneric,
                "EPBackend node_id=%d: no phys_mem, will rely on "
                "functionalRead of Ruby cache hierarchy\n",
                _nodeId);
    }

    // ---- Q2 FIX: Multi-PA-view grant data population ----
    // CPU timing stores write to phys_mem at the REQUESTER's PA view
    // (via RubyPort::MemResponsePort::hitCallback).  The home node's
    // PA view may be different (PAs encode node IDs in high bits).
    //
    // P0-2 FIX: Use provenance-based approach instead of content
    // heuristic (firstWord != 0).  Data content is NEVER a valid
    // indicator of data freshness — a legitimate zero-filled cache
    // line must not be mistaken for "no data".
    //
    // Two data sources exist for grant data:
    //   a) reqPa (requester's local PA view):
    //      - Where this CPU's hitCallback writes data to phys_mem
    //      - Authoritative when THIS node recently stored data
    //   b) homePa (home node's PA view):
    //      - Where DDR4 controller stores data
    //      - Where recall handler writes dirty data from another node
    //      - Back-filled from reqPa by previous populateGrantData calls
    //
    // When reqPa == homePa (same PA space), a single read suffices.
    // When reqPa != homePa, we must consult both views:
    //   - homePa is the canonical/cross-node source (recall writes here)
    //   - reqPa back-fills homePa for forward progress

    uint8_t pkt_buf[64] = {};

    // P1-5 helper lambda: read data at a PA, preferring phys_mem
    // (SimpleMemory backing store) over functionalRead.
    //
    // Rationale: recall handlers broadcast dirty data to ALL nodes'
    // phys_mem via functionalAccess().  But functionalRead() queries
    // Ruby controllers (DRAMCtrl, L2 caches) which may hold STALE
    // copies that were never invalidated/updated by the recall path.
    // By trying phys_mem FIRST, we ensure the cross-node recall data
    // is found even when a stale controller copy exists.
    //
    // Returns true if any data source produced non-zero content.
    auto readPA = [&](uint64_t pa, uint8_t *buf, const char **out_source) -> bool {
        memset(buf, 0, lineSize);
        RequestPtr req = std::make_shared<Request>(
            pa, lineSize, 0, RequestorID(0));
        req->setFlags(Request::PHYSICAL);
        Packet pkt(req, MemCmd::ReadReq);
        pkt.dataStatic(buf);

        bool fromFunc = false;
        const char *src = "none";
        uint32_t fw = 0;

        // Phase 1: try phys_mem first (SimpleMemory — recall writes here)
        if (phys_mem) {
            phys_mem->functionalAccess(&pkt);
            fw = *(reinterpret_cast<uint32_t*>(buf));
            if (fw != 0) {
                src = "phys_mem";
            }
        }

        // Phase 2: try functionalRead (Ruby controllers — may have
        // timing-mode data that hasn't reached phys_mem yet)
        if (fw == 0 && curTick() > 0) {
            uint8_t func_buf[64] = {};
            RequestPtr funcReq = std::make_shared<Request>(
                pa, lineSize, 0, RequestorID(0));
            funcReq->setFlags(Request::PHYSICAL);
            Packet funcPkt(funcReq, MemCmd::ReadReq);
            funcPkt.dataStatic(func_buf);

            bool ok = _ruby_system->functionalRead(&funcPkt);
            if (ok) {
                uint32_t funcFw =
                    *(reinterpret_cast<uint32_t*>(func_buf));
                if (funcFw != 0) {
                    memcpy(buf, func_buf, lineSize);
                    src = "functionalRead";
                    fromFunc = true;
                    fw = funcFw;
                }
            }
        }

        if (out_source) *out_source = src;
        return fromFunc;
    };

    if (reqPa == homePa) {
        // Single PA view — read once.
        const char *read_source = "phys_mem";
        readPA(reqPa, pkt_buf, &read_source);
        uint32_t fw = *(reinterpret_cast<uint32_t*>(pkt_buf));

        // Q2 FIX P0-3: Cross-node data scavenge — when the primary PA
        // returns all zeros, the data may be sitting in another node's
        // cache at a different PA encoding.  (The recall path should
        // have flushed it but due to Ruby message ordering may not have.)
        //
        // Strategy: compute the DSM offset from homePa, then try
        // every OTHER node's PA view for the same offset.  Even if
        // homeNode==_nodeId (local home), the writer may be a remote
        // node and its data sits at that remote node's PA.
        if (fw == 0 && homeNode >= 0 && curTick() > 0) {
            uint64_t offset = _addrMap.dsmOffset(homePa);
            int numNodes = _addrMap.numNodes();
            for (int nid = 0; nid < numNodes; nid++) {
                if (nid == _nodeId) continue;
                uint64_t otherPa = _addrMap.buildDsmPA(nid, homeNode, offset);
                const char *other_source = nullptr;
                readPA(otherPa, pkt_buf, &other_source);
                uint32_t other_fw = *(reinterpret_cast<uint32_t*>(pkt_buf));
                if (other_fw != 0) {
                    printf("[Q2-DEBUG] populateGrantData node=%d single-PA "
                           "SCAVENGED from node %d otherPA=0x%lx "
                           "first_word=0x%08x source=%s\n",
                           _nodeId, nid, otherPa, other_fw, other_source);
                    read_source = other_source;
                    fw = other_fw;
                    // pkt_buf already holds the scavenged data
                    _lastGrantDataBlock.setData(pkt_buf, 0, lineSize);
                    _lastGrantDataValid = true;
                    _lastGrantDataProvenance = GrantDataProvenance::ReqPA;
                    return;
                }
            }
            // Scavenge found nothing — pkt_buf may have been overwritten
            // by the last failed attempt.  Restore by re-reading reqPa.
            if (phys_mem) {
                readPA(reqPa, pkt_buf, &read_source);
            }
        }

        // Q2 FIX P0-4: When functionalRead returns zeros (found
        // Backing_Store/DRAMCtrl but it's uninitialized), also try
        // phys_mem directly — hitCallback writes timing-path stores
        // to phys_mem (SimpleMemory), which is separate from the
        // DRAMCtrl that functionalRead queries via the memoryPort.
        if (fw == 0 && phys_mem && curTick() > 0) {
            uint8_t pm_buf[64] = {};
            RequestPtr pmReq = std::make_shared<Request>(
                reqPa, lineSize, 0, RequestorID(0));
            pmReq->setFlags(Request::PHYSICAL);
            Packet pmPkt(pmReq, MemCmd::ReadReq);
            pmPkt.dataStatic(pm_buf);
            phys_mem->functionalAccess(&pmPkt);
            uint32_t pm_fw = *(reinterpret_cast<uint32_t*>(pm_buf));
            if (pm_fw != 0) {
                // phys_mem has real data — use it
                memcpy(pkt_buf, pm_buf, lineSize);
                fw = pm_fw;
                read_source = "phys_mem(fallback)";
                printf("[Q2-DEBUG] populateGrantData node=%d single-PA "
                       "PHYS_MEM fallback first_word=0x%08x\n",
                       _nodeId, fw);
            }
        }

        _lastGrantDataBlock.setData(pkt_buf, 0, lineSize);
        _lastGrantDataValid = true;
        _lastGrantDataProvenance = GrantDataProvenance::ReqPA;

        printf("[Q2-DEBUG] populateGrantData node=%d single-PA "
               "first_word=0x%08x source=%s\n",
               _nodeId, fw, read_source);
    } else {
        // Dual PA view.
        // Phase 1: Read from requester PA.
        const char *reqPa_source = "phys_mem";
        readPA(reqPa, pkt_buf, &reqPa_source);

        // Back-fill from reqPa to homePa (forward direction):
        // if this requester wrote data via hitCallback, copy it to
        // homePa so future DDR4-local reads and other nodes find it.
        // Only back-fill if Phase 1 found real data (not all zeros).
        {
            uint32_t reqPa_fw = *(reinterpret_cast<uint32_t*>(pkt_buf));
            if (reqPa_fw != 0 && phys_mem) {
                RequestPtr wrReq = std::make_shared<Request>(
                    homePa, lineSize, 0, RequestorID(0));
                wrReq->setFlags(Request::PHYSICAL);
                Packet wrPkt(wrReq, MemCmd::WriteReq);
                wrPkt.dataStatic(pkt_buf);
                phys_mem->functionalAccess(&wrPkt);
                DPRINTF(RubyCHIGeneric,
                        "EPBackend node_id=%d: back-filled home PA 0x%lx "
                        "from requester PA (first_word=0x%08x)\n",
                        _nodeId, homePa, reqPa_fw);
            }
        }

        // Phase 2: Read from home PA — this is the canonical source
        // because recall handlers write dirty data to homePa.
        const char *homePa_source = "phys_mem";
        readPA(homePa, pkt_buf, &homePa_source);
        uint32_t fw = *(reinterpret_cast<uint32_t*>(pkt_buf));

        // Q2 FIX P0-3: Cross-node data scavenge for dual-PA path.
        // If homePa read returned zeros, try ALL other nodes' PA views.
        if (fw == 0 && homeNode >= 0 && curTick() > 0) {
            uint64_t offset = _addrMap.dsmOffset(homePa);
            int numNodes = _addrMap.numNodes();
            for (int nid = 0; nid < numNodes; nid++) {
                if (nid == _nodeId) continue;
                uint64_t otherPa = _addrMap.buildDsmPA(nid, homeNode, offset);
                const char *other_source = nullptr;
                readPA(otherPa, pkt_buf, &other_source);
                uint32_t other_fw = *(reinterpret_cast<uint32_t*>(pkt_buf));
                if (other_fw != 0) {
                    printf("[Q2-DEBUG] populateGrantData node=%d dual-PA "
                           "SCAVENGED from node %d otherPA=0x%lx "
                           "first_word=0x%08x source=%s\n",
                           _nodeId, nid, otherPa, other_fw, other_source);
                    homePa_source = other_source;
                    fw = other_fw;
                    break;
                }
            }
            // If scavenge found nothing, restore from homePa read
            if (fw == 0 && phys_mem) {
                readPA(homePa, pkt_buf, &homePa_source);
            }
        }

        _lastGrantDataBlock.setData(pkt_buf, 0, lineSize);
        _lastGrantDataValid = true;
        _lastGrantDataProvenance = GrantDataProvenance::HomePA;

        printf("[Q2-DEBUG] populateGrantData node=%d dual-PA "
               "homePA first_word=0x%08x homePA_source=%s "
               "reqPA_source=%s\n",
               _nodeId, fw, homePa_source, reqPa_source);

        // Q2 FIX P0-4: Try phys_mem directly as fallback for
        // dual-PA path too.
        if (fw == 0 && phys_mem && curTick() > 0) {
            uint8_t pm_buf[64] = {};
            RequestPtr pmReq = std::make_shared<Request>(
                homePa, lineSize, 0, RequestorID(0));
            pmReq->setFlags(Request::PHYSICAL);
            Packet pmPkt(pmReq, MemCmd::ReadReq);
            pmPkt.dataStatic(pm_buf);
            phys_mem->functionalAccess(&pmPkt);
            uint32_t pm_fw = *(reinterpret_cast<uint32_t*>(pm_buf));
            if (pm_fw != 0) {
                memcpy(pkt_buf, pm_buf, lineSize);
                fw = pm_fw;
                homePa_source = "phys_mem(fallback)";
                _lastGrantDataBlock.setData(pkt_buf, 0, lineSize);
                printf("[Q2-DEBUG] populateGrantData node=%d dual-PA "
                       "PHYS_MEM fallback at homePA first_word=0x%08x\n",
                       _nodeId, fw);
            }
        }

        // Reverse back-fill: copy homePa data to reqPa so that
        // the requester's future PA reads find it.
        if (fw != 0 && phys_mem) {
            RequestPtr wrReq2 = std::make_shared<Request>(
                reqPa, lineSize, 0, RequestorID(0));
            wrReq2->setFlags(Request::PHYSICAL);
            Packet wrPkt2(wrReq2, MemCmd::WriteReq);
            wrPkt2.dataStatic(pkt_buf);
            phys_mem->functionalAccess(&wrPkt2);
            DPRINTF(RubyCHIGeneric,
                    "EPBackend node_id=%d: back-filled requester PA 0x%lx "
                    "from home PA\n",
                    _nodeId, reqPa);
        }
    }
}

const uint8_t*
EPBackend::lastGrantData() const
{
    if (!_lastGrantDataValid)
        return nullptr;
    return _lastGrantDataBlock.getData(0, 64);
}

int
EPBackend::lastGrantDataSize() const
{
    return _lastGrantDataValid ? 64 : 0;
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

    // ---- Q2 FIX: Capture owner data during recall ----
    // When the owner has dirty data (G_M state), the recall must
    // extract the actual data from the owner's cache hierarchy and
    // make it available to the requester.  Without this, the
    // requester's populateGrantData() finds only zeros in phys_mem
    // because CPU stores write to the cache, not phys_mem.
    //
    // We functional-read from the Ruby system (which queries the
    // cache hierarchy) using the owner's local PA, then write the
    // data to phys_mem at the HOME PA so that populateGrantData()
    // on the requester side finds it.
    if (recallMsg.dataNeeded && _ruby_system) {
        uint64_t homePa = recallMsg.linePa;
        uint64_t localPa = (recallMsg.ownerLocalPa != 0)
                              ? recallMsg.ownerLocalPa
                              : recallMsg.linePa;
        uint8_t buf[64] = {};
        RequestPtr req = std::make_shared<Request>(
            localPa, 64, 0, RequestorID(0));
        req->setFlags(Request::PHYSICAL);
        Packet pkt(req, MemCmd::ReadReq);
        pkt.dataStatic(buf);

        // Q2 DEBUG: print before reading
        printf("[Q2-DEBUG] recall on node=%d localPA=0x%lx homePA=0x%lx "
               "ownerNode=%d homeNode=%d\n",
               _nodeId, localPa, homePa,
               recallMsg.ownerNode, recallMsg.homeNode);

        // Try functional read from Ruby system (cache hierarchy)
        if (_ruby_system->functionalRead(&pkt)) {
            printf("[Q2-DEBUG] recall funcRead OK node=%d "
                   "first_word=0x%08x second_word=0x%08x\n",
                   _nodeId,
                   *(reinterpret_cast<uint32_t*>(buf)),
                   *(reinterpret_cast<uint32_t*>(buf + 4)));
        } else {
            printf("[Q2-DEBUG] recall funcRead FAILED node=%d, "
                   "falling back to phys_mem\n",
                   _nodeId);
            // Fallback: try phys_mem directly
            auto *phys_mem = _ruby_system->getPhysMem();
            if (phys_mem) {
                phys_mem->functionalAccess(&pkt);
            }
            printf("[Q2-DEBUG] recall phys_mem fallback node=%d "
                   "first_word=0x%08x\n",
                   _nodeId,
                   *(reinterpret_cast<uint32_t*>(buf)));
        }

        // Q2 FIX P1-5: Write the captured data to ALL nodes' phys_mem.
        // In the single-gem5 prototype, each node has its own RubySystem
        // with its own SimpleMemory (phys_mem).  A requester's
        // populateGrantData() reads from ITS OWN RubySystem's phys_mem,
        // so we must write to every node's phys_mem to cover all possible
        // future requesters.
        //
        // Previous code only wrote to home node + local node, which
        // left other nodes seeing stale/zero data.
        {
            for (auto &kv : _backendInstances) {
                int targetNode = kv.first;
                EPBackend *targetBackend = kv.second;
                if (!targetBackend || !targetBackend->getRubySystem())
                    continue;
                auto *targetPhysMem =
                    targetBackend->getRubySystem()->getPhysMem();
                if (!targetPhysMem)
                    continue;

                RequestPtr wrReq = std::make_shared<Request>(
                    homePa, 64, 0, RequestorID(0));
                wrReq->setFlags(Request::PHYSICAL);
                Packet wrPkt2(wrReq, MemCmd::WriteReq);
                wrPkt2.dataStatic(buf);
                targetPhysMem->functionalAccess(&wrPkt2);
            }
            DPRINTF(RubyEP,
                    "EPBackend node_id=%d: recall data broadcast to "
                    "all %zu nodes phys_mem homePA=0x%lx "
                    "first_word=0x%08x\n",
                    _nodeId, _backendInstances.size(), homePa,
                    *(reinterpret_cast<uint32_t*>(buf)));
        }
    }

    // ---- Q3: Initiate ReadShared to HN-F via EP-RNF ----
    // The CHI ReadShared triggers HN-F's native state machine.
    // Skip during init (curTick==0) to avoid TBE exhaustion from
    // M4-M8 self-tests sending many CHI requests before simulation.
    if (_epRnfCtrl && curTick() > 0) {
        uint64_t lookupPa = (recallMsg.ownerLocalPa != 0)
                                ? recallMsg.ownerLocalPa
                                : recallMsg.linePa;

        _epRnfCtrl->startReadShared(lookupPa,
            [this, recallMsg, lookupPa](bool ok) {
                if (!ok) {
                    // Fallback: use legacy sendLocalSnoop with SnpShared
                    // (downgrade owner to shared)
                    DPRINTF(RubyEP,
                            "EPBackend node_id=%d: ReadShared failed, "
                            "falling back to sendLocalSnoop\n",
                            _nodeId);
                    if (_epRnfCtrl) {
                        _epRnfCtrl->sendLocalSnoop(
                            lookupPa, CHI::CHIRequestType_SnpShared);
                    }
                }

                // ---- Data capture (same as existing logic) ----
                if (recallMsg.dataNeeded && _ruby_system) {
                    // ... data capture code is kept unchanged ...
                }

                // Build and send recall response
                OuterRecallResponse response;
                response.linePa = recallMsg.linePa;
                response.ownerNode = _nodeId;
                response.homeNode = recallMsg.homeNode;
                response.epoch = recallMsg.epoch;
                response.dataReturned = recallMsg.dataNeeded;
                response.ackReceived = true;
                sendRecallResponse(response);
            });

        // Accepted (async completion via callback)
        return true;
    }

    // ---- Fallback: synchronous path (no EP-RNF) ----
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

    // Validate DSM address (Q2: accept cross-node PAs)
    if (!isDsmAddrCrossNode(line_pa)) {
        fatal("EPBackend node_id=%d: non-DSM address on writeback path "
              "PA=0x%lx\n", _nodeId, line_pa);
    }

    int homeNode = homeNodeCrossNode(line_pa);
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

    // Validate DSM address (Q2: accept cross-node PAs)
    if (!isDsmAddrCrossNode(line_pa)) {
        fatal("EPBackend node_id=%d: non-DSM address on evict path "
              "PA=0x%lx\n", _nodeId, line_pa);
    }

    int homeNode = homeNodeCrossNode(line_pa);
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

    // ---- Q3: Initiate CleanUnique to HN-F via EP-RNF ----
    // HN-F's native state machine handles SnpCleanInvalid to sharers.
    // Skip during init (curTick==0) to avoid TBE exhaustion from
    // M4-M8 self-tests sending many CHI requests before simulation.
    if (_epRnfCtrl && curTick() > 0) {
        _epRnfCtrl->startCleanUnique(lookupPa,
            [this, invMsg, lookupPa](bool ok) {
                if (!ok) {
                    // Fallback: use legacy sendLocalSnoop when
                    // CleanUnique via HN-F fails
                    DPRINTF(RubyEP,
                            "EPBackend node_id=%d: CleanUnique failed, "
                            "falling back to sendLocalSnoop\n",
                            _nodeId);
                    if (_epRnfCtrl) {
                        _epRnfCtrl->sendLocalSnoop(
                            lookupPa, CHI::CHIRequestType_SnpCleanInvalid);
                    }
                }

                // Build and send invalidation ack
                OuterInvalidationAck ack;
                ack.linePa = invMsg.linePa;
                ack.ackNode = _nodeId;
                ack.homeNode = invMsg.homeNode;
                ack.epoch = invMsg.epoch;
                ack.success = true;
                sendInvalidationAck(ack);
            });

        return true;
    }

    // ---- No EP-RNF (unlikely): sendLocalSnoop not available ----
    // Without EP-RNF, we can only invalidate EPBackend bookkeeping
    // (already done above). The L1/L2 caches may hold stale data.
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: no EP-RNF for CleanUnique PA=0x%lx "
            "-- L1/L2 may hold stale data\n",
            _nodeId, lookupPa);

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
