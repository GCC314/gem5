#include "mem/ruby/protocol/chi/ep/EPBackend.hh"

#include <cstdio>
#include <cstring>
#include <execinfo.h>
#include <sstream>
#include <unistd.h>

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

// ---- F3: HomeMemoryService method implementations ----
bool HomeMemoryService::read(uint64_t homePa, uint8_t *buf, int size) const
{
    if (!physMem) {
        memset(buf, 0, size);
        return false;
    }
    memset(buf, 0, size);
    RequestPtr req = std::make_shared<Request>(homePa, size, 0, RequestorID(0));
    req->setFlags(Request::PHYSICAL);
    Packet pkt(req, MemCmd::ReadReq);
    pkt.dataStatic(buf);
    physMem->functionalAccess(&pkt);
    return true;
}

bool HomeMemoryService::write(uint64_t homePa, const uint8_t *buf, int size) const
{
    if (!physMem) return false;
    RequestPtr req = std::make_shared<Request>(homePa, size, 0, RequestorID(0));
    req->setFlags(Request::PHYSICAL);
    Packet pkt(req, MemCmd::WriteReq);
    pkt.dataStatic(const_cast<uint8_t*>(buf));
    physMem->functionalAccess(&pkt);
    return true;
}

// ---- F3: End HomeMemoryService ----

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

    // v4: Check for existing requester entry (retry after BUSY/recall).
    // If found, reuse epoch/reqId so that Clear matches GRANT_HANDSHAKE.
    auto existing = _requesterLines.find(line_pa);

    // Same-node duplicate ReadShared coalescing (remote homes only).
    // another local CPU may miss on the same remote line after a sibling CPU
    // already obtained R_S/R_E/R_M.  Issuing a brand-new outer request here
    // clobbers the stable requester-line state back to R_WAIT_GRANT and can
    // enqueue a pointless duplicate behind a foreign requester, which is what
    // drives the TC6/TC11 dup_retry stall.  Instead, report BUSY and let HN-F
    // retry once the earlier fill becomes visible in the local hierarchy.
    //
    // F12: Only coalesce for REMOTE homes (homeNode != _nodeId).
    // Local-home DSM reads must always go through UBCC for directory tracking.
    if (neededPerm == 0 && existing != _requesterLines.end() &&
        homeNode != _nodeId) {
        RequesterLineState st = existing->second.state;
        if (st == RequesterLineState::R_S ||
            st == RequesterLineState::R_E ||
            st == RequesterLineState::R_M) {
            DPRINTF(RubyEP,
                    "EPBackend node_id=%d: duplicate shared miss PA=0x%lx "
                    "while requester state=%d already covers it — retry local\n",
                    _nodeId, line_pa, static_cast<int>(st));
            return -1;
        }
    }

    bool isRetry = (existing != _requesterLines.end() &&
                    existing->second.state == RequesterLineState::R_WAIT_GRANT);

    uint64_t reqIdVal;
    RequesterLineEntry entry;
    if (isRetry) {
        reqIdVal = existing->second.reqId;
        entry = existing->second;   // preserve original epoch/reqId
        entry.pendingReq = reqType;
        entry.writeIntent = writeIntent;
        entry.homeNode = homeNode;
    } else {
        _epochCounter++;
        reqIdVal = _epochCounter;  // v4: monotonic reqId from epoch counter
        entry.lineAddr = line_pa;
        entry.state = RequesterLineState::R_WAIT_GRANT;
        entry.pendingReq = reqType;
        entry.epoch = _epochCounter;
        entry.reqId = reqIdVal;    // v4: store reqId
        entry.writeIntent = writeIntent;
        entry.homeNode = homeNode;
    }
    _requesterLines[line_pa] = entry;

    // Dispatch to home node's UBCC via cross-node registry.
    UBCCController *homeUbcc = UBCCController::getInstance(homeNode);
    if (!homeUbcc) {
        if (homeNode != _nodeId) {
            fatal("EPBackend node_id=%d: remote UBCC for homeNode=%d "
                  "not registered (local UBCC is NOT a valid fallback "
                  "for cross-node access).  Check UBCC registry.\n",
                  _nodeId, homeNode);
        }
        // Only allow fallback when home IS the local node
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
    reqEnv.reqId = reqIdVal;  // v4: requester-allocated reqId
    _lastReqEnv = reqEnv;

    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: outer request envelope "
            "linePa=0x%lx reqType=%d writeIntent=%d srcNode=%d epoch=%lu reqId=%lu\n",
            _nodeId, reqEnv.linePa, static_cast<int>(reqEnv.reqType),
            reqEnv.writeIntent, reqEnv.srcNode, reqEnv.epoch, reqEnv.reqId);

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
    GrantDataSource dataSource = GrantDataSource::HomeMemory;
    uint64_t authEpoch = 0;

    UBCC_OuterGrantType ubccGrant =
        homeUbcc->processOuterRequest(homePa, ubccReq, writeIntent, _nodeId,
                                      entry.epoch, reqIdVal,
                                      &grantVisibleTick, &sentinelVisibleTick,
                                      &recallNeeded, &recallOwnerNode,
                                      &dataSource, &authEpoch);

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
        recallMsg.ownerLocalPa = _addrMap.buildDsmPA(
            recallOwnerNode, homeNode, offset);
        recallMsg.ownerNode = recallOwnerNode;
        recallMsg.homeNode = homeNode;
        recallMsg.epoch = homeUbcc->getEpochForLine(homePa);
        recallMsg.reqId = reqIdVal;  // v4: outer transaction reqId
        recallMsg.isReadRequest = (reqType == OuterReqType::GlobalReadShared);
        recallMsg.dataNeeded = true;

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
                    invMsg.sharerLocalPa = _addrMap.buildDsmPA(
                        s, homeNode, offset);
                    invMsg.sharerNode = s;
                    invMsg.homeNode = homeNode;
                    invMsg.epoch = homeEpoch;
                    invMsg.reqId = reqIdVal;  // v4: outer transaction reqId

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
                        homeUbcc->processInvalidationAck(homePa, s, homeEpoch, reqIdVal);
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

    // upgrade_invalidate_fix D5: use home's GRANT_HANDSHAKE baseEpoch
    // for the Clear tuple, NOT the requester's local entry.epoch.
    // The home may have rebased the epoch for queued/replayed requests.
    uint64_t grantBaseEpoch = authEpoch;
    if (grantBaseEpoch == 0) {
        grantBaseEpoch = entry.epoch;
    }
    grantEnv.epoch = grantBaseEpoch;
    grantEnv.reqId = reqIdVal;  // v4: outer transaction reqId
    grantEnv.grantVisibleTick = grantVisibleTick;
    grantEnv.sentinelVisibleTick = sentinelVisibleTick;

    // Also update local entry.epoch to match for future retries
    entry.epoch = grantBaseEpoch;
    _requesterLines[line_pa] = entry;

    // Self-test assertion: sentinelVisibleTick <= grantVisibleTick
    if (sentinelVisibleTick > grantVisibleTick) {
        fatal("EPBackend node_id=%d: tick ordering violation "
              "PA=0x%lx sentinelVisibleTick=%lu > grantVisibleTick=%lu\n",
              _nodeId, line_pa, sentinelVisibleTick, grantVisibleTick);
    }

    // Q3: Busy — caller should retry later.
    // This request never obtained a grant, so the temporary M6 busy window
    // must be torn down here; otherwise EP-RNF keeps the line marked as
    // outerTxnPending forever and later retries / delayed snoop responses
    // can self-deadlock.
    if (static_cast<int>(ubccGrant) < 0) {
        if (_epRnfCtrl) {
            _epRnfCtrl->setOuterTxnPending(line_pa, false);
            _epRnfCtrl->signalOuterTxnComplete(line_pa);
        }
        return -1;
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

    // upgrade_invalidate_fix D5: save PendingGrantTxn for Clear tuple correctness
    {
        PendingGrantTxn txn;
        txn.valid = true;
        txn.linePa = homePa;
        txn.homeNode = homeNode;
        txn.baseEpoch = grantBaseEpoch;
        txn.reqId = reqIdVal;
        txn.grantType = grantEnv.grantType;
        _pendingGrantTxns[line_pa] = txn;
    }

    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: outer grant envelope "
            "linePa=0x%lx grantType=%d homeNode=%d epoch=%lu reqId=%lu\n",
            _nodeId, grantEnv.linePa, static_cast<int>(grantEnv.grantType),
            grantEnv.homeNode, grantEnv.epoch, grantEnv.reqId);

    // Handle grant result and update bookkeeping
    OuterGrantType result = handleGrant(line_pa, grant, homeNode);

    if (dataSource == GrantDataSource::RecallBuffer) {
        DataBlock recallBlk(64);
        bool recallDataOk = homeUbcc->copyOutstandingGrantData(homePa, recallBlk);
        setRecallCaptureData(recallBlk, recallDataOk);
    }

    // v4: Populate grant data using formal F3 data source
    populateGrantData(homePa, dataSource);

    // ---- M6: Clear outer txn pending and signal completion ----
    if (_epRnfCtrl) {
        _epRnfCtrl->setOuterTxnPending(line_pa, false);
        _epRnfCtrl->signalOuterTxnComplete(line_pa);
    }

    // v4: Send Clear to home UBCC to commit the GRANT_HANDSHAKE intended result.
    // Per §3.3, §3.5, §5.1-5.4: the commit point for normal misses is when
    // home accepts the matching Clear, not when the grant was first emitted.
    // F2: Use grant envelope tuple (epoch, reqId), not entry.epoch which may
    // have been overwritten by a subsequent retry.
    sendClear(homePa, homeNode, grantEnv.epoch, grantEnv.reqId);

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

// ---- F3: Grant Data Populator (HomeMemory / RecallBuffer / NoData) ----

void
EPBackend::populateGrantData(uint64_t homePa, GrantDataSource dataSource)
{
    static const int lineSize = 64; // cache line size
    printf("[F3-DEBUG] populateGrantData node=%d homePA=0x%lx dataSource=%d\n",
           _nodeId, homePa, static_cast<int>(dataSource));

    uint8_t buf[64] = {};
    bool dataPopulated = false;

    switch (dataSource) {
        case GrantDataSource::HomeMemory: {
            // F3: Read clean/shared data from DDR4 via HomeMemoryService.
            // This is the single authoritative entry point for HomeMemory data.
            auto *physMem = _ruby_system ? _ruby_system->getPhysMem() : nullptr;
            HomeMemoryService hms(physMem);
            if (hms.read(homePa, buf, lineSize)) {
                _lastGrantDataBlock.setData(buf, 0, lineSize);
                _lastGrantDataValid = true;
                _lastGrantDataSource = GrantDataSource::HomeMemory;
                dataPopulated = true;
                DPRINTF(RubyCHIGeneric,
                        "EPBackend node_id=%d: HomeMemory read PA=0x%lx OK\n",
                        _nodeId, homePa);
            } else {
                // No physMem — zero-fill (valid for uninitialized DSM memory)
                _lastGrantDataBlock.setData(buf, 0, lineSize);
                _lastGrantDataValid = true;
                _lastGrantDataSource = GrantDataSource::NoData;
                dataPopulated = true;
                DPRINTF(RubyCHIGeneric,
                        "EPBackend node_id=%d: no physMem for HomeMemory "
                        "PA=0x%lx, zero-filled\n", _nodeId, homePa);
            }
            break;
        }

        case GrantDataSource::RecallBuffer: {
            // F3: Copy from recall capture buffer (dirty data from owner eviction).
            // This is consumed exactly once per recall cycle.
            if (_recallCaptureDataValid) {
                _lastGrantDataBlock = _recallCaptureDataBlock;
                _recallCaptureDataValid = false; // consume once
                _lastGrantDataValid = true;
                _lastGrantDataSource = GrantDataSource::RecallBuffer;
                dataPopulated = true;
                DPRINTF(RubyCHIGeneric,
                        "EPBackend node_id=%d: RecallBuffer data used "
                        "for PA=0x%lx\n", _nodeId, homePa);
            } else {
                // F3: Recall buffer data not ready — leave _lastGrantDataValid=false
                // so EPSNFController can detect and defer/retry instead of
                // silently sending zero-filled CompData.
                warn("EPBackend node_id=%d: RecallBuffer data NOT READY "
                     "for PA=0x%lx — grant data deferred\n",
                     _nodeId, homePa);
            }
            break;
        }

        case GrantDataSource::NoData: {
            // F3: Explicit NoData — zero-fill is the correct behavior.
            memset(buf, 0, lineSize);
            _lastGrantDataBlock.setData(buf, 0, lineSize);
            _lastGrantDataValid = true;
            _lastGrantDataSource = GrantDataSource::NoData;
            dataPopulated = true;
            DPRINTF(RubyCHIGeneric,
                    "EPBackend node_id=%d: NoData, zero-filled PA=0x%lx\n",
                    _nodeId, homePa);
            break;
        }
    }

    if (!dataPopulated) {
        _lastGrantDataValid = false;
        _lastGrantDataSource = dataSource;  // preserve intent for caller inspection
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

    // ---- F2: Real CHI recall via EP-RNF → HN-F → L2 ----
    // Replaces the functionalRead + phys_mem broadcast + fake sendRecallResponse
    // path with a proper async CHI request.  The callback sends the recall
    // response carrying actual data captured from the cache hierarchy.
    uint64_t ownerLocalPa = (recallMsg.ownerLocalPa != 0)
                               ? recallMsg.ownerLocalPa
                               : recallMsg.linePa;

    if (!_epRnfCtrl) {
        fatal("EPBackend node_id=%d: no EP_RNF controller for recall "
              "PA=0x%lx\n", _nodeId, recallMsg.linePa);
    }

    // Capture recallMsg fields for the async callback
    OuterRecallMsg capturedMsg = recallMsg;

    if (recallMsg.isReadRequest) {
        // Read recall: ReadShared to downgrade owner to R_S
        printf("[RECALL-DIAG] node=%d initiating ReadShared recall PA=0x%lx\n",
               _nodeId, recallMsg.linePa);
        _epRnfCtrl->startReadShared(ownerLocalPa,
            [this, capturedMsg](bool success) {
                printf("[RECALL-DIAG] node=%d ReadShared callback success=%d valid=%d\n",
                       _nodeId, success, _recallCaptureDataValid);
                OuterRecallResponse resp;
                resp.linePa = capturedMsg.linePa;
                resp.ownerNode = capturedMsg.ownerNode;
                resp.homeNode = capturedMsg.homeNode;
                resp.epoch = capturedMsg.epoch;
                resp.reqId = capturedMsg.reqId;
                resp.ackReceived = success;
                resp.dataReturned = capturedMsg.dataNeeded && success &&
                                    _recallCaptureDataValid;
                if (_recallCaptureDataValid) {
                    resp.dataPayload = _recallCaptureDataBlock;
                    resp.hasDataPayload = true;
                }
                sendRecallResponse(resp);
            });
    } else {
        // Write recall: ReadUnique with RecallUnique proxy op
        _epRnfCtrl->startReadUnique(ownerLocalPa,
            [this, capturedMsg](bool success) {
                OuterRecallResponse resp;
                resp.linePa = capturedMsg.linePa;
                resp.ownerNode = capturedMsg.ownerNode;
                resp.homeNode = capturedMsg.homeNode;
                resp.epoch = capturedMsg.epoch;
                resp.reqId = capturedMsg.reqId;
                resp.ackReceived = success;
                resp.dataReturned = capturedMsg.dataNeeded && success &&
                                    _recallCaptureDataValid;
                if (_recallCaptureDataValid) {
                    resp.dataPayload = _recallCaptureDataBlock;
                    resp.hasDataPayload = true;
                }
                sendRecallResponse(resp);
            });
    }

    // Return true: recall initiated asynchronously.
    // The callback will send the response to the home UBCC.
    return true;
}

bool
EPBackend::sendRecallResponse(const OuterRecallResponse &response)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: sendRecallResponse "
            "PA=0x%lx homeNode=%d dataReturned=%d hasData=%d\n",
            _nodeId, response.linePa, response.homeNode,
            response.dataReturned, response.hasDataPayload);

    // Store for inspection
    _lastRecallResponse = response;
    _recallResponseSentCount++;

    if (response.hasDataPayload) {
        EPBackend *homeBackend = EPBackend::getBackendInstance(response.homeNode);
        RubySystem *homeRuby = homeBackend ? homeBackend->getRubySystem() : nullptr;
        auto *physMem = homeRuby ? homeRuby->getPhysMem() : nullptr;
        HomeMemoryService hms(physMem);
        uint8_t buf[64] = {};
        memcpy(buf, response.dataPayload.getData(0, 64), 64);
        bool installed = hms.write(response.linePa, buf, 64);
        printf("[RECALL-DIAG] home-install node=%d home=%d PA=0x%lx installed=%d hasData=%d\n",
               _nodeId, response.homeNode, response.linePa,
               installed, response.hasDataPayload);
    }

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

    // F2: Pass recall data payload to home UBCC for storage in RECALL ost
    bool ok = homeUbcc->processRecallResponse(
        response.linePa, response.ownerNode, response.dataReturned,
        response.epoch, response.reqId,
        response.hasDataPayload ? &response.dataPayload : nullptr);

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
    printf("[INVAL-DIAG] node=%d handleInvalidationRequest PA=0x%lx home=%d sharerLocalPA=0x%lx\n",
           _nodeId, invMsg.linePa, invMsg.homeNode, invMsg.sharerLocalPa);
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: handleInvalidationRequest "
            "PA=0x%lx sharerNode=%d homeNode=%d epoch=%lu reqId=%lu\n",
            _nodeId, invMsg.linePa, invMsg.sharerNode,
            invMsg.homeNode, invMsg.epoch, invMsg.reqId);

    // M8 P0-1: Validate — this node must be the invalidation target.
    if (invMsg.sharerNode != _nodeId) {
        fatal("EPBackend node_id=%d: invalidation target mismatch "
              "expected=%d got=%d\n",
              _nodeId, invMsg.sharerNode, _nodeId);
    }

    // Store invalidation message for inspection
    _lastInvalidateMsg = invMsg;
    _invalidationReceivedCount++;

    // Update requester-side bookkeeping
    uint64_t lookupPa = (invMsg.sharerLocalPa != 0)
                           ? invMsg.sharerLocalPa
                           : invMsg.linePa;
    {
        auto it = _requesterLines.find(lookupPa);
        if (it != _requesterLines.end()) {
            it->second.state = RequesterLineState::R_I;
        }
    }

    // ---- v4 (§4.2.4): FIXED — use EP-RNF.startCleanUnique, wait for
    // callback before sending invalidation ack.  Previous code directly
    // ack'd, bypassing HN-F and losing grant/invalidation serialization.
    if (_epRnfCtrl) {
        // Capture invMsg by value for the callback
        OuterInvalidateMsg capturedMsg = invMsg;
        printf("[INVAL-DIAG] node=%d calling startCleanUnique PA=0x%lx\n",
               _nodeId, capturedMsg.sharerLocalPa);
        _epRnfCtrl->startCleanUnique(
            capturedMsg.sharerLocalPa,
            [this, capturedMsg](bool ok) {
                printf("[INVAL-DIAG] node=%d startCleanUnique callback PA=0x%lx ok=%d\n",
                       _nodeId, capturedMsg.linePa, ok);
                OuterInvalidationAck ack;
                ack.linePa = capturedMsg.linePa;
                ack.ackNode = _nodeId;
                ack.homeNode = capturedMsg.homeNode;
                ack.epoch = capturedMsg.epoch;
                ack.reqId = capturedMsg.reqId;
                ack.success = ok;
                sendInvalidationAck(ack);
            });
        return true;
    } else {
        // Fallback: if no EP-RNF controller, ack directly (prototype mode)
        warn("EPBackend node_id=%d: no EP-RNF controller, "
             "sending invalidation ack directly (bypasses HN-F)\n",
             _nodeId);
        OuterInvalidationAck ack;
        ack.linePa = invMsg.linePa;
        ack.ackNode = _nodeId;
        ack.homeNode = invMsg.homeNode;
        ack.epoch = invMsg.epoch;
        ack.reqId = invMsg.reqId;
        ack.success = true;
        sendInvalidationAck(ack);
        return true;
    }
}

bool
EPBackend::sendInvalidationAck(const OuterInvalidationAck &ack)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: sendInvalidationAck "
            "PA=0x%lx ackNode=%d homeNode=%d epoch=%lu reqId=%lu\n",
            _nodeId, ack.linePa, ack.ackNode,
            ack.homeNode, ack.epoch, ack.reqId);

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
        ack.linePa, ack.ackNode, ack.epoch, ack.reqId);

    if (!ok) {
        warn("EPBackend node_id=%d: home UBCC rejected invalidation ack "
             "PA=0x%lx\n", _nodeId, ack.linePa);
    }

    return ok;
}

// ---- v4: Local Upgrade Management (§4.1.4, §4.2.3) ----

bool
EPBackend::notifyLocalWriteUpgrade(uint64_t line_pa, int homeNode,
                                    int desiredPerm, UpgradeCause cause,
                                    uint64_t &outEpoch, uint64_t &outReqId)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: notifyLocalWriteUpgrade "
            "PA=0x%lx homeNode=%d desiredPerm=%d\n",
            _nodeId, line_pa, homeNode, desiredPerm);

    // Translate local PA to home PA
    uint64_t offset = _addrMap.dsmOffset(line_pa);
    uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode, offset);

    // Allocate new epoch and reqId
    _epochCounter++;
    uint64_t epochVal = _epochCounter;
    uint64_t reqIdVal = _epochCounter;

    // Get home UBCC
    UBCCController *homeUbcc = UBCCController::getInstance(homeNode);
    if (!homeUbcc) {
        if (homeNode != _nodeId) {
            fatal("EPBackend node_id=%d: remote UBCC for homeNode=%d "
                  "not registered for upgrade\n", _nodeId, homeNode);
        }
        homeUbcc = _ubcc;
    }
    if (!homeUbcc) {
        fatal("EPBackend node_id=%d: no UBCC for upgrade PA=0x%lx\n",
              _nodeId, line_pa);
    }

    // Convert EPBackend UpgradeCause to UBCC UpgradeCause
    UBCC_UpgradeCause ubccCause =
        (cause == UpgradeCause::LocalCleanUnique)
            ? UBCC_UpgradeCause::LocalCleanUnique
            : UBCC_UpgradeCause::LocalStoreUpgrade;

    // Send OuterUpgradeReq to home UBCC
    OuterUpgradeReq upgradeReq;
    upgradeReq.linePa = homePa;
    upgradeReq.srcNode = _nodeId;
    upgradeReq.epoch = epochVal;
    upgradeReq.reqId = reqIdVal;
    upgradeReq.desiredPerm = desiredPerm;
    upgradeReq.cause = cause;
    _lastUpgradeReq = upgradeReq;

    bool accepted = homeUbcc->processOuterUpgradeReq(
        homePa, _nodeId, epochVal, reqIdVal,
        desiredPerm, ubccCause);

    if (accepted) {
        // Store returned values (reservedEpoch, echoed reqId)
        outEpoch = epochVal;
        outReqId = reqIdVal;

        // upgrade_invalidate_fix: determine if invalidation fanout is needed
        uint64_t upgradeTargetMask = homeUbcc->getUpgradePendingTargetMask(homePa);

        if (upgradeTargetMask != 0) {
            // Other sharers exist — must invalidate them before Ack(true)
            printf("[UPGRADE-DIAG] node=%d upgrade accepted PENDING PA=0x%lx "
                   "targetMask=0x%lx — fanning out invalidations\n",
                   _nodeId, line_pa, upgradeTargetMask);

            // Fanout invalidations to each target sharer
            // Reuse existing EPBackend invalidation routing path
            uint64_t homeEpoch = homeUbcc->getEpochForLine(homePa);
            uint64_t offset = _addrMap.dsmOffset(line_pa);

            uint64_t remainingMask = upgradeTargetMask;
            for (int s = 0; s < 64 && remainingMask != 0; s++) {
                uint64_t sBit = (1ULL << s);
                if (remainingMask & sBit) {
                    remainingMask &= ~sBit;

                    OuterInvalidateMsg invMsg;
                    invMsg.linePa = homePa;
                    invMsg.sharerLocalPa = _addrMap.buildDsmPA(
                        s, homeNode, offset);
                    invMsg.sharerNode = s;
                    invMsg.homeNode = homeNode;
                    invMsg.epoch = homeEpoch;
                    invMsg.reqId = reqIdVal;

                    _lastInvalidateMsg = invMsg;

                    // Route invalidation to the sharer node's EPBackend
                    EPBackend *sharerBackend = EPBackend::getBackendInstance(s);
                    if (sharerBackend) {
                        DPRINTF(RubyEP,
                                "EPBackend node_id=%d: upgrade fanout "
                                "invalidation to node %d\n", _nodeId, s);
                        sharerBackend->handleInvalidationRequest(invMsg);
                    } else {
                        // Direct ack through home UBCC (fallback)
                        DPRINTF(RubyEP,
                                "EPBackend node_id=%d: sharer EPBackend for "
                                "node %d not found — issuing direct ack\n",
                                _nodeId, s);
                        homeUbcc->processInvalidationAck(homePa, s, homeEpoch, reqIdVal);
                    }
                }
            }

            // Ack is NOT ready yet — will be sent when all acks arrive
            OuterUpgradeAck ack;
            ack.linePa = homePa;
            ack.homeNode = homeNode;
            ack.dstNode = _nodeId;
            ack.epoch = epochVal;
            ack.reqId = reqIdVal;
            ack.accepted = false;  // deferred: not yet ready
            _lastUpgradeAck = ack;
        } else {
            // No other sharers — immediate Ack(true)
            OuterUpgradeAck ack;
            ack.linePa = homePa;
            ack.homeNode = homeNode;
            ack.dstNode = _nodeId;
            ack.epoch = epochVal;
            ack.reqId = reqIdVal;
            ack.accepted = true;  // immediate: Ack(true) ready now
            _lastUpgradeAck = ack;

            DPRINTF(RubyEP,
                    "EPBackend node_id=%d: upgrade accepted immediate "
                    "PA=0x%lx epoch=%lu reqId=%lu (no other sharers)\n",
                    _nodeId, line_pa, epochVal, reqIdVal);
        }
    } else {
        OuterUpgradeAck ack;
        ack.linePa = homePa;
        ack.homeNode = homeNode;
        ack.dstNode = _nodeId;
        ack.epoch = epochVal;
        ack.reqId = reqIdVal;
        ack.accepted = false;
        _lastUpgradeAck = ack;
    }

    return accepted;
}

bool
EPBackend::sendUpgradeDone(uint64_t line_pa, int homeNode,
                            uint64_t epoch, uint64_t reqId)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: sendUpgradeDone "
            "PA=0x%lx homeNode=%d epoch=%lu reqId=%lu\n",
            _nodeId, line_pa, homeNode, epoch, reqId);

    uint64_t offset = _addrMap.dsmOffset(line_pa);
    uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode, offset);

    UBCCController *homeUbcc = UBCCController::getInstance(homeNode);
    if (!homeUbcc) {
        if (homeNode != _nodeId) {
            fatal("EPBackend node_id=%d: remote UBCC for upgrade done\n",
                  _nodeId);
        }
        homeUbcc = _ubcc;
    }
    if (!homeUbcc) {
        return false;
    }

    OuterUpgradeDone doneMsg;
    doneMsg.linePa = homePa;
    doneMsg.srcNode = _nodeId;
    doneMsg.homeNode = homeNode;
    doneMsg.epoch = epoch;
    doneMsg.reqId = reqId;
    _lastUpgradeDone = doneMsg;

    bool accepted = homeUbcc->processOuterUpgradeDone(
        homePa, _nodeId, epoch, reqId);

    OuterUpgradeDoneAck doneAck;
    doneAck.linePa = homePa;
    doneAck.homeNode = homeNode;
    doneAck.dstNode = _nodeId;
    doneAck.epoch = epoch;
    doneAck.reqId = reqId;
    doneAck.accepted = accepted;
    _lastUpgradeDoneAck = doneAck;

    return accepted;
}

// ---- v4: Clear / ClearAck (§3.5) ----

bool
EPBackend::sendClear(uint64_t line_pa, int homeNode,
                      uint64_t epoch, uint64_t reqId)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: sendClear "
            "PA=0x%lx homeNode=%d epoch=%lu reqId=%lu\n",
            _nodeId, line_pa, homeNode, epoch, reqId);

    UBCCController *homeUbcc = UBCCController::getInstance(homeNode);
    if (!homeUbcc) {
        if (homeNode != _nodeId) {
            fatal("EPBackend node_id=%d: remote UBCC for Clear\n", _nodeId);
        }
        homeUbcc = _ubcc;
    }
    if (!homeUbcc) {
        return false;
    }

    // upgrade_invalidate_fix D5: prefer PendingGrantTxn.baseEpoch
    // over caller-supplied epoch for replay/retry correctness
    uint64_t clearEpoch = epoch;
    auto txnIt = _pendingGrantTxns.find(line_pa);
    if (txnIt != _pendingGrantTxns.end() && txnIt->second.valid) {
        clearEpoch = txnIt->second.baseEpoch;
        // Invalidate after use (single-consumer)
        txnIt->second.valid = false;
    }

    // Q1: If outstanding already consumed by another node's Clear, soft-skip.
    if (homeUbcc && homeUbcc->getOutstandingBaseEpoch(line_pa) == 0) {
        printf("[SENDCLEAR-SKIP] node=%d PA=0x%lx epoch=%lu reqId=%lu\n",
               _nodeId, line_pa, clearEpoch, reqId);
        return true;
    }

    OuterClearMsg clearMsg;
    clearMsg.linePa = line_pa;
    clearMsg.srcNode = _nodeId;
    clearMsg.homeNode = homeNode;
    clearMsg.epoch = clearEpoch;
    clearMsg.reqId = reqId;
    clearMsg.reason = ClearReason::GrantHandshake;
    _lastClearMsg = clearMsg;

    bool accepted = homeUbcc->processClear(line_pa, _nodeId, clearEpoch, reqId);

    OuterClearAckMsg ack;
    ack.linePa = line_pa;
    ack.homeNode = homeNode;
    ack.dstNode = _nodeId;
    ack.epoch = epoch;
    ack.reqId = reqId;
    ack.accepted = accepted;
    _lastClearAckMsg = ack;

    return accepted;
}

// ---- upgrade_invalidate_fix: upgrade ack callback ----

void
EPBackend::notifyUpgradeAckReady(uint64_t linePa)
{
    // Called by home UBCC when all invalidation acks for an upgrade
    // have been received. Triggers the deferred receiveUpgradeAck()
    // on the local EPRNFController so that SnpResp_I can be sent to HN-F.
    if (_epRnfCtrl) {
        uint64_t callbackPa = linePa;
        // receiveUpgradeAck() tracks UpgradePending by requester-local PA,
        // while home UBCC notifies us with the home-view PA. Translate back
        // to the requester's local PA so deferred SnpResp_I / UpgradeDone can
        // find the pending context for cross-node upgrades.
        int homeNode = _lastUpgradeAck.homeNode;
        if (homeNode >= 0) {
            uint64_t offset = _addrMap.dsmOffset(linePa);
            callbackPa = _addrMap.buildDsmPA(_nodeId, homeNode, offset);
            _lastUpgradeAck.accepted = true;
        }
        DPRINTF(RubyEP,
                "EPBackend node_id=%d: notifyUpgradeAckReady PA=0x%lx "
                "localPA=0x%lx — triggering deferred receiveUpgradeAck\n",
                _nodeId, linePa, callbackPa);
        _epRnfCtrl->receiveUpgradeAck(callbackPa);
    } else {
        warn("EPBackend node_id=%d: notifyUpgradeAckReady PA=0x%lx "
             "but no EPRNFController registered\n",
             _nodeId, linePa);
    }
}

} // namespace ruby
} // namespace gem5
