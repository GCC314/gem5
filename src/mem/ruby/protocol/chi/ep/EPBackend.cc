#include "mem/ruby/protocol/chi/ep/EPBackend.hh"

#include <cstring>
#include <array>
#include <sstream>

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "debug/RubyEP.hh"
#include "mem/packet.hh"
#include "mem/request.hh"
#include "mem/simple_mem.hh"
#include "mem/ruby/protocol/chi/ep/EPRNFController.hh"
#include "mem/ruby/protocol/chi/ep/MetaRNFController.hh"
#include "mem/ruby/protocol/chi/ep/UBAdapter.hh"
#include "mem/ruby/protocol/chi/ep/EPSNFController.hh"
#include "mem/ruby/protocol/CHI/CHIRequestType.hh"
#include "mem/ruby/system/RubySystem.hh"
#include "params/EPBackend.hh"
#include "sim/cur_tick.hh"

namespace gem5
{

namespace ruby
{

namespace
{

uint64_t
makeRequesterReqId(int nodeId, uint64_t seq)
{
    return (static_cast<uint64_t>(nodeId & 0xff) << 56) |
           (seq & 0x00ffffffffffffffULL);
}

} // anonymous namespace

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
    _addrMap(p.num_nodes, p.num_sockets, 128ULL * 1024 * 1024),
    _metaRnf(p.meta_rnf),
    _numSockets(p.num_sockets),
    _ruby_system(p.ruby_system),
    _metadataPrivateBase(p.metadata_private_base),
    _metadataPrivateSize(p.metadata_private_size),
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
    if (_numSockets < 1) {
        fatal("EPBackend node_id=%d: num_sockets=%d must be >= 1\n",
              _nodeId, _numSockets);
    }
    _ubAdapters.resize(_numSockets, nullptr);
    _epSnfs.resize(_numSockets, nullptr);  // v4-dual-socket: per-socket EP-SNF slots

    auto *ruby_system = p.ruby_system;

    // v4-dual-socket: Register legacy single adapter into slot 0 if provided.
    UBAdapter *legacyAdapter = p.ub_adapter;
    if (legacyAdapter) {
        registerAdapter(0, legacyAdapter);
    }
    // Socket-plane: register per-socket UBAdapters (index == socket_id). Passing
    // them as a VectorParam also makes each a child SimObject, so its init()
    // runs and binds its own ubio Port (ubio(node, socket)). Without this, only
    // socket 0's adapter was tree-attached and socket-1 traffic had no transport.
    for (int s = 0; s < (int)p.ub_adapters.size(); ++s) {
        if (p.ub_adapters[s])
            registerAdapter(s, p.ub_adapters[s]);
    }

    // M6: Register this EPBackend in the static cross-node routing registry
    _backendInstances[_nodeId] = this;

    // Phase 0: startup manifest — metadata DRAM range reporting
    inform(
        "[EPBACKEND-MANIFEST] node=%d num_sockets=%d "
        "metadata_dram_base=0x%lx metadata_dram_total=%lu MiB "
        "per_socket=%lu MiB\n",
        _nodeId, _numSockets,
        _metadataPrivateBase,
        _metadataPrivateSize / (1024 * 1024),
        (_metadataPrivateSize / _numSockets) / (1024 * 1024));
}

// v4-dual-socket: per-socket EP-SNF registration (§3.6)
void
EPBackend::registerEpSnf(int socketId, EPSNFController *ctrl)
{
    if (socketId < 0) {
        fatal("EPBackend node_id=%d: registerEpSnf socketId=%d < 0\n",
              _nodeId, socketId);
    }
    if (socketId >= (int)_epSnfs.size()) {
        _epSnfs.resize(socketId + 1, nullptr);
    }
    if (_epSnfs[socketId] && _epSnfs[socketId] != ctrl) {
        fatal("EPBackend node_id=%d: registerEpSnf socket=%d already has "
              "different controller (old=%p new=%p)\n",
              _nodeId, socketId, (void*)_epSnfs[socketId], (void*)ctrl);
    }
    _epSnfs[socketId] = ctrl;
    if (_numSockets < (int)_epSnfs.size())
        _numSockets = _epSnfs.size();

    if (_ubAdapters.size() > (size_t)socketId && _ubAdapters[socketId]) {
        if (_verboseLog) {
        DPRINTF(RubyEP, "[WIRE] node=%d wiring adapter[%d]->snf callback\n",
                _nodeId, socketId);
        }
        _ubAdapters[socketId]->setOnResponseWired([this, socketId]{
            if (_verboseLog) {
            DPRINTF(RubyEP, "[RSP-FIRE] node=%d socket=%d scheduling EPSNF wakeup\n",
                    _nodeId, socketId);
            }
            if (socketId < (int)_epSnfs.size() && _epSnfs[socketId])
                _epSnfs[socketId]->scheduleEvent(Cycles(1));
        });
    }
}

EPSNFController*
EPBackend::getEpSnf(int socketId) const
{
    if (socketId >= 0 && socketId < (int)_epSnfs.size())
        return _epSnfs[socketId];
    return nullptr;
}

EPBackend::~EPBackend()
{
    // M6: Deregister from cross-node routing registry
    _backendInstances.erase(_nodeId);
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

void m9SelfTest_run(EPBackend*);


void
EPBackend::init()
{
    SimObject::init();

    if (!_metaRnf) {
        _metaRnf = MetaRNFController::getInstance(_nodeId, 0);
    }

    // v4-dual-socket: Wire UBCC to UBIOModule *via each socket's adapter.
    // For single-socket (legacy), only index 0 is used.
    for (int s = 0; s < _numSockets; s++) {
        UBAdapter *adapter = getUBAdapter(s);
        if (adapter) {
            adapter->bindBackend(this);
        }
    }

    // Wire UBAdapter -> EPSNF response wakeup
    for (int s = 0; s < _numSockets; s++) {
        UBAdapter *adapter = getUBAdapter(s);
        EPSNFController *snf = getEpSnf(s);
        if (adapter && snf) {
            adapter->setOnResponseWired([this, snf]{
                if (_verboseLog)
                    DPRINTF(RubyEP, "[RSP-FIRE] scheduling EPSNF wakeup\n");
                snf->scheduleEvent(Cycles(1));
            });
        }
    }

    // v4-dual-socket: EP-SNF completeness check (§3.6 change 3)
    for (int s = 0; s < _numSockets; ++s) {
        fatal_if(_epSnfs[s] == nullptr,
                 "EPBackend node_id=%d: missing EP-SNF for socket %d", _nodeId, s);
    }

}

void
EPBackend::wakeup()
{
    for (int s = 0; s < _numSockets; ++s) {
        UBAdapter *adapter = getUBAdapter(s);
        if (adapter && adapter->port())
            adapter->wakeup();
    }
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
    return _addrMap.isDsm(_nodeId, pa);
}

uint64_t
EPBackend::getEpRnfSnoopCount() const
{
    return _epRnfSnoopCount;
}

void
EPBackend::resetEpRnfSnoopCount()
{
    _epRnfSnoopCount = 0;
}

void
EPBackend::incrementEpRnfSnoopCount()
{
    _epRnfSnoopCount++;
}

// ---- M5: Remote Miss Request Dispatch ----

int
EPBackend::handleRemoteMiss(uint64_t line_pa, int neededPerm, bool writeIntent,
                             int& outHomeNode)
{
    // v4-dual-socket: default ingressSocket=0 for backward compat.
    // Callers should use the overload with ingressSocket parameter.
    return handleRemoteMiss(line_pa, neededPerm, writeIntent,
                             0, outHomeNode);
}

int
EPBackend::handleRemoteMiss(uint64_t line_pa, int neededPerm, bool writeIntent,
                             int ingressSocket,
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

    // v4-dual-socket: derive homeSocket from PA encoding.
    // With num_sockets=1, this always returns 0.
    int homeSocket = _addrMap.homeSocket(_nodeId, line_pa);
    if (homeSocket < 0) homeSocket = 0;

    // Socket-plane model: the request egresses via its ingress socket's own
    // UBAdapter, which binds to ubio(node, ingressSocket). Each socket has its
    // own Port, so cross-socket DSM reads reach the correct plane's ubio.
    int adapterIdx = (ingressSocket >= 0 && ingressSocket < _numSockets)
                         ? ingressSocket : 0;
    UBAdapter *adapter = getUBAdapter(adapterIdx);
    if (!adapter) {
        fatal("EPBackend node_id=%d: no UBAdapter for socket %d\n",
              _nodeId, adapterIdx);
    }

    // Translate PA from requester's view to the home (node, socket) view.
    // The homeSocket MUST be encoded into the home PA so it lands in the home
    // plane's DSM segment; otherwise the message's dstSocket (=homeSocket) and
    // its homeLinePa disagree and it is routed to the wrong plane's ubio.
    uint64_t offset = _addrMap.dsmOffset(line_pa);
    uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode, offset, homeSocket);

    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: translating PA 0x%lx -> home PA 0x%lx "
            "homeNode=%d homeSocket=%d ingressSocket=%d offset=0x%lx\n",
            _nodeId, line_pa, homePa, homeNode, homeSocket, ingressSocket, offset);

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

    // ---- Async grant/clear: pending-Clear fast path ----
    // If we already obtained the outer grant and only Clear remains, do NOT
    // retry the whole miss. Re-issuing a
    // fresh ReadReq here would (a) allocate a brand-new reqId (handleGrant has
    // already moved the line out of R_WAIT_GRANT, so isRetry would be false) and
    // (b) make the already-cached ClearResp{original reqId} unmatchable — an
    // infinite retry loop that ends in a Sequencer deadlock. Instead re-drive
    // sendClear() with the ORIGINAL reqId/epoch saved in the pending grant txn,
    // and complete the transaction once the ClearResp is accepted.
    {
        auto pgt = _pendingGrantTxns.find(homePa);
        if (pgt != _pendingGrantTxns.end() && pgt->second.valid) {
            int clearRet = sendClear(homePa, pgt->second.homeNode,
                                     pgt->second.baseEpoch, pgt->second.reqId,
                                     pgt->second.sourceAdapter);
            if (clearRet == -2)
                return -2;   // ClearResp not here yet; keep waiting (same reqId)
            if (clearRet <= 0)
                return -1;   // send/reject is retryable; retain txn and guard
            // Clear accepted: sendClear() has consumed the txn. Finish up.
            OuterGrantType g = pgt->second.grantType;
            const Tick start = pgt->second.outerStartTick;
            const uint64_t completedReqId = pgt->second.reqId;
            _pendingGrantTxns.erase(pgt);
            if (start) {
                inform(
                    "[EP-PERF] kind=outer node=%d pa=0x%lx reqId=%lu "
                    "start=%lu end=%lu latency_ps=%lu\n",
                    _nodeId, homePa, completedReqId, start, curTick(),
                    curTick() - start);
            }
            if (_epRnfCtrl) {
                _epRnfCtrl->setOuterTxnPending(line_pa, false);
                _epRnfCtrl->signalOuterTxnComplete(line_pa);
            }
            return static_cast<int>(g);
        }
    }

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

    // §5.2 Silent Upgrade (Write Hit): when the local requester already holds
    // R_E (clean exclusive) or R_M (dirty modified), guaranteed sole owner by
    // directory one-hot invariant, the write can complete locally with zero
    // cross-node messages — no OuterUpgradeReq, no epoch increment on the home.
    // This is the cross-node analogue of MESI's E→M (or M→M) silent upgrade.
    if (neededPerm == 1 && existing != _requesterLines.end() &&
        homeNode != _nodeId) {
        RequesterLineState st = existing->second.state;
        if (st == RequesterLineState::R_E ||
            st == RequesterLineState::R_M) {
            bool silent = params().silent_upgrade;
            if (silent) {
                // R_E → R_M (or R_M stays R_M): no outer request needed
                existing->second.state = RequesterLineState::R_M;
                if (_verboseLog) {
                DPRINTF(RubyEP, "[UPGRADE-DIAG] node=%d SILENT-WRITE-HIT PA=0x%lx "
                       "(state=%d→R_M, zero cross-node messages)\n",
                        _nodeId, line_pa, static_cast<int>(st));
                }
                inform(
                    "[EP-PERF] kind=upgrade_silent node=%d pa=0x%lx "
                    "start=%lu end=%lu latency_ps=0\n",
                    _nodeId, line_pa, curTick(), curTick());
                outHomeNode = homeNode;
                return static_cast<int>(OuterGrantType::GlobalGrantModified);
            }
        }
    }

    bool isRetry = (existing != _requesterLines.end() &&
                    existing->second.state == RequesterLineState::R_WAIT_GRANT);

    uint64_t reqIdVal;
    RequesterLineEntry entry;
    if (isRetry) {
        reqIdVal = existing->second.reqId;
        // A retry belongs to the already-issued outer transaction. Preserve its
        // first issue tick so capacity waits and transport retries remain part
        // of the reported end-to-end protocol latency.
        entry = existing->second;
        entry.pendingReq = reqType;
        entry.writeIntent = writeIntent;
        entry.homeNode = homeNode;
    } else {
        _epochCounter++;
        reqIdVal = makeRequesterReqId(_nodeId, _epochCounter);
        entry.lineAddr = line_pa;
        entry.state = RequesterLineState::R_WAIT_GRANT;
        entry.pendingReq = reqType;
        entry.epoch = _epochCounter;
        entry.reqId = reqIdVal;    // v4: store reqId
        entry.writeIntent = writeIntent;
        entry.homeNode = homeNode;
    }
    if (!isRetry)
        entry.outerStartTick = curTick();
    _requesterLines[line_pa] = entry;

    if (!adapter) {
        fatal("EPBackend node_id=%d: UBAdapter required for remote miss "
              "PA=0x%lx homeNode=%d\n",
              _nodeId, line_pa, homeNode);
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
    OuterReqType ubccReq =
        (reqType == OuterReqType::GlobalReadShared)
            ? OuterReqType::GlobalReadShared
            : OuterReqType::GlobalReadUnique;

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
    uint64_t grantEpoch = 0;

    OuterGrantType grantTypeVar;
    int pendingInvCount = -1;
    uint64_t pendingInvMask = 0;
    uint64_t committedEpoch = 0;
    DataBlock routedGrantData(64);
    bool routedGrantDataValid = false;
    int grantInt = adapter->sendReadReq(
        homePa, static_cast<int>(ubccReq), writeIntent, _nodeId,
        entry.epoch, reqIdVal, homeNode, ingressSocket, homeSocket,
        &grantVisibleTick, &sentinelVisibleTick,
        &recallNeeded, &recallOwnerNode,
        &dataSource, &authEpoch, &grantEpoch,
        &pendingInvCount, &pendingInvMask, &committedEpoch,
        &routedGrantData, &routedGrantDataValid);

    // Port async path: -2 means response pending, callers will retry
    if (grantInt == -2) return -2;

    grantTypeVar = static_cast<OuterGrantType>(grantInt);

    DPRINTF(RubyEP, "[DEBUG-TC5-CLEAR-TRACE] handleRemoteMiss node=%d localPA=0x%lx homePA=0x%lx "
            "grantTypeVar=%d reqId=%lu entryEpoch=%lu authEpoch=%lu recallNeeded=%d owner=%d\n",
            _nodeId, line_pa, homePa, static_cast<int>(grantTypeVar), reqIdVal,
            entry.epoch, authEpoch, recallNeeded, recallOwnerNode);
    DPRINTF(RubyEP, "[DEBUG-RECALL-OUTPUT] EPBackend n=%d PA=0x%lx recallNeeded=%d recallOwnerNode=%d\n",
            _nodeId, line_pa, recallNeeded, recallOwnerNode);

    // ---- M6: Handle recall path ----
    // If the home UBCC signals that a recall is needed, we must
    // route the recall through the owner node's EPBackend
    // (not bypass it with a direct processRecallResponse call).
    if (recallNeeded && recallOwnerNode >= 0) {
        DPRINTF(RubyEP, "[DEBUG-RECALL-ROUTE] EPBackend node=%d PA=0x%lx ownerNode=%d\n",
                _nodeId, line_pa, recallOwnerNode);
        DPRINTF(RubyEP,
                "EPBackend node_id=%d: M6 recall needed PA=0x%lx "
                "ownerNode=%d requesterNode=%d\n",
                _nodeId, line_pa, recallOwnerNode, _nodeId);

        // Build recall message
        OuterRecallMsg recallMsg;
        recallMsg.linePa = homePa;
        recallMsg.ownerLocalPa = _addrMap.buildDsmPA(
            recallOwnerNode, homeNode, offset, homeSocket);
        recallMsg.ownerNode = recallOwnerNode;
        recallMsg.homeNode = homeNode;
        recallMsg.epoch = committedEpoch;
        recallMsg.reqId = reqIdVal;  // v4: outer transaction reqId
        recallMsg.isReadRequest = (reqType == OuterReqType::GlobalReadShared);
        recallMsg.dataNeeded = true;

        _lastRecallMsg = recallMsg;

        DPRINTF(RubyEP,
                "EPBackend node_id=%d: routing recall to owner "
                "EPBackend node %d via UBAdapter\n",
                _nodeId, recallOwnerNode);
        getUBAdapter(0)->sendRecallReqToOwner(recallOwnerNode, recallMsg, homeSocket);
    }

    // ---- M8: Invalidation now owned by Home UBCC (direct fanout) ----
    // Requester no longer routes invalidations; home UBCC sends them
    // directly via its outbound sender interface (ubio_main injects).
    if (pendingInvCount > 0) {
        DPRINTF(RubyEP,
                "EPBackend node_id=%d: home UBCC owns invalidation fanout "
                "PA=0x%lx pendingInvCount=%d mask=0x%lx\n",
                _nodeId, line_pa, pendingInvCount, pendingInvMask);
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

    // Clear identifies the pending transaction with the base epoch, while the
    // granted cache line must carry the epoch Home commits on that Clear.
    uint64_t ownerEpoch = grantEpoch ? grantEpoch : grantBaseEpoch;
    entry.epoch = ownerEpoch;
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
    if (static_cast<int>(grantTypeVar) < 0) {
        if (_epRnfCtrl) {
            _epRnfCtrl->setOuterTxnPending(line_pa, false);
            _epRnfCtrl->signalOuterTxnComplete(line_pa);
        }
        return -1;
    }

    // Convert UBCC grant back to EPBackend's OuterGrantType
    OuterGrantType grant;
    switch (grantTypeVar) {
        case OuterGrantType::GlobalGrantShared:
            grant = OuterGrantType::GlobalGrantShared;
            grantEnv.grantType = OuterGrantType::GlobalGrantShared;
            break;
        case OuterGrantType::GlobalGrantExclusive:
            grant = OuterGrantType::GlobalGrantExclusive;
            grantEnv.grantType = OuterGrantType::GlobalGrantExclusive;
            break;
        case OuterGrantType::GlobalGrantModified:
            grant = OuterGrantType::GlobalGrantModified;
            grantEnv.grantType = OuterGrantType::GlobalGrantModified;
            break;
        default:
            fatal("EPBackend node_id=%d: unknown UBCC grant %d\n",
                  _nodeId, static_cast<int>(grantTypeVar));
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
        txn.sourceAdapter = adapterIdx;
        txn.grantType = grantEnv.grantType;
        txn.outerStartTick = entry.outerStartTick;
        _pendingGrantTxns[homePa] = txn;
        DPRINTF(RubyEP, "[DEBUG-TC5-CLEAR-TRACE] savePendingGrantTxn node=%d keyPA=0x%lx homePA=0x%lx "
                "baseEpoch=%lu reqId=%lu grantType=%d\n",
                _nodeId, homePa, homePa, txn.baseEpoch, txn.reqId,
                static_cast<int>(txn.grantType));
    }

    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: outer grant envelope "
            "linePa=0x%lx grantType=%d homeNode=%d epoch=%lu reqId=%lu\n",
            _nodeId, grantEnv.linePa, static_cast<int>(grantEnv.grantType),
            grantEnv.homeNode, grantEnv.epoch, grantEnv.reqId);

    // Handle grant result and update bookkeeping
    OuterGrantType result = handleGrant(line_pa, grant, homeNode);

    // In split-mode all grant data arrives via ReadResp payload from ubio.
    // Use payload whenever available; only fall back to zero-fill (NoData)
    // for truly uninitialised DSM lines (first access, no prior write).
    // Phase C1: route push/ReadResp grant data directly to _lastGrantDataBlock.
    // Do NOT use _recallCaptureData* — it is a controller-global slot shared
    // across all PAs and can be cleared by a concurrent unrelated Recall.
    if (routedGrantDataValid) {
        _lastGrantDataBlock = routedGrantData;
        _lastGrantDataValid = true;
        _lastGrantDataSource = GrantDataSource::RecallBuffer;
        PendingGrantData &pendingData = _pendingGrantData[line_pa];
        pendingData.data = routedGrantData;
        pendingData.source = GrantDataSource::RecallBuffer;
        pendingData.valid = true;
        DPRINTF(RubyEP,
                     "[C1-GRANT-DATA-DIRECT] node=%d pa=0x%lx "
                     "grant data routed directly (bypass recall capture)\n",
                     _nodeId, homePa);
        // ── Phase C4 trace point 7: EPBackend routed grant word ──
        {
            uint64_t off = homePa & 0x1FFFULL;
            uint64_t ckOff = homePa & 0xFFFFFULL;
            if (ckOff < 0x80000ULL && (off % 64 == 0)) {
                uint64_t w0 = 0;
                for (int i = 0; i < 8; i++)
                    ((uint8_t*)&w0)[i] = _lastGrantDataBlock.getByte(i);
                DPRINTF(RubyEP,
                    "[C4-EPB-DIRECT] node=%d pa=0x%lx off=0x%lx w0=0x%016lx\n",
                    _nodeId, homePa, off, w0);
            }
        }
    } else {
        // No payload — uninitialised line, zero-fill is correct
        populateGrantData(homePa, GrantDataSource::NoData);
        PendingGrantData &pendingData = _pendingGrantData[line_pa];
        pendingData.data = _lastGrantDataBlock;
        pendingData.source = _lastGrantDataSource;
        // Presence is separate from explicit NoData; only the latter may zero-fill.
        pendingData.valid = _lastGrantDataValid &&
            _lastGrantDataSource != GrantDataSource::NoData;
    }

    int clearRet = sendClear(homePa, homeNode, grantEnv.epoch, grantEnv.reqId,
                             adapterIdx);
    if (clearRet == -2) return -2;
    if (clearRet <= 0)
        return -1;

    // ---- M6: Clear outer txn pending and signal completion (after Clear) ----
    if (_epRnfCtrl) {
        _epRnfCtrl->setOuterTxnPending(line_pa, false);
        _epRnfCtrl->signalOuterTxnComplete(line_pa);
    }

    return static_cast<int>(result);

    fatal("EPBackend node_id=%d: no UBCC available for remote miss "
          "PA=0x%lx\n", _nodeId, line_pa);
    return -1;
}

int
EPBackend::handleRemoteDemandMiss(uint64_t line_pa, int neededPerm,
                                  bool writeIntent, int ingressSocket,
                                  int& outHomeNode)
{
    auto existing = _requesterLines.find(line_pa);
    if (existing != _requesterLines.end() &&
        existing->second.state != RequesterLineState::R_WAIT_GRANT) {
        DPRINTF(RubyEP,
                "EPBackend node_id=%d: HN-F confirmed local miss PA=0x%lx "
                "invalidating stale requester state=%d\n",
                _nodeId, line_pa, static_cast<int>(existing->second.state));
        existing->second.state = RequesterLineState::R_I;
    }
    return handleRemoteMiss(line_pa, neededPerm, writeIntent,
                            ingressSocket, outHomeNode);
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
            if (_verboseLog) {
            DPRINTF(RubyEP, "[RE-DIAG] node=%d line 0x%lx -> R_E (GrantExclusive)\n",
                    _nodeId, line_pa);
            }
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

    // X-fix: clear stale active-recall markers when this requester line is
    // re-acquired (R_S/R_E/R_M). This preserves RECALL-SNOOP protection for
    // the immediate recall-induced SnpCleanInvalid (which arrives before any
    // explicit re-fetch), while preventing old recall markers from poisoning a
    // later genuine local upgrade on the same line (TC42 pattern).
    clearActiveRecall(line_pa);
    if (homeNode >= 0) {
        int homeSocket = _addrMap.homeSocket(_nodeId, line_pa);
        if (homeSocket < 0) homeSocket = 0;
        uint64_t offset = _addrMap.dsmOffset(line_pa);
        uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode,
                                              offset, homeSocket);
        if (homePa != line_pa) {
            clearActiveRecall(homePa);
        }
    }

    return grant;
}

// ---- F3: Grant Data Populator (HomeMemory / RecallBuffer / NoData) ----

void
EPBackend::populateGrantData(uint64_t homePa, GrantDataSource dataSource)
{
    static const int lineSize = 64; // cache line size
    DPRINTF(RubyEP, "[DEBUG-F3-DEBUG] populateGrantData node=%d homePA=0x%lx dataSource=%d\n",
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

int
EPBackend::takeGrantData(uint64_t linePa, DataBlock &data,
                         GrantDataSource &source)
{
    auto it = _pendingGrantData.find(linePa);
    if (it == _pendingGrantData.end()) {
        return -1;
    }

    source = it->second.source;
    if (it->second.valid)
        data = it->second.data;
    const bool valid = it->second.valid;
    _pendingGrantData.erase(it);
    return valid ? 1 : 0;
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

void
EPBackend::setMetaRnfController(MetaRNFController *ctrl)
{
    _metaRnf = ctrl;
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

// Phase 0.4: C4 Direct-Forward gate — controlled by SimObject Param direct_fwd.
// Default True enables direct-forward (owner→requester bypass).

bool
EPBackend::handleRecallRequest(const OuterRecallMsg &recallMsg)
{
    if (_verboseLog) {
    DPRINTF(RubyEP, "[RECALL-ENTRY] EPBackend node=%d PA=0x%lx ownerNode=%d homeNode=%d\n",
            _nodeId, recallMsg.linePa, recallMsg.ownerNode, recallMsg.homeNode);
    }
    inform(
                 "[RECALL-ENTRY-ERR] node=%d PA=0x%lx ownerNode=%d homeNode=%d reqId=%lu isRead=%d dataNeeded=%d curT=%lu\n",
                 _nodeId, recallMsg.linePa, recallMsg.ownerNode,
                 recallMsg.homeNode, recallMsg.reqId,
                 recallMsg.isReadRequest ? 1 : 0,
                 recallMsg.dataNeeded ? 1 : 0, curTick());
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

    // Track active recall for self-snoop detection in EPRNFController.
    // Store both local PA (matches SnpCleanInvalid msg->m_addr) and home PA.
    {
        uint64_t localPA = (recallMsg.ownerLocalPa != 0)
                              ? recallMsg.ownerLocalPa
                              : recallMsg.linePa;
        _activeRecallPAs[localPA] = true;
        _activeRecallPAs[recallMsg.linePa] = true;
        if (_verboseLog) {
        DPRINTF(RubyEP, "[RECALL-DIAG] node=%d active-recall-set linePA=0x%lx localPA=0x%lx\n",
                _nodeId, recallMsg.linePa, localPA);
        }
    }

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
        if (_verboseLog) {
        DPRINTF(RubyEP, "[RECALL-DIAG] node=%d initiating ReadShared recall PA=0x%lx\n",
                _nodeId, recallMsg.linePa);
        }
        inform(
                     "[RECALL-START-ERR] node=%d kind=ReadShared linePA=0x%lx localPA=0x%lx reqId=%lu curT=%lu\n",
                     _nodeId, recallMsg.linePa, ownerLocalPa,
                     recallMsg.reqId, curTick());
        // R2: Clear stale recall capture data before initiating new recall
        setRecallCaptureData(DataBlock(64), false);
        _epRnfCtrl->startReadShared(ownerLocalPa,
            [this, capturedMsg](bool success) {
                if (_verboseLog) {
                DPRINTF(RubyEP, "[RECALL-DIAG] node=%d ReadShared callback success=%d valid=%d\n",
                        _nodeId, success, _recallCaptureDataValid);
                }
                inform(
                             "[RECALL-CB-ERR] node=%d kind=ReadShared linePA=0x%lx reqId=%lu success=%d valid=%d curT=%lu\n",
                             _nodeId, capturedMsg.linePa, capturedMsg.reqId,
                             success ? 1 : 0,
                             _recallCaptureDataValid ? 1 : 0, curTick());
                OuterRecallResponse resp;
                resp.linePa = capturedMsg.linePa;
                resp.ownerNode = capturedMsg.ownerNode;
                resp.homeNode = capturedMsg.homeNode;
                resp.epoch = capturedMsg.epoch;
                resp.reqId = capturedMsg.reqId;
                resp.ackReceived = success;
                resp.dataReturned = capturedMsg.dataNeeded && success &&
                                    _recallCaptureDataValid;
                // R2: Gate data payload on dataReturned (not raw _recallCaptureDataValid)
                if (resp.dataReturned) {
                    resp.dataPayload = _recallCaptureDataBlock;
                    resp.hasDataPayload = true;
                }
                // C4: Direct-forward data to requester (requester ≠ owner ≠ home)
                {
                    bool canForward = params().direct_fwd &&
                                      (capturedMsg.requesterNode >= 0 &&
                                       capturedMsg.requesterNode != capturedMsg.ownerNode &&
                                       capturedMsg.requesterNode != capturedMsg.homeNode);
                    if (canForward && resp.dataReturned && capturedMsg.requesterNode >= 0) {
                        if (getUBAdapter(0)) {
                            CoherenceMessage directData;
                            directData.h.type = CoherenceMessageType::ReadResp;
                            directData.h.srcNode = _nodeId;
                            directData.h.srcSocket = 0;
                            directData.h.dstNode = capturedMsg.requesterNode;
                            directData.h.dstSocket = capturedMsg.requesterSocket;
                            directData.h.homeNode = capturedMsg.homeNode;
                            directData.h.homeSocket = 0;
                            directData.h.homeLinePa = capturedMsg.linePa;
                            directData.h.epoch = capturedMsg.epoch;
                            // C4: reqId=0 so this ReadResp is NOT consumed
                            // by the requester's synchronous sendReadReq poll.
                            // The push-grant from home carries the full metadata.
                            directData.h.reqId = 0;
                            directData.h.flags = static_cast<uint32_t>(CFLAG_DATA_FORWARDED)
                                               | static_cast<uint32_t>(CFLAG_HAS_DATA)
                                               | static_cast<uint32_t>(CFLAG_DATA_RETURNED);
                            memcpy(directData.b.readResp.grantData,
                                   resp.dataPayload.getData(0, 64), 64);
                            getUBAdapter(0)->sendDirectData(directData);
                            resp.dataForwarded = true;
                            resp.dataForwardedTo = capturedMsg.requesterNode;
                            if (_verboseLog) {
                            DPRINTF(RubyEP, "[C4-FORWARD] RS node=%d forward data to requester=%d PA=0x%lx\n",
                                    _nodeId, capturedMsg.requesterNode, capturedMsg.linePa);
                            }
                        }
                    }
                }
                sendRecallResponse(resp);
            });
    } else {
        // Write recall: ReadUnique with RecallUnique proxy op
        if (_verboseLog) {
        DPRINTF(RubyEP, "[RECALL-DIAG] node=%d initiating ReadUnique recall PA=0x%lx\n",
                _nodeId, recallMsg.linePa);
        }
        inform(
                     "[RECALL-START-ERR] node=%d kind=ReadUnique linePA=0x%lx localPA=0x%lx reqId=%lu curT=%lu\n",
                     _nodeId, recallMsg.linePa, ownerLocalPa,
                      recallMsg.reqId, curTick());

        // R2: Clear stale recall capture data before initiating new recall
        setRecallCaptureData(DataBlock(64), false);
        _epRnfCtrl->startReadUnique(ownerLocalPa,
            [this, capturedMsg](bool success) {
                inform(
                             "[RECALL-PROXY-CALLBACK] node=%d homePA=0x%lx "
                             "reqId=%lu success=%d dataValid=%d tick=%lu\n",
                             _nodeId, capturedMsg.linePa, capturedMsg.reqId,
                             success ? 1 : 0,
                             _recallCaptureDataValid ? 1 : 0, curTick());
                if (_verboseLog) {
                DPRINTF(RubyEP, "[RECALL-DIAG] node=%d ReadUnique callback success=%d\n",
                        _nodeId, success);
                }
                inform(
                             "[RECALL-CB-ERR] node=%d kind=ReadUnique linePA=0x%lx reqId=%lu success=%d valid=%d curT=%lu\n",
                             _nodeId, capturedMsg.linePa, capturedMsg.reqId,
                             success ? 1 : 0,
                             _recallCaptureDataValid ? 1 : 0, curTick());
                OuterRecallResponse resp;
                resp.linePa = capturedMsg.linePa;
                resp.ownerNode = capturedMsg.ownerNode;
                resp.homeNode = capturedMsg.homeNode;
                resp.epoch = capturedMsg.epoch;
                resp.reqId = capturedMsg.reqId;
                resp.ackReceived = success;
                resp.dataReturned = capturedMsg.dataNeeded && success &&
                                    _recallCaptureDataValid;
                // R2: Gate data payload on dataReturned (not raw _recallCaptureDataValid)
                if (resp.dataReturned) {
                    resp.dataPayload = _recallCaptureDataBlock;
                    resp.hasDataPayload = true;
                }
                // C4: Direct-forward data to requester (requester ≠ owner ≠ home)
                {
                    bool canForward = params().direct_fwd &&
                                      (capturedMsg.requesterNode >= 0 &&
                                       capturedMsg.requesterNode != capturedMsg.ownerNode &&
                                       capturedMsg.requesterNode != capturedMsg.homeNode);
                    if (canForward && resp.dataReturned && capturedMsg.requesterNode >= 0) {
                        if (getUBAdapter(0)) {
                            CoherenceMessage directData;
                            directData.h.type = CoherenceMessageType::ReadResp;
                            directData.h.srcNode = _nodeId;
                            directData.h.srcSocket = 0;
                            directData.h.dstNode = capturedMsg.requesterNode;
                            directData.h.dstSocket = capturedMsg.requesterSocket;
                            directData.h.homeNode = capturedMsg.homeNode;
                            directData.h.homeSocket = 0;
                            directData.h.homeLinePa = capturedMsg.linePa;
                            directData.h.epoch = capturedMsg.epoch;
                            // C4: reqId=0 so this ReadResp is NOT consumed
                            // by the requester's synchronous sendReadReq poll.
                            directData.h.reqId = 0;
                            directData.h.flags = static_cast<uint32_t>(CFLAG_DATA_FORWARDED)
                                               | static_cast<uint32_t>(CFLAG_HAS_DATA)
                                               | static_cast<uint32_t>(CFLAG_DATA_RETURNED);
                            memcpy(directData.b.readResp.grantData,
                                   resp.dataPayload.getData(0, 64), 64);
                            getUBAdapter(0)->sendDirectData(directData);
                            resp.dataForwarded = true;
                            resp.dataForwardedTo = capturedMsg.requesterNode;
                            if (_verboseLog) {
                            DPRINTF(RubyEP, "[C4-FORWARD] RU node=%d forward data to requester=%d PA=0x%lx\n",
                                    _nodeId, capturedMsg.requesterNode, capturedMsg.linePa);
                            }
                        }
                    }
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
    if (_verboseLog) {
    DPRINTF(RubyEP, "[RECALL-RESP] node=%d PA=0x%lx homeNode=%d dataReturned=%d\n",
            _nodeId, response.linePa, response.homeNode, response.dataReturned);
    }
    inform(
                 "[RECALL-RESP-ERR] node=%d PA=0x%lx homeNode=%d reqId=%lu dataReturned=%d curT=%lu\n",
                 _nodeId, response.linePa, response.homeNode,
                 response.reqId, response.dataReturned ? 1 : 0, curTick());
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: sendRecallResponse "
            "PA=0x%lx homeNode=%d dataReturned=%d hasData=%d\n",
            _nodeId, response.linePa, response.homeNode,
            response.dataReturned, response.hasDataPayload);

    // Store for inspection
    _lastRecallResponse = response;
    _recallResponseSentCount++;

    // NOTE: active recall tracking is NOT cleared here.
    // The SnpCleanInvalid from the RECALL arrives AFTER sendRecallResponse
    // (HN-F invalidates the old copy after granting to the new owner).
    // The entry is cleared when the SnpCleanInvalid is handled in
    // EPRNFController::handleSnpCleanInvalid via clearActiveRecall().

    // R2: Require both dataReturned AND hasDataPayload before installing to home memory.
    //
    // NOTE (multi-process split): getBackendInstance(homeNode) only finds the
    // home node's EPBackend when home and owner run in the SAME process. In a
    // split (one gem5 process per node) build, a remote home returns nullptr
    // here, physMem becomes null, and HomeMemoryService::write() no-ops
    // (returns false). That is intentional: the authoritative delivery of
    // recall data to the home is the IPC sendRecallResp() below, which the
    // home node's UBCC/UBAdapter applies. This in-process write is only a
    // same-process fast path / redundant shortcut.
    // C4: Direct-forward sends extra copy to requester; home still needs the
    // data via RecallResp for its authoritative home-data grant construction.
    if (response.dataReturned && response.hasDataPayload) {
        EPBackend *homeBackend = EPBackend::getBackendInstance(response.homeNode);
        RubySystem *homeRuby = homeBackend ? homeBackend->getRubySystem() : nullptr;
        auto *physMem = homeRuby ? homeRuby->getPhysMem() : nullptr;
        HomeMemoryService hms(physMem);
        uint8_t buf[64] = {};
        memcpy(buf, response.dataPayload.getData(0, 64), 64);
        bool installed = hms.write(response.linePa, buf, 64);
        if (_verboseLog) {
        DPRINTF(RubyEP, "[RECALL-DIAG] home-install node=%d home=%d PA=0x%lx installed=%d hasData=%d\n",
                _nodeId, response.homeNode, response.linePa,
                installed, response.hasDataPayload);
        }
    }

    if (!getUBAdapter(0)) {
        fatal("EPBackend node_id=%d: UBAdapter required for recall response "
              "PA=0x%lx homeNode=%d\n",
              _nodeId, response.linePa, response.homeNode);
    }
    // RecallResp returns to the home directory plane; derive its socket from the
    // home line PA so it routes to ubio(homeNode, homeSocket) and matches the
    // outstanding RECALL there (hardcoding 0 stranded cross-socket recalls).
    int rrHomeSocket = _addrMap.homeSocket(response.homeNode, response.linePa);
    if (rrHomeSocket < 0) rrHomeSocket = 0;
    bool ok = getUBAdapter(0)->sendRecallResp(
        response.linePa, response.ownerNode, response.dataReturned,
        response.epoch, response.reqId,
        response.hasDataPayload ? &response.dataPayload : nullptr,
        response.homeNode, rrHomeSocket);

    if (!ok) {
        warn("EPBackend node_id=%d: home UBCC rejected recall response "
             "PA=0x%lx\n", _nodeId, response.linePa);
    }

    return ok;
}

bool
EPBackend::hasActiveRecall(uint64_t pa) const
{
    return _activeRecallPAs.find(pa) != _activeRecallPAs.end();
}

void
EPBackend::clearActiveRecall(uint64_t pa)
{
    auto erased = _activeRecallPAs.erase(pa);
    if (erased) {
        if (_verboseLog) {
        DPRINTF(RubyEP, "[RECALL-DIAG] node=%d active-recall-clear PA=0x%lx\n",
                _nodeId, pa);
        }
    }
}

bool
EPBackend::hasRequesterExclusive(uint64_t pa) const
{
    auto it = _requesterLines.find(pa);
    if (it == _requesterLines.end()) {
        if (_verboseLog) {
        DPRINTF(RubyEP, "[RE-DIAG] node=%d hasRequesterExclusive PA=0x%lx -> FALSE "
               "(no entry, total=%zu)\n",
               _nodeId, pa, _requesterLines.size());
        }
        return false;
    }
    int st = static_cast<int>(it->second.state);
    bool result = (it->second.state == RequesterLineState::R_E ||
                    it->second.state == RequesterLineState::R_M);
    if (_verboseLog) {
    DPRINTF(RubyEP, "[RE-DIAG] node=%d hasRequesterExclusive PA=0x%lx -> %s "
           "(state=%d, lineAddr=0x%lx)\n",
           _nodeId, pa, result ? "TRUE" : "FALSE",
           st, it->second.lineAddr);
    }
    return result;
}

// ---- M7: Writeback / Evict ----

void
EPBackend::handleHomeWritebackComplete(uint64_t homePa)
{
        if (_verboseLog) DPRINTF(RubyEP, "[EP-HOME-WB] node=%d pa=0x%lx\n", _nodeId, homePa);
    // v4-dual-socket: Send HomeWritebackNotify through adapter instead of
    // For single-socket backward compat, homeSocket = 0.
    int homeSocket = _addrMap.homeSocket(_nodeId, homePa);
    if (homeSocket < 0) homeSocket = 0;
    sendHomeWritebackNotify(homePa, homeSocket);
}

int
EPBackend::handleWriteback(uint64_t line_pa, bool keepAsClean,
                           const uint8_t *dirtyData,
                           const WritebackQueryMeta *queryMeta,
                           uint64_t *outQueryReqId,
                           uint64_t cachedQlmReqId,
                           int sourceSocket)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: handleWriteback PA=0x%lx "
            "keepAsClean=%d hasData=%d cachedQlmReqId=%lu\n",
            _nodeId, line_pa, keepAsClean,
            dirtyData != nullptr, cachedQlmReqId);

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

    uint64_t offset = _addrMap.dsmOffset(line_pa);
    int homeSocket = _addrMap.homeSocket(_nodeId, line_pa);
    if (homeSocket < 0) homeSocket = 0;

    uint64_t epochVal = 0;
    int requesterNode = _nodeId;
    auto it = _requesterLines.find(line_pa);

    if (queryMeta && queryMeta->valid) {
        // Phase 2 async: use pre-resolved metadata — skip QLM entirely
        epochVal = queryMeta->epochVal;
        requesterNode = queryMeta->requesterNode;
        DPRINTF(RubyEP,
            "[EP-WB-CACHED-META] node=%d pa=0x%lx epoch=%lu owner=%d keepAsClean=%d\n",
            _nodeId, line_pa, epochVal, requesterNode, keepAsClean);
    } else if (it != _requesterLines.end()) {
        epochVal = it->second.epoch;
    } else {
        // Query home UBCC via async QLM (Phase 2)
        uint64_t qEpoch = 0;
        int qOwnerNode = -1;
        bool qFound = false;
        UBAdapter *wa = getUBAdapter(sourceSocket);
        if (wa) {
            int qRet = wa->sendQueryLineMetaReq(line_pa, homeNode, homeSocket,
                                                qEpoch, qOwnerNode, qFound,
                                                outQueryReqId,
                                                cachedQlmReqId);
            if (qRet == -2) {
                // QLM is in-flight (or re-check with cachedReqId returned -2
                // meaning the response hasn't arrived yet).  Caller must retry.
                DPRINTF(RubyEP,
                    "[EP-QLM-PENDING] node=%d pa=0x%lx cachedReqId=%lu\n",
                    _nodeId, line_pa, cachedQlmReqId);
                return -2;
            }
        }
        if (qFound && qOwnerNode >= 0) {
            epochVal = qEpoch;
            requesterNode = qOwnerNode;
        } else {
            // ── Phase 2 corrective: found=false, owner<0, epoch=0 ──
            // These are terminal failures.  Do NOT fabricate an epoch and
            // do NOT silently drop dirty data.  Report explicit diagnostics
            // and return a definitive failure so the caller can retire the
            // pending writeback with audit evidence.
            warn(
                "[EP-WB-QLM-FAIL] node=%d pa=0x%lx found=%d owner=%d epoch=%lu "
                "cachedReqId=%lu — terminal, NOT fabricating epoch\n",
                _nodeId, line_pa, qFound ? 1 : 0, qOwnerNode, qEpoch,
                cachedQlmReqId);
            return -3;   // terminal failure — retire pending writeback
        }
    }

    return handleWritebackWithMeta(line_pa, keepAsClean, dirtyData,
                                    epochVal, requesterNode, sourceSocket);
}

int
EPBackend::handleWritebackWithMeta(uint64_t line_pa, bool keepAsClean,
                                    const uint8_t *dirtyData,
                                    uint64_t epochVal, int requesterNode,
                                    int sourceSocket)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: handleWritebackWithMeta PA=0x%lx "
            "epoch=%lu requester=%d keepAsClean=%d\n",
            _nodeId, line_pa, epochVal, requesterNode, keepAsClean);

    int homeNode = homeNodeCrossNode(line_pa);
    if (homeNode < 0) {
        fatal("EPBackend node_id=%d: invalid home node for writeback "
              "PA=0x%lx\n", _nodeId, line_pa);
    }
    uint64_t offset = _addrMap.dsmOffset(line_pa);
    int homeSocket = _addrMap.homeSocket(_nodeId, line_pa);
    if (homeSocket < 0) homeSocket = 0;
    uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode, offset, homeSocket);

    if (_verboseLog) {
    DPRINTF(RubyEP, "[EP-HANDLE-WB] node=%d pa=0x%lx epoch=%lu requester=%d keepAsClean=%d\n",
            _nodeId, line_pa, epochVal, requesterNode, keepAsClean);
    }

    // Build writeback message envelope
    _lastWritebackMsg.linePa = homePa;
    _lastWritebackMsg.requesterNode = requesterNode;
    _lastWritebackMsg.homeNode = homeNode;
    _lastWritebackMsg.epoch = epochVal;
    _lastWritebackMsg.keepAsClean = keepAsClean;

    UBAdapter *adapter = getUBAdapter(sourceSocket);
    if (!adapter) {
        fatal("EPBackend node_id=%d: UBAdapter required for writeback "
              "PA=0x%lx homeNode=%d sourceSocket=%d\n",
              _nodeId, line_pa, homeNode, sourceSocket);
    }
    int wbRet = adapter->sendWritebackReq(
        homePa, requesterNode, epochVal, keepAsClean, homeNode, homeSocket,
        dirtyData);
    bool wbPending = (wbRet == -2);
    bool ok = (wbRet > 0);
    if (wbPending) {
        DPRINTF(RubyEP,
                     "[EP-WB-PENDING] node=%d pa=0x%lx home=%d epoch=%lu\n",
                     _nodeId, homePa, homeNode, epochVal);
    }

    // Build ack envelope
    _lastAckMsg.linePa = homePa;
    _lastAckMsg.homeNode = homeNode;
    _lastAckMsg.epoch = epochVal;
    _lastAckMsg.success = ok || wbPending;

    // Update requester bookkeeping based on result
    auto it = _requesterLines.find(line_pa);
    if (ok) {
        _writebackCount++;
        if (it != _requesterLines.end()) {
            if (keepAsClean) {
                // Owner retains clean exclusive (G_E)
                it->second.state = RequesterLineState::R_E;
                if (_verboseLog) {
                DPRINTF(RubyEP, "[RE-DIAG] node=%d line 0x%lx -> R_E (writeback keepAsClean)\n",
                        _nodeId, line_pa);
                }
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

    return ok || wbPending;
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
    int homeSocket = _addrMap.homeSocket(_nodeId, line_pa);
    if (homeSocket < 0) homeSocket = 0;
    uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode, offset, homeSocket);

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

    if (!getUBAdapter(0)) {
        fatal("EPBackend node_id=%d: UBAdapter required for evict "
              "PA=0x%lx homeNode=%d\n",
              _nodeId, line_pa, homeNode);
    }
    int evRet = getUBAdapter(0)->sendEvictReq(homePa, _nodeId, epochVal,
                                              homeNode, homeSocket);
    bool evPending = (evRet == -2);
    bool ok = (evRet > 0);
    if (evPending) {
        DPRINTF(RubyEP,
                     "[EP-EVICT-PENDING] node=%d pa=0x%lx home=%d epoch=%lu\n",
                     _nodeId, homePa, homeNode, epochVal);
    }

    // Build ack envelope
    _lastAckMsg.linePa = homePa;
    _lastAckMsg.homeNode = homeNode;
    _lastAckMsg.epoch = epochVal;
    _lastAckMsg.success = ok || evPending;

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

    return ok || evPending;
}

// ---- M8: Global Invalidation Management ----

bool
EPBackend::handleInvalidationRequest(const OuterInvalidateMsg &invMsg)
{
    if (_verboseLog) {
    DPRINTF(RubyEP, "[INVAL-DIAG] node=%d handleInvalidationRequest PA=0x%lx home=%d sharerLocalPA=0x%lx\n",
            _nodeId, invMsg.linePa, invMsg.homeNode, invMsg.sharerLocalPa);
    }
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

    // Update requester-side bookkeeping. Capture the PRE-invalidation state so
    // we can tell whether this node actually held a copy of the line.
    uint64_t lookupPa = (invMsg.sharerLocalPa != 0)
                           ? invMsg.sharerLocalPa
                           : invMsg.linePa;
    bool hadLocalCopy = false;
    {
        auto it = _requesterLines.find(lookupPa);
        if (it != _requesterLines.end()) {
            // A copy is held only in R_S/R_E/R_M. R_I means already invalid and
            // R_WAIT_GRANT means a request is in flight but no copy is held yet.
            RequesterLineState st = it->second.state;
            hadLocalCopy = (st == RequesterLineState::R_S ||
                            st == RequesterLineState::R_E ||
                            st == RequesterLineState::R_M);
            it->second.state = RequesterLineState::R_I;
        }
    }

// If a SnpCleanInvalid-upgrade is currently held for this line, do NOT
    // issue startCleanUnique to the local HN-F.
    if (_epRnfCtrl) {
        // If the held upgrade was rejected by the home, abandon this upgrade.
        // Send SnpResp_I to the local HN-F so its CleanUnique #1 completes as
        // stale (CompUCRespStale) — L2 learns it did NOT get exclusive, so the
        // store will retry with a fresh ReadUnique/CleanUnique. This is safe
        // (no split-brain): L2 knows it doesn't own the line. Also send
        // InvalidateAck directly (bypass startCleanUnique — no second
        // CleanUnique to the HN-F). After the store retries, a new upgrade
        // request will be issued when the line is re-fetched.
        if (_epRnfCtrl->isHeldUpgradeRejected(lookupPa)) {
            // TC16 dual-upgrade race LOSER path (abandon-and-downgrade).
            //
            // Our global OuterUpgradeReq was rejected by home because another
            // node won the race and took ownership. The winner's write made our
            // shared (SC) copy STALE, so an upgrade (S->M) is no longer valid —
            // we must recall the winner's fresh data (I->M) instead. But the L2
            // requestor already issued a local CleanUnique that is parked at the
            // local HN-F (BUSY_BLKD) waiting for our held SnpResp_I.
            //
            // We therefore release the held snoop with SnpResp_I marked STALE.
            // The local HN-F, seeing stale on a CleanUnique's terminating snoop
            // response, removes the requestor from dir_sharers and completes the
            // CleanUnique as Comp_UC(stale=1). The L2 detects stale and re-issues
            // a fresh ReadUnique, which recalls the winner's data from home. No
            // split-brain: the L2 never enters UC on the stale completion.
            //
            // We also ack the winner directly so its invalidation fanout drains
            // (bypassing startCleanUnique — there is no second CleanUnique).
            DPRINTF(RubyEP,
                    "EPBackend node_id=%d: InvalidateReq PA=0x%lx — held "
                    "upgrade rejected; abandon via stale SnpResp_I + ack winner\n",
                    _nodeId, lookupPa);
            MachineID hnfDest = _epRnfCtrl->getHeldUpgradeHnfDest(lookupPa);
            uint64_t hnfRaw = ((uint64_t)hnfDest.type << 24) | hnfDest.num;
            _epRnfCtrl->clearHeldUpgrade(lookupPa);
            sendSnpRespIForRejected(lookupPa, hnfRaw);  // stale=true
            OuterInvalidationAck ack;
            ack.linePa = invMsg.linePa;
            ack.ackNode = _nodeId;
            ack.homeNode = invMsg.homeNode;
            ack.epoch = invMsg.epoch;
            ack.reqId = invMsg.reqId;
            sendInvalidationAck(ack);
            return true;
        }
        // Held upgrade still pending (not rejected) — defer the InvalidateReq
        // until the held snoop is resolved.
        if (_epRnfCtrl->hasHeldUpgrade(lookupPa)) {
            DPRINTF(RubyEP,
                    "EPBackend node_id=%d: deferring InvalidateReq PA=0x%lx — "
                    "held snoop in progress\n",
                    _nodeId, lookupPa);
            _deferredInvalidationReqs[lookupPa] = invMsg;
            return true;
        }
    }

    // fix (stale-sharer invalidation): if this node holds NO local copy of the
    // line (already recalled / evicted / never had one) and has no held upgrade
    // snoop in progress, there is nothing to CleanUnique. startCleanUnique would
    // stall forever (its callback never fires with no line to clean), leaving
    // the home's INVALIDATE outstanding stuck in WAITING_ALL_ACKS and
    // deadlocking all later upgrades (TC98). Invalidating an already-absent copy
    // is idempotent, so ack immediately. This is the requester-side complement
    // to keeping the home directory's sharer mask fresh: even if a stale sharer
    // slips into the target mask, the invalidation still drains.
    if (!hadLocalCopy) {
        if (_verboseLog) {
        DPRINTF(RubyEP, "[INVAL-DIAG] node=%d no local copy PA=0x%lx — immediate ack "
               "(stale sharer)\n",
               _nodeId, lookupPa);
        }
        DPRINTF(RubyEP,
                "EPBackend node_id=%d: InvalidateReq PA=0x%lx — no local copy, "
                "acking immediately (idempotent)\n",
                _nodeId, lookupPa);
        OuterInvalidationAck ack;
        ack.linePa = invMsg.linePa;
        ack.ackNode = _nodeId;
        ack.homeNode = invMsg.homeNode;
        ack.epoch = invMsg.epoch;
        ack.reqId = invMsg.reqId;
        sendInvalidationAck(ack);
        return true;
    }

    // ---- v4 (§4.2.4): FIXED — use EP-RNF.startCleanUnique, wait for
    // callback before sending invalidation ack.  Previous code directly
    // ack'd, bypassing HN-F and losing grant/invalidation serialization.
    if (_epRnfCtrl) {
        // Capture invMsg by value for the callback
        OuterInvalidateMsg capturedMsg = invMsg;
        if (_verboseLog) {
        DPRINTF(RubyEP, "[INVAL-DIAG] node=%d calling startCleanUnique PA=0x%lx\n",
                _nodeId, capturedMsg.sharerLocalPa);
        }
        _epRnfCtrl->startCleanUnique(
            capturedMsg.sharerLocalPa,
            [this, capturedMsg](bool ok) {
                if (_verboseLog) {
                DPRINTF(RubyEP, "[INVAL-DIAG] node=%d startCleanUnique callback PA=0x%lx ok=%d\n",
                        _nodeId, capturedMsg.linePa, ok);
                }
                OuterInvalidationAck ack;
                ack.linePa = capturedMsg.linePa;
                ack.ackNode = _nodeId;
                ack.homeNode = capturedMsg.homeNode;
                ack.epoch = capturedMsg.epoch;
                ack.reqId = capturedMsg.reqId;
                sendInvalidationAck(ack);
            });
        return true;
    } else {
        fatal("EPBackend node_id=%d: invalidation path requires EP-RNF "
              "controller; direct InvalidateAck would bypass CHI barrier "
              "for PA=0x%lx\n",
              _nodeId, invMsg.linePa);
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

    if (!getUBAdapter(0)) {
        fatal("EPBackend node_id=%d: UBAdapter required for invalidation ack "
              "PA=0x%lx homeNode=%d\n",
              _nodeId, ack.linePa, ack.homeNode);
    }
    int ackHomeSocket = _addrMap.homeSocket(ack.homeNode, ack.linePa);
    if (ackHomeSocket < 0) ackHomeSocket = 0;
    bool ok = getUBAdapter(0)->sendInvalidateAck(
        ack.linePa, ack.ackNode, ack.epoch, ack.reqId,
        ack.homeNode, ackHomeSocket);

    if (!ok) {
        warn("EPBackend node_id=%d: home UBCC rejected invalidation ack "
             "PA=0x%lx\n", _nodeId, ack.linePa);
    }

    return ok;
}

// ---- v4: Local Upgrade Management (§4.1.4, §4.2.3) ----

bool
EPBackend::notifyLocalWriteUpgrade(uint64_t line_pa, int homeNode,
                                    int sourceSocket,
                                    int desiredPerm, UpgradeCause cause,
                                    uint64_t &outEpoch, uint64_t &outReqId,
                                    bool *outRejected, bool *outNotSharer,
                                    bool forceResend)
{
    if (outRejected) *outRejected = false;
    if (outNotSharer) *outNotSharer = false;
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: notifyLocalWriteUpgrade "
            "PA=0x%lx homeNode=%d desiredPerm=%d\n",
             _nodeId, line_pa, homeNode, desiredPerm);

    fatal_if(sourceSocket < 0 || sourceSocket >= _numSockets,
             "EPBackend node_id=%d: invalid upgrade source socket %d PA=0x%lx",
             _nodeId, sourceSocket, line_pa);

    // Translate local PA to home PA
    uint64_t offset = _addrMap.dsmOffset(line_pa);
    int homeSocket = _addrMap.homeSocket(_nodeId, line_pa);
    if (homeSocket < 0) homeSocket = 0;
    uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode, offset, homeSocket);

    // Reuse reqId/epoch if an async upgrade is already pending for this line
    // (sendUpgradeReq returned -2 last time). Allocating a fresh reqId on every
    // snoop retry made the home reject the duplicate (existing outstanding) and
    // loop forever (TC3/8/10/11).
    auto put = _pendingUpgradeTxns.find(line_pa);
    const bool hadPending = (put != _pendingUpgradeTxns.end() && put->second.valid);
    fatal_if(hadPending && put->second.sourceSocket != sourceSocket,
             "EPBackend node_id=%d: pending upgrade socket changed PA=0x%lx "
             "old=%d new=%d", _nodeId, line_pa, put->second.sourceSocket,
             sourceSocket);

    uint64_t epochVal;
    uint64_t reqIdVal;
    if (hadPending) {
        epochVal = put->second.epoch;
        reqIdVal = put->second.reqId;
    } else {
        _epochCounter++;
        epochVal = _epochCounter;
        reqIdVal = makeRequesterReqId(_nodeId, _epochCounter);
    }
    const Tick upgradeStartTick = hadPending ? put->second.startTick : curTick();

    UBAdapter *adapter = getUBAdapter(sourceSocket);
    if (!adapter) {
        fatal("EPBackend node_id=%d: UBAdapter required for upgrade "
              "PA=0x%lx homeNode=%d sourceSocket=%d\n",
              _nodeId, line_pa, homeNode, sourceSocket);
    }

    // Send OuterUpgradeReq to home UBCC via message passing
    OuterUpgradeReq upgradeReq;
    upgradeReq.linePa = homePa;
    upgradeReq.srcNode = _nodeId;
    upgradeReq.epoch = epochVal;
    upgradeReq.reqId = reqIdVal;
    upgradeReq.desiredPerm = desiredPerm;
    upgradeReq.cause = cause;
    _lastUpgradeReq = upgradeReq;

    // Publish the stable tuple before transportSend(). The Port path can
    // deliver an UpgradeResp and re-enter completeHeldUpgrade() before the
    // original sendUpgradeReq() returns. Without this pre-registration, that
    // callback observes no pending transaction and emits a duplicate request
    // with the same reqId; the accepted and temporary-reject responses then
    // race and can drive fresh-reqId churn.
    if (!hadPending) {
        PendingUpgradeTxn txn;
        txn.valid = true;
        txn.linePa = line_pa;
        txn.homeNode = homeNode;
        txn.sourceSocket = sourceSocket;
        txn.epoch = epochVal;
        txn.reqId = reqIdVal;
        txn.startTick = upgradeStartTick;
        txn.acceptedPending = false;
        _pendingUpgradeTxns[line_pa] = txn;
        put = _pendingUpgradeTxns.find(line_pa);
    }

    uint64_t upgradeTargetMask = 0;
    uint64_t committedEpoch = 0;
    // DROP/NO-RESP recovery: when forceResend is set (watchdog fired for a
    // held pending upgrade), retransmit the OuterUpgradeReq with the SAME
    // reqId/epoch instead of merely polling (checkOnly). Reusing the same reqId
    // keeps the home's dedup idempotent: if the original was dropped the home
    // processes it fresh; if it was already accepted the home returns the cached
    // grant (WAITING_CLEAR path) rather than a spurious "existing outstanding"
    // reject. We must NOT allocate a fresh reqId (that churns the home into
    // rejecting every retransmit — the TC3/8/10/11 livelock).
    const bool checkOnly = hadPending && !forceResend;
    if (forceResend) {
        // The first accepted response can still be cached with a non-zero
        // target mask while its later UpgradeAckNotify is the message that was
        // lost. A recovery resend must bypass that stale stage and actually
        // reach the home with the same tuple so it can replay current progress.
        adapter->clearReadyResponsesForLine(homePa);
    }
    int upgradeRet = adapter->sendUpgradeReq(
        homePa, _nodeId, epochVal, reqIdVal,
        desiredPerm, static_cast<int>(cause),
        &upgradeTargetMask, &committedEpoch, homeNode, homeSocket,
        checkOnly /*checkOnly: don't re-send an in-flight upgrade*/,
        forceResend /*forceWire: bypass stale accepted-pending response*/);
    if (upgradeRet == -2) {
        DPRINTF(RubyEP,
                     "[EP-UPGRADE-PENDING] node=%d pa=0x%lx home=%d epoch=%lu reqId=%lu\n",
                     _nodeId, homePa, homeNode, epochVal, reqIdVal);
        // Save the pending reqId/epoch so the next snoop retry reuses them and
        // hits the cached UpgradeResp, rather than allocating a new reqId that
        // the home rejects (existing outstanding).
        PendingUpgradeTxn txn;
        txn.valid = true;
        txn.linePa = line_pa;
        txn.homeNode = homeNode;
        txn.sourceSocket = sourceSocket;
        txn.epoch = epochVal;
        txn.reqId = reqIdVal;
        txn.startTick = upgradeStartTick;
        txn.acceptedPending = hadPending && put->second.acceptedPending;
        _pendingUpgradeTxns[line_pa] = txn;
        return false;
    }
    // Keep the stable tuple while an accepted upgrade is still waiting for
    // remote invalidation acks. If UpgradeAckNotify is lost, the held-upgrade
    // watchdog must resend the same reqId and let the home replay its current
    // accepted stage. Immediate upgrades have no notification to wait for.
    bool accepted = (upgradeRet > 0);
    if (upgradeRet == 0 && hadPending && put->second.acceptedPending) {
        // Accepted-pending is monotonic for a stable tuple. A temporary reject
        // with the same reqId can be an older duplicate response that arrived
        // after the accepted response; it must not clear the tuple and start
        // fresh-reqId retries while the home is waiting for UpgradeDone.
        warn(
            "[EP-UPGRADE-STALE-REJECT] node=%d pa=0x%lx epoch=%lu reqId=%lu "
            "ignored_after_accepted_pending=1\n",
            _nodeId, homePa, epochVal, reqIdVal);
        return false;
    }
    if (accepted) {
        inform(
            "[EP-PERF] kind=upgrade_network node=%d pa=0x%lx reqId=%lu "
            "start=%lu end=%lu latency_ps=%lu\n",
            _nodeId, homePa, reqIdVal, upgradeStartTick, curTick(),
            curTick() - upgradeStartTick);
    }

    // upgradeRet == 0 means the home explicitly rejected this upgrade (an
    // upgrade for this line is already outstanding for another requester).
    // upgradeRet == -2 means pending (no response yet — possibly dropped),
    // but that path already returned false above without setting rejected.
    // Only an explicit TEMP-REJECT (upgradeRet == 0) sets rejected=true so
    // completeHeldUpgrade can distinguish TEMP-REJECT (exponential backoff)
    // from a still-pending / DROPped upgrade (hold + watchdog resend).
    // (Two requesters upgrading the same line otherwise deadlock: each holds
    // its SnpResp_I waiting for its own upgrade, so neither can be invalidated
    // for the other's upgrade. TC16/25/53 double-upgrade race.)
    if (upgradeRet == 0 && outRejected)
        *outRejected = true;
    // upgradeRet == -3: PERMANENT reject (requester no longer a committed
    // sharer — lost a dual-upgrade race). Caller must abandon + ReadUnique.
    // upgradeRet == 0: TEMPORARY reject (existing outstanding) — retry later.
    if ((upgradeRet == -3) && outNotSharer)
        *outNotSharer = true;

    if (accepted) {
        // Store returned values (reservedEpoch, echoed reqId)
        outEpoch = epochVal;
        outReqId = reqIdVal;

        if (upgradeTargetMask != 0) {
            // Other sharers exist — must invalidate them before Ack(true).
            //
            // fix1 (home-owned invalidation fanout): the InvalidateReq fanout to
            // each target sharer is now emitted by the HOME UBCC when it creates
            // the WAITING_ALL_ACKS outstanding (UBCCController::processOuterUpgradeReq
            // → fanoutInvalidateTargets), unifying it with the plain-INVALIDATE
            // path. The requester side must NOT also fan out here: doing both
            // caused split ownership where, in the hot-line RS/RU + recall +
            // batch-RS-replay interleaving, the fanout could be dropped, orphaning
            // the outstanding (WAITING_ALL_ACKS forever) and deadlocking all later
            // upgrades (TC98 stall at transfer #10). The requester only records
            // the deferred (accepted=false) Ack and waits for the home to signal
            // completion once all acks land.
            if (_verboseLog) {
            DPRINTF(RubyEP, "[UPGRADE-DIAG] node=%d upgrade accepted PENDING PA=0x%lx "
                   "targetMask=0x%lx — home owns fanout (requester defers Ack)\n",
                   _nodeId, line_pa, upgradeTargetMask);
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

            PendingUpgradeTxn txn;
            txn.valid = true;
            txn.linePa = line_pa;
            txn.homeNode = homeNode;
            txn.sourceSocket = sourceSocket;
            txn.epoch = epochVal;
            txn.reqId = reqIdVal;
            txn.startTick = upgradeStartTick;
            txn.acceptedPending = true;
            _pendingUpgradeTxns[line_pa] = txn;
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

            if (put != _pendingUpgradeTxns.end())
                put->second.valid = false;

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
EPBackend::sendUpgradeDone(uint64_t line_pa, int homeNode, int sourceSocket,
                            uint64_t epoch, uint64_t reqId)
{
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: sendUpgradeDone "
            "PA=0x%lx homeNode=%d epoch=%lu reqId=%lu\n",
            _nodeId, line_pa, homeNode, epoch, reqId);

    uint64_t offset = _addrMap.dsmOffset(line_pa);
    int homeSocket = _addrMap.homeSocket(_nodeId, line_pa);
    if (homeSocket < 0) homeSocket = 0;
    uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode, offset, homeSocket);

    UBAdapter *adapter = getUBAdapter(sourceSocket);
    if (!adapter) {
        fatal("EPBackend node_id=%d: UBAdapter required for upgrade done "
              "PA=0x%lx homeNode=%d sourceSocket=%d\n",
              _nodeId, line_pa, homeNode, sourceSocket);
    }

    OuterUpgradeDone doneMsg;
    doneMsg.linePa = homePa;
    doneMsg.srcNode = _nodeId;
    doneMsg.homeNode = homeNode;
    doneMsg.epoch = epoch;
    doneMsg.reqId = reqId;
    _lastUpgradeDone = doneMsg;

    int doneRet = adapter->sendUpgradeDoneReq(
        homePa, _nodeId, epoch, reqId, homeNode, homeSocket);
    bool donePending = (doneRet == -2);
    bool accepted = (doneRet > 0);
    if (donePending) {
        DPRINTF(RubyEP,
                     "[EP-UPGDONE-PENDING] node=%d pa=0x%lx home=%d epoch=%lu reqId=%lu\n",
                     _nodeId, homePa, homeNode, epoch, reqId);
    }

    OuterUpgradeDoneAck doneAck;
    doneAck.linePa = homePa;
    doneAck.homeNode = homeNode;
    doneAck.dstNode = _nodeId;
    doneAck.epoch = epoch;
    doneAck.reqId = reqId;
    doneAck.accepted = accepted || donePending;
    _lastUpgradeDoneAck = doneAck;

    return accepted || donePending;
}

// ---- v4: Clear / ClearAck (§3.5) ----

    int
EPBackend::sendClear(uint64_t line_pa, int homeNode,
                     uint64_t epoch, uint64_t reqId, int sourceAdapter)
{
    inform(
                 "[CLEAR-SEND] node=%d pa=0x%lx homeNode=%d epoch=%lu reqId=%lu\n",
                 _nodeId, line_pa, homeNode, epoch, reqId);
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: sendClear "
            "PA=0x%lx homeNode=%d epoch=%lu reqId=%lu\n",
            _nodeId, line_pa, homeNode, epoch, reqId);

    // upgrade_invalidate_fix D5: prefer PendingGrantTxn.baseEpoch
    uint64_t clearEpoch = epoch;
    auto txnIt = _pendingGrantTxns.find(line_pa);
    bool foundPendingGrantTxn =
        (txnIt != _pendingGrantTxns.end() && txnIt->second.valid);
    if (txnIt != _pendingGrantTxns.end() && txnIt->second.valid) {
        clearEpoch = txnIt->second.baseEpoch;
        // Do NOT invalidate here. The grant/clear handshake is asynchronous:
        // the first sendClear typically returns -2 (ClearResp not yet here) and
        // the line must be retried. Consuming the txn on first use let the line
        // leave R_WAIT_GRANT, get a new reqId on retry, and never match the
        // cached ClearResp. We invalidate below only once the clear is accepted.
    }

    DPRINTF(RubyEP, "[DEBUG-TC5-CLEAR-TRACE] sendClear node=%d linePA=0x%lx homeNode=%d "
            "callerEpoch=%lu clearEpoch=%lu reqId=%lu pendingTxnHit=%d\n",
            _nodeId, line_pa, homeNode, epoch, clearEpoch, reqId,
            foundPendingGrantTxn);

    OuterClearMsg clearMsg;
    clearMsg.linePa = line_pa;
    clearMsg.srcNode = _nodeId;
    clearMsg.homeNode = homeNode;
    clearMsg.epoch = clearEpoch;
    clearMsg.reqId = reqId;
    clearMsg.reason = ClearReason::GrantHandshake;
    _lastClearMsg = clearMsg;

    UBAdapter *adapter = getUBAdapter(sourceAdapter);
    if (!adapter) {
        fatal("EPBackend node_id=%d: UBAdapter required for clear "
              "PA=0x%lx homeNode=%d sourceAdapter=%d\n",
              _nodeId, line_pa, homeNode, sourceAdapter);
    }
    // line_pa here is the home PA (socket-encoded); derive its home socket so
    // the ClearReq routes to the home plane's ubio (matching the original
    // grant). Hardcoding 0 sent cross-socket clears to the wrong plane, so the
    // grant handshake never completed and the requester deadlocked.
    int clearHomeSocket = _addrMap.homeSocket(homeNode, line_pa);
    if (clearHomeSocket < 0) clearHomeSocket = 0;
    int clearRet = adapter->sendClearReq(
        line_pa, _nodeId, clearEpoch, reqId, homeNode, clearHomeSocket);
    bool accepted = (clearRet > 0);  // -2=pending, -1=error, 0=rejected, 1=accepted

    // Consume the pending grant txn only once the clear is actually accepted,
    // so retries while it is still pending (clearRet==-2) keep matching reqId.
    if (accepted && txnIt != _pendingGrantTxns.end() && txnIt->second.valid) {
        txnIt->second.valid = false;
    }

    DPRINTF(RubyEP, "[DEBUG-TC5-CLEAR-TRACE] sendClearResult node=%d linePA=0x%lx homeNode=%d "
            "clearEpoch=%lu reqId=%lu accepted=%d\n",
            _nodeId, line_pa, homeNode, clearEpoch, reqId, accepted);

    OuterClearAckMsg ack;
    ack.linePa = line_pa;
    ack.homeNode = homeNode;
    ack.dstNode = _nodeId;
    ack.epoch = epoch;
    ack.reqId = reqId;
    ack.accepted = accepted;
    _lastClearAckMsg = ack;

    return clearRet;
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
            int homeSocket = _addrMap.homeSocket(homeNode, linePa);
            if (homeSocket < 0) homeSocket = 0;
            callbackPa = _addrMap.buildDsmPA(_nodeId, homeNode, offset,
                                             homeSocket);
            _lastUpgradeAck.accepted = true;
            clearPendingUpgradeTxn(callbackPa);
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

void
EPBackend::clearPendingUpgradeTxn(uint64_t linePa)
{
    auto it = _pendingUpgradeTxns.find(linePa);
    if (it != _pendingUpgradeTxns.end())
        it->second.valid = false;
}

void
EPBackend::sendSnpRespIForRejected(uint64_t linePa, uint64_t hnfDestRaw)
{
    if (_epRnfCtrl) {
        MachineID hnfDest;
        hnfDest.type = (MachineType)(hnfDestRaw >> 24);
        hnfDest.num = (NodeID)(hnfDestRaw & 0xFFFFFF);
        // staleMark=true: abandon path — tells local HN-F this CleanUnique must
        // complete as stale (Comp_UC stale=1), so the L2 downgrades to ReadUnique.
        _epRnfCtrl->sendSnpRespI(linePa, hnfDest, true);
    }
}

void
EPBackend::clearCachedUpgradeResp(uint64_t linePa, int sourceSocket)
{
    // Clear any rejected UpgradeResp from the UBAdapter's ready-response cache
    // so the next upgrade attempt sends a fresh UpgradeReq instead of hitting
    // the stale rejected response.
    UBAdapter *adapter = getUBAdapter(sourceSocket);
    fatal_if(!adapter,
             "EPBackend node_id=%d: missing upgrade response adapter socket=%d "
             "PA=0x%lx", _nodeId, sourceSocket, linePa);
    adapter->clearReadyResponsesForLine(linePa);
}

void
EPBackend::flushDeferredInvalidation(uint64_t linePa)
{
    // Called after a held snoop has been resolved (SnpResp_I sent, local copy
    // invalidated). Process any InvalidateReq that was deferred because it
    // arrived while the snoop was held. Since the SnpResp_I already
    // invalidated the local copy, we can ack directly without startCleanUnique.
    auto it = _deferredInvalidationReqs.find(linePa);
    if (it == _deferredInvalidationReqs.end())
        return;
    OuterInvalidateMsg invMsg = it->second;
    _deferredInvalidationReqs.erase(it);

    // TC16 dual-upgrade race LOSER: if the held upgrade for this line was
    // REJECTED by home, the deferred InvalidateReq must drive the stale-abandon
    // path (release the held snoop with SnpResp_I(stale=1) so the local HN-F
    // completes the CleanUnique as stale and the L2 downgrades to ReadUnique),
    // NOT a bare ack. A bare ack would leave the held CleanUnique parked at the
    // HN-F forever (Sequencer deadlock).
    if (_epRnfCtrl && _epRnfCtrl->isHeldUpgradeRejected(linePa)) {
        DPRINTF(RubyEP,
                "EPBackend node_id=%d: flushing deferred InvalidateReq PA=0x%lx "
                "— held upgrade rejected, abandon via stale SnpResp_I + ack\n",
                _nodeId, invMsg.linePa);
        MachineID hnfDest = _epRnfCtrl->getHeldUpgradeHnfDest(linePa);
        uint64_t hnfRaw = ((uint64_t)hnfDest.type << 24) | hnfDest.num;
        _epRnfCtrl->clearHeldUpgrade(linePa);
        sendSnpRespIForRejected(linePa, hnfRaw);  // stale=true
        OuterInvalidationAck ackR;
        ackR.linePa = invMsg.linePa;
        ackR.ackNode = _nodeId;
        ackR.homeNode = invMsg.homeNode;
        ackR.epoch = invMsg.epoch;
        ackR.reqId = invMsg.reqId;
        sendInvalidationAck(ackR);
        return;
    }

    DPRINTF(RubyEP,
            "EPBackend node_id=%d: flushing deferred InvalidateReq "
            "PA=0x%lx home=%d — direct ack (copy already invalidated)\n",
            _nodeId, invMsg.linePa, invMsg.homeNode);

    OuterInvalidationAck ack;
    ack.linePa = invMsg.linePa;
    ack.ackNode = _nodeId;
    ack.homeNode = invMsg.homeNode;
    ack.epoch = invMsg.epoch;
    ack.reqId = invMsg.reqId;
    sendInvalidationAck(ack);
}

void
EPBackend::onUpgradeRespArrived(uint64_t reqId)
{
    // Event-driven completion of a held SnpCleanInvalid-upgrade. Called when an
    // OuterUpgradeResp arrives on the async Port path (UBAdapter). Without this,
    // a no-other-sharers upgrade (which sends NO UpgradeAckNotify) would have to
    // wait for the snoop to be re-issued to pull the cached UpgradeResp — i.e.
    // the busy-wait livelock (TC16/25/53). Instead we proactively drive the
    // held upgrade to completion as soon as its response is available.
    if (!_epRnfCtrl)
        return;

    for (auto &kv : _pendingUpgradeTxns) {
        if (kv.second.valid && kv.second.reqId == reqId) {
            DPRINTF(RubyEP,
                    "EPBackend node_id=%d: onUpgradeRespArrived reqId=%lu "
                    "localPA=0x%lx — completing held upgrade\n",
                    _nodeId, reqId, kv.first);
            _epRnfCtrl->completeHeldUpgrade(kv.first);
            return;
        }
    }
}

// ---- v4-dual-socket: sendHomeWritebackNotify ----

void
EPBackend::sendHomeWritebackNotify(uint64_t homePa, int homeSocket)
{
    DPRINTF(RubyEP, "[EP-HOME-WB-NOTIFY] node=%d pa=0x%lx homeSocket=%d\n",
            _nodeId, homePa, homeSocket);

    // Determine homeNode from PA
    int homeNode = _addrMap.homeNode(_nodeId, homePa);
    if (homeNode < 0) {
        // Try cross-node
        homeNode = homeNodeCrossNode(homePa);
    }
    if (homeNode < 0) {
        warn("EPBackend node_id=%d: sendHomeWritebackNotify: cannot determine homeNode for PA=0x%lx\n",
             _nodeId, homePa);
        return;
    }

    // Use the adapter for homeSocket (or fall back to slot 0)
    UBAdapter *adapter = getUBAdapter(homeSocket);
    if (!adapter) {
        adapter = getUBAdapter(0);
    }
    if (!adapter) {
        warn("EPBackend node_id=%d: sendHomeWritebackNotify: no adapter for PA=0x%lx\n",
             _nodeId, homePa);
        return;
    }

    // Get current epoch from UBCC (for stale check at UBCC)
    uint64_t epochVal = 0;
    {
        uint64_t qEpoch = 0;
        int qOwnerNode = -1;
        bool qFound = false;
        UBAdapter *na = getUBAdapter(homeSocket);
        if (!na) na = getUBAdapter(0);
        if (na) {
            int qRet = na->sendQueryLineMetaReq(homePa, homeNode, homeSocket,
                                                qEpoch, qOwnerNode, qFound);
            if (qRet == -2) {
                DPRINTF(RubyEP,
                             "[EP-HWB-QLM-PENDING] node=%d pa=0x%lx home=%d socket=%d\n",
                             _nodeId, homePa, homeNode, homeSocket);
            }
            if (qFound) epochVal = qEpoch;
        }
    }

    adapter->sendHomeWritebackNotify(homePa, epochVal, homeNode, homeSocket);
}

// ---- v4-dual-socket: handleQueryLineMetaResp ----

void
EPBackend::handleQueryLineMetaResp(const CoherenceMessage &msg)
{
    // Called when UBAdapter receives a QueryLineMetaResp from the router.
    // Phase 2 async: the response is also stored in _readyResponses keyed by
    // reqId, so EPSNFController can look it up in processPendingWritebacks()
    // without blocking.
    if (_verboseLog) {
    DPRINTF(RubyEP, "[EP-QLM-RESP] node=%d pa=0x%lx found=%d epoch=%lu ownerNode=%d reqId=%lu\n",
            _nodeId, msg.h.homeLinePa,
            msg.b.queryLineMetaResp.found,
            msg.b.queryLineMetaResp.epoch,
            msg.b.queryLineMetaResp.ownerNode,
            msg.h.reqId);
    }
    // If found=false or owner/epoch inconsistency, do NOT silently discard
    // dirty data — the caller (processPendingWritebacks) will fabricate a
    // best-effort epoch rather than dropping the line. See handleWriteback.
}

} // namespace ruby
} // namespace gem5
