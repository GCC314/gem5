#include "mem/ruby/protocol/chi/ep/EPBackend.hh"

#include <cstdio>
#include <cstring>
#include <cstdarg>
#include <array>
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

void
appendTmpLog(const char *file, const char *fmt, ...)
{
    char path[256];
    std::snprintf(path, sizeof(path), "/workspace/tmp_logs/%s", file);
    FILE *fp = std::fopen(path, "a");
    if (!fp) {
        return;
    }

    va_list ap;
    va_start(ap, fmt);
    std::vfprintf(fp, fmt, ap);
    va_end(ap);
    std::fclose(fp);
}

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
    _pageAllocCursor(p.metadata_private_base + (p.metadata_private_size / 2)),
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

    if (socketId == 0 && !_ubAdapters.empty() && _ubAdapters[0]) {
        std::fprintf(stderr, "[WIRE] node=%d wiring adapter->snf callback\n", _nodeId);
        _ubAdapters[0]->setOnResponseWired([this]{
            std::fprintf(stderr, "[RSP-FIRE] node=%d scheduling EPSNF wakeup\n", _nodeId);
            if (!_epSnfs.empty() && _epSnfs[0])
                _epSnfs[0]->scheduleEvent(Cycles(1));
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

uint64_t
EPBackend::metadataBackstorePa(uint64_t homePa) const
{
    if (_metadataPrivateSize < 64) {
        return _metadataPrivateBase;
    }
    const uint64_t slot_count = _metadataPrivateSize / 64;
    const uint64_t line_idx = (homePa >> 6) % slot_count;
    return _metadataPrivateBase + line_idx * 64;
}

uint64_t
EPBackend::allocatePagePa()
{
    const uint64_t end = _metadataPrivateBase + _metadataPrivateSize;
    if (_pageAllocCursor + BackstorePageSize > end)
        return 0;

    uint64_t pa = _pageAllocCursor;
    _pageAllocCursor += BackstorePageSize;
    return pa;
}

EPBackend::MetaLine
EPBackend::encodeMetaLine(uint64_t homePa, int state,
                          uint64_t sharersMask, uint64_t epoch)
{
    MetaLine line{};
    line[0] = 1; // valid
    memcpy(line.data() + 8, &homePa, sizeof(homePa));
    int64_t s = state;
    memcpy(line.data() + 16, &s, sizeof(s));
    memcpy(line.data() + 24, &sharersMask, sizeof(sharersMask));
    memcpy(line.data() + 32, &epoch, sizeof(epoch));
    return line;
}

bool
EPBackend::decodeMetaLine(uint64_t expectedHomePa, const MetaLine &line,
                          MetaStoreDecoded &entry)
{
    if (line[0] == 0) {
        return false;
    }

    uint64_t key = 0;
    memcpy(&key, line.data() + 8, sizeof(key));
    if (key != expectedHomePa) {
        return false;
    }

    int64_t st = 0;
    memcpy(&st, line.data() + 16, sizeof(st));
    memcpy(&entry.sharersMask, line.data() + 24, sizeof(entry.sharersMask));
    memcpy(&entry.epoch, line.data() + 32, sizeof(entry.epoch));
    entry.state = static_cast<int>(st);

    if (entry.state < static_cast<int>(UBCCMESIState::G_I) ||
        entry.state > static_cast<int>(UBCCMESIState::G_M)) {
        return false;
    }
    if (entry.state == static_cast<int>(UBCCMESIState::G_I) &&
        entry.sharersMask != 0) {
        return false;
    }
    if (entry.state == static_cast<int>(UBCCMESIState::G_S) &&
        entry.sharersMask == 0) {
        return false;
    }
    if (entry.state == static_cast<int>(UBCCMESIState::G_E) ||
        entry.state == static_cast<int>(UBCCMESIState::G_M)) {
        if (__builtin_popcountll(entry.sharersMask) != 1) {
            return false;
        }
    }
    return true;
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
            adapter->setOnResponseWired([snf]{
                std::fprintf(stderr, "[RSP-FIRE] scheduling EPSNF wakeup\n");
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
    // If we already obtained the outer grant for this line and are only waiting
    // for the ClearResp to confirm it, do NOT retry the whole miss. Re-issuing a
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
                                     pgt->second.baseEpoch, pgt->second.reqId);
            if (clearRet == -2)
                return -2;   // ClearResp not here yet; keep waiting (same reqId)
            // ClearResp accepted: sendClear() has consumed the txn. Finish up.
            OuterGrantType g = pgt->second.grantType;
            _pendingGrantTxns.erase(pgt);
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
        reqIdVal = makeRequesterReqId(_nodeId, _epochCounter);
        entry.lineAddr = line_pa;
        entry.state = RequesterLineState::R_WAIT_GRANT;
        entry.pendingReq = reqType;
        entry.epoch = _epochCounter;
        entry.reqId = reqIdVal;    // v4: store reqId
        entry.writeIntent = writeIntent;
        entry.homeNode = homeNode;
    }
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
        &dataSource, &authEpoch,
        &pendingInvCount, &pendingInvMask, &committedEpoch,
        &routedGrantData, &routedGrantDataValid);

    // Port async path: -2 means response pending, callers will retry
    if (grantInt == -2) return -2;

    grantTypeVar = static_cast<OuterGrantType>(grantInt);

    printf("[TC5-CLEAR-TRACE] handleRemoteMiss node=%d localPA=0x%lx homePA=0x%lx "
           "grantTypeVar=%d reqId=%lu entryEpoch=%lu authEpoch=%lu recallNeeded=%d owner=%d\n",
           _nodeId, line_pa, homePa, static_cast<int>(grantTypeVar), reqIdVal,
           entry.epoch, authEpoch, recallNeeded, recallOwnerNode);
    printf("[RECALL-OUTPUT] EPBackend n=%d PA=0x%lx recallNeeded=%d recallOwnerNode=%d\n",
           _nodeId, line_pa, recallNeeded, recallOwnerNode);

    // ---- M6: Handle recall path ----
    // If the home UBCC signals that a recall is needed, we must
    // route the recall through the owner node's EPBackend
    // (not bypass it with a direct processRecallResponse call).
    if (recallNeeded && recallOwnerNode >= 0) {
        printf("[RECALL-ROUTE] EPBackend node=%d PA=0x%lx ownerNode=%d\n",
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
        txn.grantType = grantEnv.grantType;
        _pendingGrantTxns[homePa] = txn;
        printf("[TC5-CLEAR-TRACE] savePendingGrantTxn node=%d keyPA=0x%lx homePA=0x%lx "
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

    if (dataSource == GrantDataSource::RecallBuffer) {
        setRecallCaptureData(routedGrantData, routedGrantDataValid);
    }

    // v4: Populate grant data using formal F3 data source
    populateGrantData(homePa, dataSource);

    int clearRet = sendClear(homePa, homeNode, grantEnv.epoch, grantEnv.reqId);
    if (clearRet == -2) return -2;

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

bool
EPBackend::handleRecallRequest(const OuterRecallMsg &recallMsg)
{
    printf("[RECALL-ENTRY] EPBackend node=%d PA=0x%lx ownerNode=%d homeNode=%d\n",
           _nodeId, recallMsg.linePa, recallMsg.ownerNode, recallMsg.homeNode);
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
        // R2: Clear stale recall capture data before initiating new recall
        setRecallCaptureData(DataBlock(64), false);
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
                // R2: Gate data payload on dataReturned (not raw _recallCaptureDataValid)
                if (resp.dataReturned) {
                    resp.dataPayload = _recallCaptureDataBlock;
                    resp.hasDataPayload = true;
                }
                sendRecallResponse(resp);
            });
    } else {
        // Write recall: ReadUnique with RecallUnique proxy op
        printf("[RECALL-DIAG] node=%d initiating ReadUnique recall PA=0x%lx\n",
               _nodeId, recallMsg.linePa);
        // R2: Clear stale recall capture data before initiating new recall
        setRecallCaptureData(DataBlock(64), false);
        _epRnfCtrl->startReadUnique(ownerLocalPa,
            [this, capturedMsg](bool success) {
                printf("[RECALL-DIAG] node=%d ReadUnique callback success=%d\n",
                       _nodeId, success);
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
    printf("[RECALL-RESP] node=%d PA=0x%lx homeNode=%d dataReturned=%d\n",
           _nodeId, response.linePa, response.homeNode, response.dataReturned);
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: sendRecallResponse "
            "PA=0x%lx homeNode=%d dataReturned=%d hasData=%d\n",
            _nodeId, response.linePa, response.homeNode,
            response.dataReturned, response.hasDataPayload);

    // Store for inspection
    _lastRecallResponse = response;
    _recallResponseSentCount++;

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
    if (response.dataReturned && response.hasDataPayload) {
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

// ---- M7: Writeback / Evict ----

void
EPBackend::handleHomeWritebackComplete(uint64_t homePa)
{
    printf("[EP-HOME-WB] node=%d pa=0x%lx\n", _nodeId, homePa);
    // v4-dual-socket: Send HomeWritebackNotify through adapter instead of
    // For single-socket backward compat, homeSocket = 0.
    int homeSocket = _addrMap.homeSocket(_nodeId, homePa);
    if (homeSocket < 0) homeSocket = 0;
    sendHomeWritebackNotify(homePa, homeSocket);
}

int
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
    // v4-dual-socket: derive homeSocket from PA (always 0 for single-socket)
    int homeSocket = _addrMap.homeSocket(_nodeId, line_pa);
    if (homeSocket < 0) homeSocket = 0;
    uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode, offset, homeSocket);

    // Look up requester entry to get epoch
    // For home-local writebacks (called from EPSNF), fall back to UBCC
    // directory which holds the authoritative epoch and owner.
    // when _requesterLines miss and multiple sockets are active.
    uint64_t epochVal = 0;
    int requesterNode = _nodeId;
    auto it = _requesterLines.find(line_pa);
    if (it != _requesterLines.end()) {
        epochVal = it->second.epoch;
    } else {
        // Query home UBCC via message for epoch + owner
        uint64_t qEpoch = 0;
        int qOwnerNode = -1;
        bool qFound = false;
        UBAdapter *wa = getUBAdapter(0);
        if (wa) {
            int qRet = wa->sendQueryLineMetaReq(line_pa, homeNode, homeSocket,
                                                qEpoch, qOwnerNode, qFound);
            if (qRet == -2) {
                std::fprintf(stderr,
                             "[EP-QLM-PENDING] node=%d pa=0x%lx home=%d socket=%d\n",
                             _nodeId, line_pa, homeNode, homeSocket);
                return -2;  // pending — caller must retry
            }
        }
        if (qFound) {
            epochVal = qEpoch;
            if (qOwnerNode >= 0) requesterNode = qOwnerNode;
        }
        if (epochVal == 0) {
            _epochCounter++;
            epochVal = _epochCounter;
        }
    }

    printf("[EP-HANDLE-WB] node=%d pa=0x%lx epoch=%lu requester=%d keepAsClean=%d\n",
           _nodeId, line_pa, epochVal, requesterNode, keepAsClean);

    // Build writeback message envelope
    _lastWritebackMsg.linePa = homePa;
    _lastWritebackMsg.requesterNode = requesterNode;
    _lastWritebackMsg.homeNode = homeNode;
    _lastWritebackMsg.epoch = epochVal;
    _lastWritebackMsg.keepAsClean = keepAsClean;

    if (!getUBAdapter(0)) {
        fatal("EPBackend node_id=%d: UBAdapter required for writeback "
              "PA=0x%lx homeNode=%d\n",
              _nodeId, line_pa, homeNode);
    }
    int wbRet = getUBAdapter(0)->sendWritebackReq(
        homePa, requesterNode, epochVal, keepAsClean, homeNode, homeSocket);
    bool wbPending = (wbRet == -2);
    bool ok = (wbRet > 0);
    if (wbPending) {
        std::fprintf(stderr,
                     "[EP-WB-PENDING] node=%d pa=0x%lx home=%d epoch=%lu\n",
                     _nodeId, homePa, homeNode, epochVal);
    }

    // Build ack envelope
    _lastAckMsg.linePa = homePa;
    _lastAckMsg.homeNode = homeNode;
    _lastAckMsg.epoch = epochVal;
    _lastAckMsg.success = ok || wbPending;

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
        std::fprintf(stderr,
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
                                    int desiredPerm, UpgradeCause cause,
                                    uint64_t &outEpoch, uint64_t &outReqId,
                                    bool *outRejected, bool *outNotSharer)
{
    if (outRejected) *outRejected = false;
    if (outNotSharer) *outNotSharer = false;
    DPRINTF(RubyEP,
            "EPBackend node_id=%d: notifyLocalWriteUpgrade "
            "PA=0x%lx homeNode=%d desiredPerm=%d\n",
            _nodeId, line_pa, homeNode, desiredPerm);

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

    if (!getUBAdapter(0)) {
        fatal("EPBackend node_id=%d: UBAdapter required for upgrade "
              "PA=0x%lx homeNode=%d\n",
              _nodeId, line_pa, homeNode);
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

    uint64_t upgradeTargetMask = 0;
    uint64_t committedEpoch = 0;
    int upgradeRet = getUBAdapter(0)->sendUpgradeReq(
        homePa, _nodeId, epochVal, reqIdVal,
        desiredPerm, static_cast<int>(cause),
        &upgradeTargetMask, &committedEpoch, homeNode, homeSocket,
        hadPending /*checkOnly: don't re-send an in-flight upgrade*/);
    if (upgradeRet == -2) {
        std::fprintf(stderr,
                     "[EP-UPGRADE-PENDING] node=%d pa=0x%lx home=%d epoch=%lu reqId=%lu\n",
                     _nodeId, homePa, homeNode, epochVal, reqIdVal);
        // Save the pending reqId/epoch so the next snoop retry reuses them and
        // hits the cached UpgradeResp, rather than allocating a new reqId that
        // the home rejects (existing outstanding).
        PendingUpgradeTxn txn;
        txn.valid = true;
        txn.linePa = line_pa;
        txn.homeNode = homeNode;
        txn.epoch = epochVal;
        txn.reqId = reqIdVal;
        _pendingUpgradeTxns[line_pa] = txn;
        return false;
    }
    // UpgradeResp arrived: clear any pending txn for this line.
    if (put != _pendingUpgradeTxns.end() && put->second.valid)
        put->second.valid = false;
    bool accepted = (upgradeRet > 0);

    // upgradeRet == 0 means the home explicitly rejected this upgrade (an
    // upgrade for this line is already outstanding for another requester).
    // The caller must NOT keep holding the snoop in that case — it has to fall
    // back to a plain SnpResp_I, give up its copy, and retry the upgrade later.
    // (Two requesters upgrading the same line otherwise deadlock: each holds
    // its SnpResp_I waiting for its own upgrade, so neither can be invalidated
    // for the other's upgrade. TC16/25/53 double-upgrade race.)
    if (!accepted && outRejected)
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
            // Other sharers exist — must invalidate them before Ack(true)
            printf("[UPGRADE-DIAG] node=%d upgrade accepted PENDING PA=0x%lx "
                   "targetMask=0x%lx — fanning out invalidations\n",
                   _nodeId, line_pa, upgradeTargetMask);

            // Fanout invalidations to each target sharer
            // Reuse existing EPBackend invalidation routing path
            uint64_t homeEpoch = committedEpoch;
            uint64_t offset = _addrMap.dsmOffset(line_pa);

            uint64_t remainingMask = upgradeTargetMask;
            for (int s = 0; s < 64 && remainingMask != 0; s++) {
                uint64_t sBit = (1ULL << s);
                if (remainingMask & sBit) {
                    remainingMask &= ~sBit;

                    OuterInvalidateMsg invMsg;
                    invMsg.linePa = homePa;
                    invMsg.sharerLocalPa = _addrMap.buildDsmPA(
                        s, homeNode, offset, homeSocket);
                    invMsg.sharerNode = s;
                    invMsg.homeNode = homeNode;
                    invMsg.epoch = homeEpoch;
                    invMsg.reqId = reqIdVal;

                    _lastInvalidateMsg = invMsg;

                    DPRINTF(RubyEP,
                            "EPBackend node_id=%d: upgrade fanout "
                            "invalidation to node %d via UBAdapter\n", _nodeId, s);
                    getUBAdapter(0)->sendInvalidateReqToSharer(s, invMsg, homeSocket);
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
    int homeSocket = _addrMap.homeSocket(_nodeId, line_pa);
    if (homeSocket < 0) homeSocket = 0;
    uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode, offset, homeSocket);

    if (!getUBAdapter(0)) {
        fatal("EPBackend node_id=%d: UBAdapter required for upgrade done "
              "PA=0x%lx homeNode=%d\n",
              _nodeId, line_pa, homeNode);
    }

    OuterUpgradeDone doneMsg;
    doneMsg.linePa = homePa;
    doneMsg.srcNode = _nodeId;
    doneMsg.homeNode = homeNode;
    doneMsg.epoch = epoch;
    doneMsg.reqId = reqId;
    _lastUpgradeDone = doneMsg;

    int doneRet = getUBAdapter(0)->sendUpgradeDoneReq(
        homePa, _nodeId, epoch, reqId, homeNode, homeSocket);
    bool donePending = (doneRet == -2);
    bool accepted = (doneRet > 0);
    if (donePending) {
        std::fprintf(stderr,
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
                      uint64_t epoch, uint64_t reqId)
{
    std::fprintf(stderr,
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

    printf("[TC5-CLEAR-TRACE] sendClear node=%d linePA=0x%lx homeNode=%d "
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

    if (!getUBAdapter(0)) {
        fatal("EPBackend node_id=%d: UBAdapter required for clear "
              "PA=0x%lx homeNode=%d\n",
              _nodeId, line_pa, homeNode);
    }
    // line_pa here is the home PA (socket-encoded); derive its home socket so
    // the ClearReq routes to the home plane's ubio (matching the original
    // grant). Hardcoding 0 sent cross-socket clears to the wrong plane, so the
    // grant handshake never completed and the requester deadlocked.
    int clearHomeSocket = _addrMap.homeSocket(homeNode, line_pa);
    if (clearHomeSocket < 0) clearHomeSocket = 0;
    int clearRet = getUBAdapter(0)->sendClearReq(
        line_pa, _nodeId, clearEpoch, reqId, homeNode, clearHomeSocket);
    bool accepted = (clearRet > 0);  // -2=pending, -1=error, 0=rejected, 1=accepted

    // Consume the pending grant txn only once the clear is actually accepted,
    // so retries while it is still pending (clearRet==-2) keep matching reqId.
    if (accepted && txnIt != _pendingGrantTxns.end() && txnIt->second.valid) {
        txnIt->second.valid = false;
    }

    printf("[TC5-CLEAR-TRACE] sendClearResult node=%d linePA=0x%lx homeNode=%d "
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

    return (clearRet == -2) ? -2 : (accepted ? 1 : 0);
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
EPBackend::clearCachedUpgradeResp(uint64_t linePa)
{
    // Clear any rejected UpgradeResp from the UBAdapter's ready-response cache
    // so the next upgrade attempt sends a fresh UpgradeReq instead of hitting
    // the stale rejected response.
    if (getUBAdapter(0))
        getUBAdapter(0)->clearReadyResponsesForLine(linePa);
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
    printf("[EP-HOME-WB-NOTIFY] node=%d pa=0x%lx homeSocket=%d\n",
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
                std::fprintf(stderr,
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
    // The response is already stored in _lastResponse by UBAdapter::recvFromRouter.
    // This is a notification hook for future async query support.
    printf("[EP-QLM-RESP] node=%d pa=0x%lx found=%d epoch=%lu ownerNode=%d\n",
           _nodeId, msg.h.homeLinePa,
           msg.b.queryLineMetaResp.found,
           msg.b.queryLineMetaResp.epoch,
           msg.b.queryLineMetaResp.ownerNode);
}

} // namespace ruby
} // namespace gem5
