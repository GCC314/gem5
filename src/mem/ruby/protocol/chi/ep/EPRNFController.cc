#include "mem/ruby/protocol/chi/ep/EPRNFController.hh"

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "debug/RubyEP.hh"
#include "mem/ruby/network/Network.hh"
#include "mem/ruby/protocol/CHI/CHIProtocolInfo.hh"
#include "mem/ruby/protocol/MemoryMsg.hh"
#include "mem/ruby/system/RubySystem.hh"
#include "mem/ruby/system/Sequencer.hh"
#include "params/EPRNFController.hh"

namespace gem5
{

namespace ruby
{

using namespace CHI;

// ---- SimObject param → static locals (set by EPRNFController::init) ----
// Phase 0.2: SimObject params take priority; env vars act as fallback
// when the param is left at default 0 (sentinel).
static uint64_t s_compack_retry = 0;       // from _params.compack_retry_cycles
static uint64_t s_wakeup_retry = 0;        // from _params.wakeup_retry_cycles
static uint64_t s_upgrade_retry_min = 0;   // from _params.upgrade_retry_min_cycles
static uint64_t s_upgrade_retry_max = 0;   // from _params.upgrade_retry_max_cycles
// DROP/NO-RESP recovery: cap the number of watchdog-driven OuterUpgradeReq
// resends for a single held upgrade. Bounds a livelock storm on a persistently
// faulty link while still tolerating several transient drops (TC111 drops one).

static uint64_t eprn_compack_retry() {
    return s_compack_retry;
}

static uint64_t eprn_wakeup_retry() {
    return s_wakeup_retry;
}

// ---- Exponential backoff for held-upgrade retries (§11) ----
// When a held SnpCleanInvalid upgrade is TEMP-REJECTED by the home (another
// global op is in progress), instead of always waiting 500µs (eprn_wakeup_retry),
// use a short exponential backoff.  The leading global op takes ~2.1 µs to
// commit (recall+grant+clear, measured from TC98 logs), so a minimum retry of
// 5µs already covers the common case with safety margin.  On repeated failures
// (rare in measured data: all 34% STALE-then-retry cases succeeded on first
// attempt), the interval doubles up to a cap of 200µs.
//
// Sequence: 5 → 10 → 20 → 40 → 80 → 160 → 200 → 200 … µs
// (@2 GHz: 1 cy = 500 ticks, so multiply by 2 to get Cycles).
static uint64_t ep_upgrade_retry_backoff_cycles(int retryCount)
{
    uint64_t minCycles = s_upgrade_retry_min;
    uint64_t maxCycles = s_upgrade_retry_max;
    uint64_t base = minCycles;
    for (int i = 0; i < retryCount && base < maxCycles; i++)
        base <<= 1;                    // double each retry
    return (base < maxCycles) ? base : maxCycles;
}

EPController::EPController(const Params &p)
  : AbstractController(p),
    reqOut(p.reqOut), snpOut(p.snpOut),
    rspOut(p.rspOut), datOut(p.datOut),
    reqIn(p.reqIn), snpIn(p.snpIn),
    rspIn(p.rspIn), datIn(p.datIn),
    cacheLineSize(p.ruby_system->getBlockSizeBytes()),
    cacheLineBits(floorLog2(cacheLineSize)),
    dataChannelSize(p.data_channel_size),
    dataMsgsPerLine(std::max(1, cacheLineSize / p.data_channel_size)),
    _nodeId(p.node_id),
    _snoopCount(0)
{
    m_machineID.type = MachineType_Cache;
    m_machineID.num = m_version;
    p.ruby_system->registerAbstractController(
        this, std::make_unique<CHIProtocolInfo>());
    p.ruby_system->m_num_controllers[m_machineID.type]++;
    m_ruby_system = p.ruby_system;
}

void
EPController::initNetQueues()
{
    int base = m_ruby_system->MachineType_base_number(m_machineID.type);

    panic_if(m_net_ptr == nullptr,
             "EPController node_id=%d: network pointer is null", _nodeId);

    m_net_ptr->setToNetQueue(m_version + base, reqOut->getOrdered(),
                             CHI_REQ, "none", reqOut);
    m_net_ptr->setToNetQueue(m_version + base, snpOut->getOrdered(),
                             CHI_SNP, "none", snpOut);
    m_net_ptr->setToNetQueue(m_version + base, rspOut->getOrdered(),
                             CHI_RSP, "none", rspOut);
    m_net_ptr->setToNetQueue(m_version + base, datOut->getOrdered(),
                             CHI_DAT, "response", datOut);

    m_net_ptr->setFromNetQueue(m_version + base, reqIn->getOrdered(),
                               CHI_REQ, "none", reqIn);
    m_net_ptr->setFromNetQueue(m_version + base, snpIn->getOrdered(),
                               CHI_SNP, "none", snpIn);
    m_net_ptr->setFromNetQueue(m_version + base, rspIn->getOrdered(),
                               CHI_RSP, "none", rspIn);
    m_net_ptr->setFromNetQueue(m_version + base, datIn->getOrdered(),
                               CHI_DAT, "response", datIn);
}

void
EPController::init()
{
    AbstractController::init();

    rspIn->setConsumer(this);
    datIn->setConsumer(this);
    snpIn->setConsumer(this);
    reqIn->setConsumer(this);

    resetStats();
}

void
EPController::addSequencer(RubyPort *seq)
{
    panic_if(seq == nullptr,
             "EPController node_id=%d: cannot add null sequencer", _nodeId);
    sequencers.emplace_back(seq);
}

void
EPController::print(std::ostream& out) const
{
    out << "[EPController node_id=" << _nodeId << " v=" << m_version << "]";
}

void
EPController::regStats()
{
    AbstractController::regStats();
}

void
EPController::collateStats()
{
}

void
EPController::resetStats()
{
    AbstractController::resetStats();
}

void
EPController::wakeup()
{
    bool pending = false;

    DPRINTF(RubyCHIGeneric, "EP node_id=%d: wakeup checking messages\n", _nodeId);
    pending = pending || receiveAllRdyMessages<CHIResponseMsg>(rspIn,
         [this](const CHIResponseMsg* msg){ return recvResponseMsg(msg); });

    pending = pending || receiveAllRdyMessages<CHIDataMsg>(datIn,
         [this](const CHIDataMsg* msg){ return recvDataMsg(msg); });

    pending = pending || receiveAllRdyMessages<CHIRequestMsg>(snpIn,
         [this](const CHIRequestMsg* msg){ return recvSnoopMsg(msg); });

    pending = pending || receiveAllRdyMessages<CHIRequestMsg>(reqIn,
         [this](const CHIRequestMsg* msg){ return recvRequestMsg(msg); });

    if (pending) {
        scheduleEvent(Cycles(1));
    }
}

void
EPController::recordCacheTrace(int cntrl, CacheRecorder* tr)
{
    panic("EPController doesn't implement recordCacheTrace");
}

AccessPermission
EPController::getAccessPermission(const Addr& param_addr)
{
    return AccessPermission_NotPresent;
}

void
EPController::functionalRead(
    const Addr& param_addr, Packet* param_pkt, WriteMask& param_mask)
{
    // No-op: EP_RNF's getAccessPermission returns NotPresent,
    // so it should never be asked for semantic data.  However,
    // RubySystem::partialFunctionalRead calls functionalRead on
    // ALL controllers (including NotPresent ones as "ctrl_others"),
    // so we must not panic here.
    DPRINTF(RubyCHIGeneric,
            "EPController node_id=%d: functionalRead called on "
            "addr=0x%lx (NotPresent, no-op)\n",
            _nodeId, param_addr);
}

int
EPController::functionalWrite(
    const Addr& param_addr, Packet* param_pkt)
{
    DPRINTF(RubyCHIGeneric,
            "EPController node_id=%d: functionalWrite called on "
            "addr=0x%lx (no-op)\n",
            _nodeId, param_addr);
    return 0;
}

int
EPController::functionalWriteBuffers(PacketPtr& pkt)
{
    int num_functional_writes = 0;
    num_functional_writes += reqOut->functionalWrite(pkt);
    num_functional_writes += snpOut->functionalWrite(pkt);
    num_functional_writes += rspOut->functionalWrite(pkt);
    num_functional_writes += datOut->functionalWrite(pkt);
    num_functional_writes += reqIn->functionalWrite(pkt);
    num_functional_writes += snpIn->functionalWrite(pkt);
    num_functional_writes += rspIn->functionalWrite(pkt);
    num_functional_writes += datIn->functionalWrite(pkt);
    return num_functional_writes;
}

bool
EPController::functionalReadBuffers(PacketPtr& pkt)
{
    if (reqOut->functionalRead(pkt)) return true;
    if (snpOut->functionalRead(pkt)) return true;
    if (rspOut->functionalRead(pkt)) return true;
    if (datOut->functionalRead(pkt)) return true;
    if (reqIn->functionalRead(pkt)) return true;
    if (snpIn->functionalRead(pkt)) return true;
    if (rspIn->functionalRead(pkt)) return true;
    if (datIn->functionalRead(pkt)) return true;
    return false;
}

bool
EPController::functionalReadBuffers(PacketPtr& pkt, WriteMask &mask)
{
    bool read = false;
    if (reqOut->functionalRead(pkt, mask)) read = true;
    if (snpOut->functionalRead(pkt, mask)) read = true;
    if (rspOut->functionalRead(pkt, mask)) read = true;
    if (datOut->functionalRead(pkt, mask)) read = true;
    if (reqIn->functionalRead(pkt, mask)) read = true;
    if (snpIn->functionalRead(pkt, mask)) read = true;
    if (rspIn->functionalRead(pkt, mask)) read = true;
    if (datIn->functionalRead(pkt, mask)) read = true;
    return read;
}

EPRNFController::EPRNFController(const Params &p)
  : EPController(p), _backend(p.ep_backend),
    _upgradeRetryMaxResends(p.upgrade_retry_max_resends),
    _numCacheControllers(0),
    _numSockets(p.downstream_destinations.size()),
    _addrMap(p.num_nodes, _numSockets, 128ULL * 1024 * 1024),
    _lastChiRequestSendTick(MaxTick),
    _pendingHnResponseCount(0),
    _delayedResolvedCount(0)
{
    // v4-dual-socket: array-ify HN-F versions from downstream destinations (§3.4)
    _hnfVersions.resize(_numSockets, -1);
    _downstreamBySocket.resize(_numSockets);
    for (int s = 0; s < _numSockets; ++s) {
        if (p.downstream_destinations[s]) {
            _hnfVersions[s] = p.downstream_destinations[s]->getVersion();
            _downstreamBySocket[s] = MachineID{MachineType_Cache, _hnfVersions[s]};
        }
    }

    // Register EP_RNF with EPBackend for delayed response support
    if (_backend) {
        _backend->setEpRnfController(this);
    }
}

void
EPRNFController::selfTest()
{
    DPRINTF(RubyCHIGeneric, "EP_RNF node_id=%d selfTest begin\n", _nodeId);

    auto test_req = std::make_shared<CHIRequestMsg>(
        curTick(), cacheLineSize, m_ruby_system);
    test_req->m_addr = 0x10000000;
    test_req->m_type = CHIRequestType_SnpShared;
    test_req->m_requestor = m_machineID;

    auto *snp_buf = snpIn;
    if (snp_buf->areNSlotsAvailable(1, curTick())) {
        snp_buf->enqueue(test_req, curTick(), cyclesToTicks(Cycles(1)),
                         false, false);
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d selfTest: injected SnpShared\n", _nodeId);
    }

    DPRINTF(RubyCHIGeneric, "EP_RNF node_id=%d selfTest end\n", _nodeId);
}

void
EPRNFController::init()
{
    EPController::init();
    fatal_if(!_backend, "EP_RNF node_id=%d: no backend attached", _nodeId);

    // Phase 1: Store SimObject params into file-local statics.
    // Params always take effect (no env fallback).
    s_compack_retry = params().compack_retry_cycles;
    s_wakeup_retry = params().wakeup_retry_cycles;
    s_upgrade_retry_min = params().upgrade_retry_min_cycles;
    s_upgrade_retry_max = params().upgrade_retry_max_cycles;
    fatal_if(_upgradeRetryMaxResends == 0,
             "EP_RNF node_id=%d: upgrade_retry_max_resends must be positive",
             _nodeId);

    // v4-dual-socket: strict completeness check (§3.4 change 2)
    // num_sockets > 1 且 downstream_destinations.size() != num_sockets -> fatal
    // 任一 _hnfVersions[s] < 0 -> fatal
    for (int s = 0; s < _numSockets; ++s) {
        fatal_if(_hnfVersions[s] < 0,
                 "EP_RNF node_id=%d: missing HN-F for socket %d "
                 "(_hnfVersions[%d]=%d < 0)", _nodeId, s, s, _hnfVersions[s]);
    }
    fatal_if(_numSockets > 1 && (int)_hnfVersions.size() != _numSockets,
             "EP_RNF node_id=%d: num_sockets=%d but only %zu HN-F versions",
             _nodeId, _numSockets, _hnfVersions.size());

    // Compute count of other Cache-type controllers for reference
    _numCacheControllers = m_ruby_system->m_num_controllers[MachineType_Cache];

    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: init done, cacheControllers=%d, "
            "numSockets=%d, hnfVersions=[",
            _nodeId, _numCacheControllers, _numSockets);
    for (int s = 0; s < _numSockets; ++s) {
        DPRINTFR(RubyCHIGeneric, "%s%d", (s ? "," : ""), _hnfVersions[s]);
    }
    DPRINTFR(RubyCHIGeneric, "]\n");

    // F4: selfTest disabled — manual snoop injection causes init-phase
    // SnpShared→EP-RNF fatal before any workload runs (§gap_analysis).
    // selfTest();
}

void
EPRNFController::wakeup()
{
    EPController::wakeup();

    processPendingResponseSends();
    processPendingDataSends();

    // Q3: Process deferred CHI requests (cleanup + safety net)
    processDeferredChiReqs();

    if (_backend)
        _backend->wakeup();

    // Process delayed upgrade retries (rejected upgrades waiting for home to drain).
    processUpgradeRetries();
    processUpgradeDoneRetries();
}

void
EPRNFController::print(std::ostream& out) const
{
    out << "[EP_RNF node_id=" << _nodeId << " v=" << m_version << "]";
}

bool
EPRNFController::recvRequestMsg(const CHIRequestMsg *msg)
{
    DPRINTF(RubyCHIGeneric, "EP_RNF node_id=%d recvRequestMsg type=%s addr=0x%lx\n",
            _nodeId, msg->m_type, msg->m_addr);
    if (_backend)
        _backend->checkAddr(msg->m_addr);
    return true;
}

bool
EPRNFController::recvSnoopMsg(const CHIRequestMsg *msg)
{
    DPRINTF(RubyCHIGeneric, "EP_RNF node_id=%d recvSnoopMsg type=%d addr=0x%lx "
            "retToSrc=%d\n",
            _nodeId, static_cast<int>(msg->m_type), msg->m_addr,
            msg->m_retToSrc);

    // M4: Increment snoop counter for test verification
    _snoopCount++;
    if (_backend) {
        _backend->incrementEpRnfSnoopCount();
        _backend->checkAddr(msg->m_addr);
    }

    // ---- Per-PA conflict arbitration (§5.1 / §10.3) ----
    // Replaces the old blind-queue logic.  When EP-RNF has an in-flight
    // CHI transaction for this PA, incoming snoops are arbitrated based
    // on the conflict matrix (§9.5):
    //   - Benign self-snoops (recall-induced) → IMMED clean SnpResp_I
    //   - Conflicting write-intent snoops → STALE SnpResp_I (abort-retry)
    //   - SnpShared/SnpSharedFwd → fatal (must not target EP-RNF)
    auto txnIt = _pendingChiTxns.find(msg->m_addr);
    bool inflight = (txnIt != _pendingChiTxns.end());
    bool hasRecall = _backend && _backend->hasActiveRecall(msg->m_addr);

    // ---- Fast path: no in-flight txn, no active recall ----
    if (!inflight && !hasRecall) {
        return processSnoopImmediate(msg);
    }

    // ---- §10.2: Benign self-snoop (recall-induced) → IMMED clean ----
    // Must be checked BEFORE STALE arbitration, otherwise the recall's own
    // post-RecallResponse cleanup snoop would be aborted, causing liveness bug.
    if (hasRecall) {
        DPRINTF(RubyEP, "[RECALL-SNOOP] node=%d PA=0x%lx "
               "recall-induced snoop — immediate clean SnpResp_I\n",
               _nodeId, msg->m_addr);
        _backend->clearActiveRecall(msg->m_addr);
        return processSnoopImmediate(msg);
    }

    // ---- From here: inflight=true, hasRecall=false ----
    // §3: HN-F excludes the requestor (EP-RNF itself) from snoop targets
    // for write-class transactions (CleanUnique/ReadUnique).  So an incoming
    // snoop during a write-class in-flight txn is always a CONFLICT, not a
    // self-snoop.  For recall ops (ReadShared), the snoop is from another
    // transaction (CHI serialisation) → also conflict under §9.5.

    // 1-entry snoop slot guard: non-fatal (HN-F single-flight makes this
    // unreachable in normal operation, but warn instead of fatal).
    if (txnIt->second.snoopSlotValid) {
        warn("EP_RNF node_id=%d: second snoop for PA=0x%lx while "
             "snoop slot already occupied — protocol may be violated; "
             "falling through to arbitration\n",
             _nodeId, msg->m_addr);
    }

    MachineID hnfDest = msg->m_requestor;
    uint64_t linePa = msg->m_addr;
    PendingChiOp inFlightOp = txnIt->second.op;

    // SnpOnce + ReadShared(NoProxyOp) in-flight → read/read coexistence
    // (§9.5 table row 3, col 3): IMMED SnpRespData_SC, not STALE.
    if (msg->m_type == CHIRequestType_SnpOnce &&
        inFlightOp == PendingChiOp::ReadShared) {
        DPRINTF(RubyEP, "[SNOOP-IMMED-SnpOnce+ReadShared] node=%d PA=0x%lx "
               "— read/read coexistence, immediate SnpRespData_SC\n",
               _nodeId, linePa);
        return processSnoopImmediate(msg);
    }

    // ---- Conflict arbitration: stale-retry (§9.5 matrix) ----
    switch (msg->m_type) {
        case CHIRequestType_SnpCleanInvalid:
        case CHIRequestType_SnpUnique:
            // Write-intent snoop during any in-flight → STALE (Q2).
            DPRINTF(RubyEP, "[SNOOP-STALE] node=%d PA=0x%lx snoop=%d inFlightOp=%d "
                   "— sending stale SnpResp_I (abort-retry)\n",
                   _nodeId, linePa, static_cast<int>(msg->m_type),
                   static_cast<int>(inFlightOp));
            sendSnpRespI(linePa, hnfDest, /*staleMark=*/true);
            return true;
        case CHIRequestType_SnpOnce:
            // Q4-a: Conservative STALE for write-class in-flight.
            // TODO: SnpOnce under ReadUnique(RecallUnique) could be
            // optimised to IMMED snapshot (weak-order read), but for
            // now we unify on STALE for maximum safety.
            DPRINTF(RubyEP, "[SNOOP-STALE-SnpOnce] node=%d PA=0x%lx inFlightOp=%d "
                   "— conservative stale SnpResp_I\n",
                   _nodeId, linePa, static_cast<int>(inFlightOp));
            sendSnpRespI(linePa, hnfDest, /*staleMark=*/true);
            return true;
        case CHIRequestType_SnpShared:
        case CHIRequestType_SnpSharedFwd:
            // Preserving snoops must not target EP-RNF (retain fatal).
            fatal("EP_RNF node_id=%d: unexpected SnpShared/SnpSharedFwd "
                  "at PA=0x%lx (routing bug; preserving snoops must not "
                  "target EP-RNF)\n",
                  _nodeId, linePa);
        default:
            // Unknown snoop: conservative fallback STALE.
            warn("[SNOOP-STALE-UNKNOWN] node=%d PA=0x%lx snoop=%d "
                   "— conservative stale SnpResp_I\n",
                   _nodeId, linePa, static_cast<int>(msg->m_type));
            sendSnpRespI(linePa, hnfDest, /*staleMark=*/true);
            return true;
    }
}

bool
EPRNFController::recvResponseMsg(const CHIResponseMsg *msg)
{
    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d recvResponseMsg type=%s addr=0x%lx\n",
            _nodeId, CHIResponseType_to_string(msg->m_type), msg->m_addr);

    // F6-diagnostic: log RetryAck/PCrdGrant for CleanUnique debugging
    if (msg->m_type == CHIResponseType_RetryAck ||
        msg->m_type == CHIResponseType_PCrdGrant) {
        auto it = _pendingChiTxns.find(msg->m_addr);
        DPRINTF(RubyEP, "[EPRNF-RETRY-DIAG] node=%d type=%s PA=0x%lx pendingFound=%d\n",
               _nodeId, msg->m_type == CHIResponseType_RetryAck ? "RetryAck" : "PCrdGrant",
               msg->m_addr, it != _pendingChiTxns.end());
    }

    // CompAck from HN-F or other agents: ignore (not tracking req responses)
    if (msg->m_type == CHIResponseType_CompAck) {
        return true;
    }

    // ---- Comp_UC completion (CleanUnique + ReadUnique) ----
    // Comp_UC is the completion token for both CleanUnique and ReadUnique.
    // For CleanUnique: no data, just the token.
    // For ReadUnique: data arrives via CompData first, then Comp_UC finalizes.
    if (msg->m_type == CHIResponseType_Comp_UC ||
        msg->m_type == CHIResponseType_Comp_UC_NoData) {
        auto it = _pendingChiTxns.find(msg->m_addr);
        if (it != _pendingChiTxns.end() &&
            it->second.proxyOp == EpProxyOp_RecallUnique) {
            inform(
                         "[RECALL-PROXY-COMPUC] node=%d localPA=0x%lx "
                         "beats=%d/%d tick=%lu\n",
                         _nodeId, msg->m_addr, it->second.beatsReceived,
                         it->second.beatsExpected, curTick());
        }
        DPRINTF(RubyEP, "[COMPUC-DIAG] node=%d received Comp_UC PA=0x%lx found=%d needsCompAck=%d\n",
               _nodeId, msg->m_addr,
               it != _pendingChiTxns.end(),
               it != _pendingChiTxns.end() ? it->second.needsCompAck : -1);
        if (it != _pendingChiTxns.end() &&
            (it->second.op == PendingChiOp::CleanUnique ||
             it->second.op == PendingChiOp::ReadUnique)) {

            if (it->second.op == PendingChiOp::ReadUnique) {
                it->second.hnfDest = msg->m_responder;
                it->second.readUniqueCompUCSeen = true;
                if (msg->m_type == CHIResponseType_Comp_UC_NoData)
                    it->second.readUniqueDataComplete = true;
                tryCompleteReadUnique(msg->m_addr);
                return true;
            }

            // Use msg->m_responder: the HN-F that sent Comp_UC to us
            it->second.hnfDest = msg->m_responder;

            // Build CompAck and try to send
            NetDest destNet(m_ruby_system);
            destNet.add(msg->m_responder);
            auto ack = std::make_shared<CHIResponseMsg>(
                curTick(), cacheLineSize, m_ruby_system,
                msg->m_addr, CHIResponseType_CompAck,
                m_machineID, destNet,
                false, false, 0, 0, MessageSizeType_Control);

            it->second.needsCompAck = true;
            sendResponseReliable(ack, [this, linePa = msg->m_addr]() {
                auto pending = _pendingChiTxns.find(linePa);
                if (pending == _pendingChiTxns.end())
                    return;
                pending->second.needsCompAck = false;
                finishChiTxn(linePa, true);
            });

            return true;
        }

        warn(
                "EP_RNF node_id=%d: Comp_UC for PA=0x%lx but no pending "
                "CleanUnique/ReadUnique txn\n",
                _nodeId, msg->m_addr);
        return true;
    }

    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d recvResponseMsg unhandled type=%s\n",
            _nodeId, CHIResponseType_to_string(msg->m_type));
    return true;
}

bool
EPRNFController::recvDataMsg(const CHIDataMsg *msg)
{
    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d recvDataMsg addr=0x%lx type=%s\n",
            _nodeId, msg->m_addr, CHIDataType_to_string(msg->m_type));

    // ---- Accept CompData response types ----
    if (msg->m_type != CHIDataType_CompData_I &&
        msg->m_type != CHIDataType_CompData_SC &&
        msg->m_type != CHIDataType_CompData_UC &&
        msg->m_type != CHIDataType_CompData_UD_PD &&
        msg->m_type != CHIDataType_CompData_SD_PD) {
        return true;
    }

    auto it = _pendingChiTxns.find(msg->m_addr);
    if (it == _pendingChiTxns.end()) {
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: CompData for PA=0x%lx but no pending txn\n",
                _nodeId, msg->m_addr);
        return true;
    }

    // ---- ReadShared completion ----
    if (it->second.op == PendingChiOp::ReadShared) {
        it->second.hnfDest = msg->m_responder;
        it->second.beatsReceived++;
        fatal_if(it->second.recallDataMask.isOverlap(msg->m_bitMask),
                 "EP_RNF node_id=%d: duplicate ReadShared data bytes for %#x",
                 _nodeId, msg->m_addr);
        it->second.recallDataBlk.copyPartial(msg->getdataBlk(),
                                             msg->m_bitMask);
        it->second.recallDataMask.orMask(msg->m_bitMask);
        it->second.recallDataValid = it->second.recallDataMask.isFull();

        // Send CompAck only on last beat (HN-F expects exactly 1 per txn)
        if (it->second.beatsReceived >= it->second.beatsExpected) {
            NetDest destNet(m_ruby_system);
            destNet.add(msg->m_responder);
            auto ack = std::make_shared<CHIResponseMsg>(
                curTick(), cacheLineSize, m_ruby_system,
                msg->m_addr, CHIResponseType_CompAck,
                m_machineID, destNet,
                false, false, 0, 0, MessageSizeType_Control);
            it->second.needsCompAck = true;
            sendResponseReliable(ack, [this, linePa = msg->m_addr]() {
                auto pending = _pendingChiTxns.find(linePa);
                if (pending == _pendingChiTxns.end())
                    return;
                pending->second.needsCompAck = false;
                finishChiTxn(linePa, true);
            });
        }
        return true;
    }

    // ---- v4: ReadUnique data beat handling (§4.3.2, §5.3) ----
    // ReadUnique completion requires both all data beats and Comp_UC. The two
    // channels may arrive in either order under O3/network pressure.
    if (it->second.op == PendingChiOp::ReadUnique) {
        if (it->second.proxyOp == EpProxyOp_RecallUnique) {
            inform(
                         "[RECALL-PROXY-DATA] node=%d localPA=0x%lx "
                         "type=%d nextBeat=%d/%d tick=%lu\n",
                         _nodeId, msg->m_addr, static_cast<int>(msg->m_type),
                         it->second.beatsReceived + 1,
                         it->second.beatsExpected, curTick());
        }
        it->second.hnfDest = msg->m_responder;
        it->second.beatsReceived++;
        fatal_if(it->second.recallDataMask.isOverlap(msg->m_bitMask),
                 "EP_RNF node_id=%d: duplicate ReadUnique data bytes for %#x",
                 _nodeId, msg->m_addr);
        it->second.recallDataBlk.copyPartial(msg->getdataBlk(),
                                             msg->m_bitMask);
        it->second.recallDataMask.orMask(msg->m_bitMask);
        it->second.recallDataValid = it->second.recallDataMask.isFull();

        if (it->second.beatsReceived >= it->second.beatsExpected) {
            fatal_if(!it->second.recallDataValid,
                     "EP_RNF node_id=%d: incomplete ReadUnique data for %#x",
                     _nodeId, msg->m_addr);
            it->second.readUniqueDataComplete = true;
        }
        tryCompleteReadUnique(msg->m_addr);
        return true;
    }

    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d recvDataMsg unhandled addr=0x%lx type=%s\n",
            _nodeId, msg->m_addr, CHIDataType_to_string(msg->m_type));
    return true;
}

// ---- M6: Delayed HN Response Management ----

bool
EPRNFController::hasPendingHnResponse(uint64_t linePa) const
{
    auto it = _pendingHnResponses.find(linePa);
    return (it != _pendingHnResponses.end() && it->second.valid &&
            !it->second.outerTxnComplete);
}

void
EPRNFController::signalOuterTxnComplete(uint64_t linePa)
{
    auto it = _pendingHnResponses.find(linePa);
    if (it == _pendingHnResponses.end() || !it->second.valid) {
        // No pending response for this line
        return;
    }

    if (it->second.outerTxnComplete) {
        // Already resolved
        return;
    }

    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: M6 outer txn complete for PA=0x%lx "
            "-- sending delayed HN response\n",
            _nodeId, linePa);

    // Mark as complete
    it->second.outerTxnComplete = true;

    // Send the delayed HN response
    NetDest dest(m_ruby_system);
    dest.add(it->second.destMachine);
    auto rsp = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        linePa, it->second.respType,
        m_machineID, dest,
        false, false, 0, 0, MessageSizeType_Control);
    sendResponseReliable(rsp, [this, linePa]() {
        _delayedResolvedCount++;
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: M6 delayed HN response sent "
                "PA=0x%lx (resolved=%d)\n",
                _nodeId, linePa, _delayedResolvedCount);
    });
}

bool
EPRNFController::isOuterTxnPending(uint64_t linePa) const
{
    auto it = _outerTxnPending.find(linePa);
    return (it != _outerTxnPending.end() && it->second);
}

void
EPRNFController::setOuterTxnPending(uint64_t linePa, bool pending)
{
    if (pending) {
        _outerTxnPending[linePa] = true;
    } else {
        _outerTxnPending.erase(linePa);
    }
}

// ---- v4: Snoop Type Dispatch (§4.3.3) ----

bool
EPRNFController::processSnoopImmediate(const CHIRequestMsg *msg)
{
    switch (msg->m_type) {
        case CHIRequestType_SnpCleanInvalid:
            return handleSnpCleanInvalid(msg);
        case CHIRequestType_SnpUnique:
            return handleSnpUnique(msg);
        case CHIRequestType_SnpOnce:
            return handleSnpOnce(msg);
        case CHIRequestType_SnpShared:
        case CHIRequestType_SnpSharedFwd:
            // R3: Restore fatal — preserving snoops must not target EP-RNF.
            // Diagnostic warn+SnpResp_SC path hid routing bugs and could hang HN-F.
            fatal("EP_RNF node_id=%d: unexpected SnpShared/SnpSharedFwd "
                  "at PA=0x%lx (routing bug; preserving snoops must not "
                  "target EP-RNF)\n",
                  _nodeId, msg->m_addr);
        default:
            // Unknown snoop: fallback to SnpResp_I
            DPRINTF(RubyCHIGeneric,
                    "EP_RNF node_id=%d: unknown snoop type=%d PA=0x%lx "
                    "— sending SnpResp_I as fallback\n",
                    _nodeId,
                    static_cast<int>(msg->m_type), msg->m_addr);
            return sendSnpRespI(msg);
    }
}

bool
EPRNFController::handleSnpCleanInvalid(const CHIRequestMsg *msg)
{
    // §4.3.3: SnpCleanInvalid — two cases:
    //   1) Non-upgrade: immediate SnpResp_I
    //   2) Local upgrade (remote sharer upgrade): OuterUpgradeReq → wait
    //      for OuterUpgradeAck(true) → then SnpResp_I (§5.5)

    EPBackend *backend = EPBackend::getBackendInstance(_nodeId);
    bool isDsmLine = backend && backend->isDsmAddrCrossNode(msg->m_addr);
    const int sourceSocket = msg->m_ubcc_ingress_socket;
    fatal_if(sourceSocket < 0 || sourceSocket >= _numSockets,
             "EP_RNF node_id=%d: invalid requester socket %d for PA=0x%lx",
             _nodeId, sourceSocket, msg->m_addr);

    // Check if upgrade is pending for this PA (set by the first snoop arrival)
    auto upIt = _upgradePending.find(msg->m_addr);
    if (upIt != _upgradePending.end() && upIt->second.valid) {
        fatal_if(upIt->second.sourceSocket != sourceSocket,
                 "EP_RNF node_id=%d: held upgrade socket changed for PA=0x%lx "
                 "old=%d new=%d", _nodeId, msg->m_addr,
                 upIt->second.sourceSocket, sourceSocket);
        // ---- Upgrade path (§5.5 t2-t5) ----
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: SnpCleanInvalid upgrade path for "
                "PA=0x%lx — deferring SnpResp_I until OuterUpgradeAck\n",
                _nodeId, msg->m_addr);

        // Record HN-F destination for deferred SnpResp_I
        upIt->second.hnfDest = msg->m_requestor;

        // SnpResp_I will be sent when receiveUpgradeAck() is called
        return true;
    }

    // ---- Self-snoop guard: if we have a pending CHI transaction for this PA,
    //     the SnpCleanInvalid is from our own RECALL-induced ReadUnique/ReadShared.
    //     Just respond SnpResp_I immediately; the RECALL handles ownership transfer.
    if (isDsmLine) {
        auto chiIt = _pendingChiTxns.find(msg->m_addr);
        if (chiIt != _pendingChiTxns.end()) {
            DPRINTF(RubyEP, "[SELF-SNOOP] node=%d SnpCleanInvalid PA=0x%lx "
                   "pendingChiTxn op=%d — immediate SnpResp_I\n",
                   _nodeId, msg->m_addr,
                   static_cast<int>(chiIt->second.op));
            return sendSnpRespI(msg);
        }
    }

    // ---- RECALL snoop guard: if EPBackend has an active recall for this PA,
    //     the SnpCleanInvalid is RECALL-induced (TC98 §6.1) — the UBCC RECALL
    //     handles ownership transfer, so no OuterUpgradeReq is needed.
    if (isDsmLine && backend && backend->hasActiveRecall(msg->m_addr)) {
        DPRINTF(RubyEP, "[RECALL-SNOOP] node=%d SnpCleanInvalid PA=0x%lx "
               "during active recall — immediate SnpResp_I\n",
               _nodeId, msg->m_addr);
        backend->clearActiveRecall(msg->m_addr);
        return sendSnpRespI(msg);
    }

    if (isDsmLine) {
        // §5.2 Silent Upgrade: when the local requester holds R_E (clean
        // exclusive) or R_M (dirty modified), guaranteed sole owner by
        // directory one-hot invariant, the write upgrade can complete
        // locally with zero cross-node messages — no OuterUpgradeReq,
        // no hold, no epoch increment on the home.  This is the cross-node
        // analogue of MESI's E→M (or M→M store) silent upgrade.
        if (backend && backend->hasRequesterExclusive(msg->m_addr)) {
            bool silent = backend->silentUpgradeEnabled();
            if (silent) {
                DPRINTF(RubyCHIGeneric,
                        "EP_RNF node_id=%d: SnpCleanInvalid PA=0x%lx "
                        "silent upgrade (R_E/R_M→M local, 0 cross-node msgs)\n",
                        _nodeId, msg->m_addr);
                DPRINTF(RubyEP, "[UPGRADE-DIAG] node=%d silent upgrade PA=0x%lx "
                       "(R_E/R_M→M, zero cross-node messages)\n",
                       _nodeId, msg->m_addr);
                return sendSnpRespI(msg);
            }
        }

        int homeNode = backend->homeNodeCrossNode(msg->m_addr);
        uint64_t epoch = 0;
        uint64_t reqId = 0;

        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: SnpCleanInvalid first-arrival upgrade path "
                "for PA=0x%lx home=%d — issuing OuterUpgradeReq\n",
                _nodeId, msg->m_addr, homeNode);
        DPRINTF(RubyEP, "[UPGRADE-DIAG] node=%d first SnpCleanInvalid PA=0x%lx home=%d\n",
               _nodeId, msg->m_addr, homeNode);

        // CHI §4.3.3 / §5.5: a snoop whose upgrade is not yet complete must be
        // *held* (accepted, response deferred), NOT NACKed. Establish the
        // _upgradePending record BEFORE issuing the OuterUpgradeReq so that:
        //   (1) any redelivery of this snoop hits the held-path above
        //       (line 667-679: return true, response stays deferred) instead of
        //       being treated as a brand-new first-arrival — which previously
        //       caused HN-F to re-issue the snoop every tick (busy-wait
        //       livelock, TC16/25/53), each retry allocating a fresh reqId.
        //   (2) the later notifyUpgradeAckReady() -> receiveUpgradeAck()
        //       callback finds its context instead of losing it.
        UpgradePending pending;
        pending.valid = true;
        pending.linePa = msg->m_addr;
        pending.homeNode = homeNode;
        pending.sourceSocket = sourceSocket;
        pending.epoch = 0;
        pending.reqId = 0;
        pending.hnfDest = msg->m_requestor;
        _upgradePending[msg->m_addr] = pending;

        // Try to complete the upgrade now. If the OuterUpgradeResp has not yet
        // arrived (async pending), completeHeldUpgrade() leaves the record valid
        // and we hold the snoop (return true, SnpResp_I deferred). The held
        // upgrade is later driven to completion event-wise by
        // completeHeldUpgrade() when UpgradeResp arrives (EPBackend
        // ::onUpgradeRespArrived) or by notifyUpgradeAckReady() when all
        // invalidation acks arrive — NOT by re-issuing the snoop every tick.
        completeHeldUpgrade(msg->m_addr);
        return true;
    }

    // ---- Non-upgrade: immediate SnpResp_I ----
    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: SnpCleanInvalid non-upgrade for PA=0x%lx "
            "— immediate SnpResp_I\n",
            _nodeId, msg->m_addr);
    warn("EP_RNF node_id=%d: SnpCleanInvalid PA=0x%lx arrived without "
         "upgradePending context; local upgrade path is disconnected\n",
         _nodeId, msg->m_addr);
    return sendSnpRespI(msg);
}

bool
EPRNFController::handleSnpUnique(const CHIRequestMsg *msg)
{
    // §4.3.3: SnpUnique → globalInvalidate, return SnpResp_I / SnpRespData_I(_PD)
    // §4.6.3: response depends on retToSrc, hasData, isDirty
    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: SnpUnique for PA=0x%lx retToSrc=%d\n",
            _nodeId, msg->m_addr, msg->m_retToSrc);

    // EP-RNF as sole sharer: respond with invalidation.
    // If retToSrc: needs to return data if dirty (SnpRespData_I_PD) or
    //   clean (SnpRespData_I).
    // If !retToSrc: SnpResp_I (no data return).
    //
    // EP-RNF is not a real caching agent; it has no dirty data.
    // Per §4.6.3: retToSrc && hasData && !isDirty → SnpRespData_I
    //             retToSrc && !hasData → SnpResp_I
    //             !retToSrc → SnpResp_I
    //
    // Since EP-RNF has no data of its own, always use SnpResp_I.
    // Data collection for writeback is handled by the callback path.

    if (msg->m_retToSrc) {
        // Need to return data. EP-RNF doesn't hold data itself;
        // HN-F handles data collection from the actual owner.
        // We return SnpRespData_I to indicate successful invalidation
        // without dirty data (PD=pass dirty=false).
        NetDest dest(m_ruby_system);
        dest.add(msg->m_requestor);
        auto rsp = std::make_shared<CHIResponseMsg>(
            curTick(), cacheLineSize, m_ruby_system,
            msg->m_addr, CHIResponseType_SnpResp_I,
            m_machineID, dest,
            false, false, 0, 0, MessageSizeType_Control);
        sendResponseReliable(rsp);
    } else {
        sendSnpRespI(msg);
    }
    return true;
}

bool
EPRNFController::handleSnpOnce(const CHIRequestMsg *msg)
{
    // §4.3.3: SnpOnce → remoteFetch, return SnpRespData_SC
    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: SnpOnce for PA=0x%lx — "
            "sending SnpRespData_SC\n",
            _nodeId, msg->m_addr);
    return sendSnpRespDataSC(msg);
}

bool
EPRNFController::sendSnpRespI(const CHIRequestMsg *msg)
{
    NetDest dest(m_ruby_system);
    dest.add(msg->m_requestor);
    auto rsp = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        msg->m_addr, CHIResponseType_SnpResp_I,
        m_machineID, dest,
        false, false, 0, 0, MessageSizeType_Control);
    sendResponseReliable(rsp);
    return true;
}

bool
EPRNFController::sendSnpRespSC(const CHIRequestMsg *msg)
{
    // F1: SnpResp_SC is only used for legitimate non-Fwd snoop responses
    // (e.g. SnpOnce → SnpRespData_SC).  Not a defensive mask for
    // preserving/Fwd snoops — those are now fatal.
    NetDest dest(m_ruby_system);
    dest.add(msg->m_requestor);
    auto rsp = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        msg->m_addr, CHIResponseType_SnpResp_SC,
        m_machineID, dest,
        false, false, 0, 0, MessageSizeType_Control);
    sendResponseReliable(rsp);
    return true;
}

bool
EPRNFController::sendSnpRespDataSC(const CHIRequestMsg *msg)
{
    // Send_SnpOnce expects SnpRespData_SC only. A separate SnpResp_SC can
    // arrive after the data channel and violate HN-F's completed expectation.
    NetDest dest(m_ruby_system);
    dest.add(msg->m_requestor);

    // Data: SnpRespData_SC (zero data — EP-RNF has no cached data)
    for (int i = 0; i < dataMsgsPerLine; i++) {
        int offset = i * dataChannelSize;
        int chunkSize = (i == dataMsgsPerLine - 1) ?
            (cacheLineSize - offset) : dataChannelSize;
        WriteMask wm(cacheLineSize);
        wm.setMask(offset, chunkSize);
        DataBlock db(cacheLineSize);  // zero-filled
        auto dat = std::make_shared<CHIDataMsg>(
            curTick(), cacheLineSize, m_ruby_system,
            msg->m_addr, CHIDataType_SnpRespData_SC,
            m_machineID, dest, db, wm,
            false, 0, false, MessageSizeType_Data);
        sendDataReliable(dat);
    }
    return true;
}

// ---- v4: Queued Snoop Processing (§4.3.3) ----

void
EPRNFController::processQueuedSnoop(uint64_t linePa)
{
    auto txnIt = _pendingChiTxns.find(linePa);
    if (txnIt == _pendingChiTxns.end() || !txnIt->second.snoopSlotValid) {
        return;  // No queued snoop
    }

    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: processing queued snoop type=%d for "
            "PA=0x%lx\n",
            _nodeId,
            static_cast<int>(txnIt->second.queuedSnoopType),
            linePa);

    // Build a synthetic CHIRequestMsg for the queued snoop
    auto synthMsg = std::make_shared<CHIRequestMsg>(
        curTick(), cacheLineSize, m_ruby_system);
    synthMsg->m_addr = linePa;
    synthMsg->m_type = txnIt->second.queuedSnoopType;
    synthMsg->m_retToSrc = txnIt->second.queuedRetToSrc;
    synthMsg->m_requestor = txnIt->second.hnfDest;

    // Clear the snoop slot before processing
    txnIt->second.snoopSlotValid = false;
    txnIt->second.queuedSnoopType = CHIRequestType_null;
    txnIt->second.queuedRetToSrc = false;

    // Process the queued snoop (no in-flight CHI txn at this point)
    processSnoopImmediate(synthMsg.get());
}

// ---- v4: Transaction Completion Helper ----

void
EPRNFController::finishChiTxn(uint64_t linePa, bool success)
{
    DPRINTF(RubyEP, "[EPRNF-FINISH] node=%d PA=0x%lx success=%d\n",
            _nodeId, linePa, success);
    auto txnIt = _pendingChiTxns.find(linePa);
    if (txnIt == _pendingChiTxns.end()) {
        return;
    }

    auto cb = txnIt->second.onComplete;
    bool hadQueuedSnoop = txnIt->second.snoopSlotValid;
    // Keep completion-side clearing for ReadShared only.
    // ReadUnique is retired by RECALL-SNOOP hit or by requester re-acquire
    // (EPBackend::handleGrant) to avoid stale-marker pollution (TC42).
    bool isReadSharedRecall =
        (txnIt->second.op == PendingChiOp::ReadShared);

    // F2: Transfer recall capture data to EPBackend before erasing txn,
    // so that the callback (which runs after erase) can access it.
    if (txnIt->second.recallDataValid && _backend) {
        _backend->setRecallCaptureData(
            txnIt->second.recallDataBlk, true);
    } else if (_backend) {
        _backend->setRecallCaptureData(
            DataBlock(cacheLineSize), false);  // invalidate previous capture
    }

    // Erase completed transaction
    _pendingChiTxns.erase(txnIt);

    // Invoke completion callback
    if (cb) {
        cb(success);
    }

    // §4.3.3: Queued snoop has higher priority than deferred CHI requests
    if (hadQueuedSnoop) {
        processQueuedSnoop(linePa);
    }

    // ReadShared recall keeps legacy completion-side clear for compatibility
    // with existing local-upgrade tests (TC8).
    if (isReadSharedRecall && _backend) {
        _backend->clearActiveRecall(linePa);
    }

    // Process retry queue entries (outbound CHI requests with strongest-op)
    processRetryQueue();

    // Process any remaining deferred CHI requests
    processDeferredChiReqs();
}

void
EPRNFController::sendResponseReliable(CHIResponseMsgPtr msg,
                                      std::function<void()> onSent)
{
    if (_pendingResponseSends.empty() && sendResponseMsg(msg)) {
        if (onSent)
            onSent();
        return;
    }

    _pendingResponseSends.push_back({std::move(msg), std::move(onSent)});
    scheduleEvent(Cycles(1));
}

void
EPRNFController::processPendingResponseSends()
{
    while (!_pendingResponseSends.empty()) {
        auto &pending = _pendingResponseSends.front();
        if (!sendResponseMsg(pending.msg))
            break;

        auto onSent = std::move(pending.onSent);
        _pendingResponseSends.pop_front();
        if (onSent)
            onSent();
    }

    if (!_pendingResponseSends.empty())
        scheduleEvent(Cycles(1));
}

void
EPRNFController::sendDataReliable(CHIDataMsgPtr msg)
{
    if (_pendingDataSends.empty() && sendDataMsg(msg))
        return;

    _pendingDataSends.push_back(std::move(msg));
    scheduleEvent(Cycles(1));
}

void
EPRNFController::processPendingDataSends()
{
    while (!_pendingDataSends.empty()) {
        if (!sendDataMsg(_pendingDataSends.front()))
            break;
        _pendingDataSends.pop_front();
    }

    if (!_pendingDataSends.empty())
        scheduleEvent(Cycles(1));
}

void
EPRNFController::tryCompleteReadUnique(uint64_t linePa)
{
    auto it = _pendingChiTxns.find(linePa);
    if (it == _pendingChiTxns.end() ||
        it->second.op != PendingChiOp::ReadUnique ||
        !it->second.readUniqueDataComplete ||
        !it->second.readUniqueCompUCSeen || it->second.needsCompAck) {
        return;
    }

    NetDest destNet(m_ruby_system);
    destNet.add(it->second.hnfDest);
    auto ack = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        linePa, CHIResponseType_CompAck,
        m_machineID, destNet,
        false, false, 0, 0, MessageSizeType_Control);

    it->second.needsCompAck = true;
    sendResponseReliable(ack, [this, linePa]() {
        auto pending = _pendingChiTxns.find(linePa);
        if (pending == _pendingChiTxns.end())
            return;
        pending->second.needsCompAck = false;
        finishChiTxn(linePa, true);
    });
}

// ---- Q3: CHI Request to HN-F ----
bool
EPRNFController::sendChiRequest(uint64_t linePa, CHIRequestType reqType,
                                EpProxyOp proxyOp)
{
    // Q3: Serialize CHI requests to prevent TBE reservation exhaustion
    // in the HN-F.  When the HN-F processes multiple requests in the same
    // event-processing cycle, allocateRequestTBE can call decrementReserved
    // twice for a single incrementReserved, triggering assertion failure.
    if (_lastChiRequestSendTick == curTick()) {
        // Defer additional sends from the same event-processing cycle.
        DPRINTF(RubyEP, "[EPRNF-DEFER] node=%d PA=0x%lx type=%d — queued\n",
                _nodeId, linePa, static_cast<int>(reqType));
        DeferredChiRequest d;
        d.linePa = linePa;
        d.reqType = reqType;
        d.proxyOp = proxyOp;
        d.startTick = curTick();
        _deferredChiReqs.push_back(d);
        scheduleEvent(Cycles(1));
        return true;  // Report success to caller (will be sent later)
    }

    // A prior request may be waiting for reqOut capacity even though no CHI
    // transaction has reached HN-F yet. Preserve FIFO ordering in that case.
    if (!_deferredChiReqs.empty()) {
        DeferredChiRequest d;
        d.linePa = linePa;
        d.reqType = reqType;
        d.proxyOp = proxyOp;
        d.startTick = curTick();
        _deferredChiReqs.push_back(d);
        scheduleEvent(Cycles(1));
        return true;
    }

    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: sendChiRequest addr=0x%lx type=%d "
            "proxyOp=%d\n",
            _nodeId, linePa, static_cast<int>(reqType),
            static_cast<int>(proxyOp));

    // Create CHI request message
    auto req = std::make_shared<CHIRequestMsg>(
        curTick(), cacheLineSize, m_ruby_system);
    req->m_addr = linePa;
    req->m_type = reqType;
    req->m_requestor = m_machineID;
    req->m_allowRetry = true;
    req->m_MessageSize = MessageSizeType_Control;
    // v4: Set ep_proxy_op sideband for special completion (§4.5.4)
    req->m_ep_proxy_op = proxyOp;

    // v4-dual-socket: select HN-F destination by PA.homeSocket (§3.4 change 3)
    int homeSocket = decodeHomeSocket(linePa);
    MachineID hnfId = _downstreamBySocket[homeSocket];
    req->m_Destination.clear();
    req->m_Destination.add(hnfId);

    // Send on reqOut → HN-F's reqIn
    bool sent = sendRequestMsg(req);
    if (sent) {
        _lastChiRequestSendTick = curTick();
    } else {
        warn("EP_RNF node_id=%d: sendChiRequest failed for addr=0x%lx "
             "(reqOut full)\n", _nodeId, linePa);
        DeferredChiRequest d;
        d.linePa = linePa;
        d.reqType = reqType;
        d.proxyOp = proxyOp;
        d.startTick = curTick();
        _deferredChiReqs.push_back(d);
        scheduleEvent(Cycles(1));
        return true;
    }

    DPRINTF(RubyCHIGeneric,
             "EP_RNF node_id=%d: sendChiRequest addr=0x%lx type=%d "
             "homeSocket=%d dest=(type=%d num=%d) sent=%d\n",
             _nodeId, linePa, static_cast<int>(reqType),
             homeSocket, hnfId.getType(), hnfId.getNum(), sent);

    return sent;
}

// ---- Q3: Process deferred CHI requests ----
void
EPRNFController::processDeferredChiReqs()
{
    while (!_deferredChiReqs.empty() &&
           _lastChiRequestSendTick != curTick()) {
        const DeferredChiRequest &d = _deferredChiReqs.front();
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: processing deferred CHI request "
                "addr=0x%lx type=%d proxyOp=%d (queued at tick=%lu)\n",
                _nodeId, d.linePa,
                static_cast<int>(d.reqType),
                static_cast<int>(d.proxyOp), d.startTick);
        auto req = std::make_shared<CHIRequestMsg>(
            curTick(), cacheLineSize, m_ruby_system);
        req->m_addr = d.linePa;
        req->m_type = d.reqType;
        req->m_requestor = m_machineID;
        req->m_allowRetry = true;
        req->m_MessageSize = MessageSizeType_Control;
        req->m_ep_proxy_op = d.proxyOp;
        req->m_Destination.clear();
        req->m_Destination.add(selectHnfDestination(d.linePa));

        if (!sendRequestMsg(req)) {
            scheduleEvent(Cycles(1));
            break;
        }

        _deferredChiReqs.pop_front();
        _lastChiRequestSendTick = curTick();
        if (!_deferredChiReqs.empty())
            scheduleEvent(Cycles(1));
    }
}

void
EPRNFController::startReadShared(uint64_t linePa,
                                 std::function<void(bool)> onComplete)
{
    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: startReadShared addr=0x%lx\n",
            _nodeId, linePa);

    if (_pendingChiTxns.find(linePa) != _pendingChiTxns.end()) {
        warn(
                "EP_RNF node_id=%d: startReadShared addr=0x%lx "
                "already has pending txn\n",
                _nodeId, linePa);
        if (onComplete) onComplete(false);
        return;
    }

    PendingChiTxn txn;
    txn.linePa = linePa;
    txn.epoch = 0;     // filled by caller via EPBackend
    txn.reqId = 0;
    txn.op = PendingChiOp::ReadShared;
    txn.proxyOp = EpProxyOp_NoProxyOp;  // ReadShared has no special completion
    txn.hnfDest = MachineID();
    txn.beatsExpected = dataMsgsPerLine;
    txn.beatsReceived = 0;
    txn.needsCompAck = false;
    txn.recallDataValid = false;
    txn.outerTxnPending = false;
    txn.readUniqueDataComplete = false;
    txn.readUniqueCompUCSeen = false;
    txn.startTick = curTick();
    txn.onComplete = onComplete;
    _pendingChiTxns[linePa] = txn;

    bool sent = sendChiRequest(linePa, CHIRequestType_ReadShared,
                               EpProxyOp_NoProxyOp);
    if (!sent) {
        _pendingChiTxns.erase(linePa);
        warn(
                "EP_RNF node_id=%d: startReadShared addr=0x%lx "
                "send failed\n", _nodeId, linePa);
        if (onComplete) onComplete(false);
    }
}

// ---- v4: Write Recall Path (§4.3.2, §5.3) ----
void
EPRNFController::startReadUnique(uint64_t linePa,
                                 std::function<void(bool)> onComplete)
{
    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: startReadUnique addr=0x%lx "
            "(write recall path)\n",
            _nodeId, linePa);

    auto pending = _pendingChiTxns.find(linePa);
    if (pending != _pendingChiTxns.end()) {
        if (pending->second.op == PendingChiOp::ReadUnique &&
            pending->second.proxyOp == EpProxyOp_RecallUnique) {
            // Home retries use the same recall tuple while the original CHI
            // proxy is still pending. Its completion owns the authoritative
            // response; reporting this duplicate as failure would fabricate a
            // no-data RecallResp for a dirty owner.
            inform(
                "[RECALL-PROXY-COALESCE] node=%d localPA=0x%lx "
                "proxy=RecallUnique\n",
                _nodeId, linePa);
            return;
        }
        warn(
                "EP_RNF node_id=%d: startReadUnique addr=0x%lx "
                "already has pending txn\n",
                _nodeId, linePa);
        if (onComplete) onComplete(false);
        return;
    }

    PendingChiTxn txn;
    txn.linePa = linePa;
    txn.epoch = 0;     // filled by caller via EPBackend
    txn.reqId = 0;
    txn.op = PendingChiOp::ReadUnique;
    txn.proxyOp = EpProxyOp_RecallUnique;  // §4.5.4: special completion scrub_to_I
    txn.hnfDest = MachineID();
    // ReadUnique returns CompData (data beats) + Comp_UC (completion token)
    txn.beatsExpected = dataMsgsPerLine;
    txn.beatsReceived = 0;
    txn.needsCompAck = false;
    txn.recallDataValid = false;
    txn.outerTxnPending = false;
    txn.readUniqueDataComplete = false;
    txn.readUniqueCompUCSeen = false;
    txn.startTick = curTick();
    txn.onComplete = onComplete;
    _pendingChiTxns[linePa] = txn;

    inform(
                 "[RECALL-PROXY-ISSUE] node=%d localPA=0x%lx proxy=RecallUnique "
                 "tick=%lu\n",
                 _nodeId, linePa, curTick());
    bool sent = sendChiRequest(linePa, CHIRequestType_ReadUnique,
                               EpProxyOp_RecallUnique);
    DPRINTF(RubyEP, "[EPRNF-RECALL] node=%d startReadUnique PA=0x%lx sent=%d\n",
            _nodeId, linePa, sent);
    if (!sent) {
        _pendingChiTxns.erase(linePa);
        warn(
                "EP_RNF node_id=%d: startReadUnique addr=0x%lx "
                "send failed\n", _nodeId, linePa);
        if (onComplete) onComplete(false);
    }
}

void
EPRNFController::startCleanUnique(uint64_t linePa,
                                  std::function<void(bool)> onComplete)
{
    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: startCleanUnique addr=0x%lx\n",
            _nodeId, linePa);

    // Check for duplicate pending transaction on this line
    if (_pendingChiTxns.find(linePa) != _pendingChiTxns.end()) {
        DPRINTF(RubyEP, "[CLEANUNIQUE-DIAG] node=%d PA=0x%lx DUPLICATE — already has pending txn op=%d\n",
                _nodeId, linePa, (int)_pendingChiTxns[linePa].op);
        warn(
                "EP_RNF node_id=%d: startCleanUnique addr=0x%lx "
                "already has pending txn\n",
                _nodeId, linePa);
        if (onComplete) onComplete(false);
        return;
    }

    // Create pending transaction entry
    PendingChiTxn txn;
    txn.linePa = linePa;
    txn.epoch = 0;     // filled by caller via EPBackend
    txn.reqId = 0;
    txn.op = PendingChiOp::CleanUnique;
    txn.proxyOp = EpProxyOp_InvalidateOnly;  // §4.5.4: special completion scrub_to_I
    txn.hnfDest = MachineID();
    // CleanUnique returns Comp_UC (completion token only, no data beats)
    txn.beatsExpected = 0;
    txn.beatsReceived = 0;
    txn.needsCompAck = false;  // hnfDest not yet known; set to true after Comp_UC sets hnfDest
    txn.recallDataValid = false;
    txn.outerTxnPending = false;
    txn.readUniqueDataComplete = false;
    txn.readUniqueCompUCSeen = false;
    txn.startTick = curTick();
    txn.onComplete = onComplete;
    _pendingChiTxns[linePa] = txn;

    // Send CleanUnique to HN-F via reqOut with InvalidateOnly proxy op
    bool sent = sendChiRequest(linePa, CHIRequestType_CleanUnique,
                               EpProxyOp_InvalidateOnly);
    DPRINTF(RubyEP, "[CLEANUNIQUE-DIAG] node=%d PA=0x%lx sendChiRequest sent=%d\n",
            _nodeId, linePa, sent);
    if (!sent) {
        // Send failed — clean up pending txn and notify caller
        _pendingChiTxns.erase(linePa);
        warn(
                "EP_RNF node_id=%d: startCleanUnique addr=0x%lx "
                "send failed, notifying caller\n",
                _nodeId, linePa);
        if (onComplete) onComplete(false);
    }
}

// ---- v4: getEpProxyOp Helper (§4.3.2) ----

EpProxyOp
EPRNFController::getEpProxyOp(PendingChiOp op)
{
    switch (op) {
        case PendingChiOp::ReadShared:
            return EpProxyOp_NoProxyOp;
        case PendingChiOp::CleanUnique:
            return EpProxyOp_InvalidateOnly;
        case PendingChiOp::ReadUnique:
            return EpProxyOp_RecallUnique;
        default:
            return EpProxyOp_NoProxyOp;
    }
}

// ---- v4: Retry Queue (§4.3.4) ----

void
EPRNFController::enqueueRetry(uint64_t linePa, uint64_t epoch, uint64_t reqId,
                              PendingChiOp op)
{
    auto it = _retryEntries.find(linePa);
    if (it != _retryEntries.end()) {
        // §4.3.4: stale epoch → discard
        if (epoch < it->second.epoch) {
            warn(
                    "EP_RNF node_id=%d: retry stale epoch %lu < %lu for "
                    "PA=0x%lx — discarding\n",
                    _nodeId, epoch, it->second.epoch, linePa);
            return;
        }

        // Same epoch: keep strongest op (ReadUnique > CleanUnique > ReadShared)
        if (epoch == it->second.epoch) {
            if (static_cast<int>(op) > static_cast<int>(it->second.strongestOp)) {
                it->second.strongestOp = op;
                it->second.reqId = reqId;
                DPRINTF(RubyCHIGeneric,
                        "EP_RNF node_id=%d: retry upgraded op for PA=0x%lx "
                        "epoch=%lu\n",
                        _nodeId, linePa, epoch);
            }
            return;
        }

        // Newer epoch: replace
        it->second.epoch = epoch;
        it->second.reqId = reqId;
        it->second.strongestOp = op;
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: retry newer epoch %lu for PA=0x%lx\n",
                _nodeId, epoch, linePa);
    } else {
        // New entry
        RetryEntry entry;
        entry.linePa = linePa;
        entry.epoch = epoch;
        entry.reqId = reqId;
        entry.strongestOp = op;
        _retryEntries[linePa] = entry;
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: retry enqueued for PA=0x%lx epoch=%lu "
                "op=%d\n",
                _nodeId, linePa, epoch, static_cast<int>(op));
    }
}

void
EPRNFController::processRetryQueue()
{
    if (_retryEntries.empty()) return;

    // Process one retry entry at a time (per-PA single-flight)
    // Entries with a CHI txn already in flight for this PA are skipped
    for (auto it = _retryEntries.begin(); it != _retryEntries.end(); ) {
        if (_pendingChiTxns.find(it->second.linePa) != _pendingChiTxns.end()) {
            // Already in flight for this PA — skip
            ++it;
            continue;
        }

        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: retry processing PA=0x%lx op=%d "
                "epoch=%lu\n",
                _nodeId, it->second.linePa,
                static_cast<int>(it->second.strongestOp),
                it->second.epoch);

        switch (it->second.strongestOp) {
            case PendingChiOp::ReadShared:
                startReadShared(it->second.linePa, nullptr);
                break;
            case PendingChiOp::CleanUnique:
                startCleanUnique(it->second.linePa, nullptr);
                break;
            case PendingChiOp::ReadUnique:
                startReadUnique(it->second.linePa, nullptr);
                break;
        }

        it = _retryEntries.erase(it);
        // Only process one to maintain single-flight
        break;
    }
}

// ---- v4: Upgrade Path (§5.5) ----

void
EPRNFController::sendSnpRespI(uint64_t linePa, MachineID hnfDest, bool staleMark)
{
    NetDest dest(m_ruby_system);
    dest.add(hnfDest);
    // The 5th ctor arg is CHIResponseMsg.stale. Only the abandon path sets it.
    auto rsp = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        linePa, CHIResponseType_SnpResp_I,
        m_machineID, dest,
        staleMark, false, 0, 0, MessageSizeType_Control);
    sendResponseReliable(rsp);
}

void
EPRNFController::scheduleUpgradeRetry(uint64_t linePa)
{
    _upgradeRetryLines.insert(linePa);
    scheduleEvent(Cycles(eprn_compack_retry()));
}

void
EPRNFController::scheduleUpgradeRetryAfterRejection(uint64_t linePa)
{
    // The rejected upgrade's InvalidateAck has been sent. Reset the rejected
    // state so completeHeldUpgrade will issue a FRESH OuterUpgradeReq (new
    // reqId/epoch) instead of hitting the stale rejected UpgradeResp.
    auto upIt = _upgradePending.find(linePa);
    if (upIt != _upgradePending.end() && upIt->second.valid) {
        upIt->second.rejected = false;
        upIt->second.needsRetry = true;
        upIt->second.retryCount++;        // exponential backoff (§11)
        // TEMP-REJECT starts a fresh tuple. Its no-response watchdog and
        // same-tuple resend budget must not inherit state from the old reqId.
        upIt->second.dropWatchdogArmed = false;
        upIt->second.dropResendCount = 0;
        upIt->second.retryExhausted = false;

    // Clear pending txn in EPBackend so notifyLocalWriteUpgrade allocates
    // a fresh reqId instead of reusing the rejected one (checkOnly).
    EPBackend *backend = EPBackend::getBackendInstance(_nodeId);
    if (backend) {
        backend->clearPendingUpgradeTxn(linePa);
        backend->clearCachedUpgradeResp(linePa, upIt->second.sourceSocket);
    }
    }
    // Schedule the retry using exponential backoff (§11).
    // The leading global op takes ~2.1 µs to drain, so the first retry at
    // 5 µs is generous.  On repeated failures the interval doubles up to a
    // cap of 200 µs.  This replaces the old fixed ~500 µs (eprn_wakeup_retry)
    // that was copied from the unrelated wakeup-retry path.
    _upgradeRetryLines.insert(linePa);
    const Cycles delay(ep_upgrade_retry_backoff_cycles(
        upIt != _upgradePending.end() ? upIt->second.retryCount : 0));
    if (upIt != _upgradePending.end())
        upIt->second.retryReadyTick = clockEdge(delay);
    scheduleEvent(delay);
}

void
EPRNFController::processUpgradeRetries()
{
    if (_upgradeRetryLines.empty())
        return;
    DPRINTF(RubyEP, "[RETRY-DIAG] node=%d processUpgradeRetries lines=%zu\n",
            _nodeId, _upgradeRetryLines.size());
    auto lines = _upgradeRetryLines;
    for (uint64_t linePa : lines) {
        auto upIt = _upgradePending.find(linePa);
        if (upIt == _upgradePending.end() || !upIt->second.valid) {
            _upgradeRetryLines.erase(linePa);
            continue;
        }
        if (upIt->second.ackReceived) {
            _upgradeRetryLines.erase(linePa);
            continue;
        }
        if (curTick() < upIt->second.retryReadyTick) {
            continue;
        }
        _upgradeRetryLines.erase(linePa);
        // Two kinds of scheduled retries share this queue:
        //
        //  (1) TEMP-REJECT retry (scheduleUpgradeRetryAfterRejection): the home
        //      explicitly rejected (existing outstanding). needsRetry is set and
        //      the pending txn was already cleared, so completeHeldUpgrade issues
        //      a FRESH OuterUpgradeReq (new reqId) after the backoff.
        //
        //  (2) DROP/NO-RESP watchdog (armed in the pending-hold branch): the
        //      upgrade is still pending and its OuterUpgradeReq/UpgradeResp may
        //      have been dropped. We must RETRANSMIT the SAME reqId (forceResend)
        //      — NOT a fresh one, which would make the home reject every
        //      retransmit as "existing outstanding" and livelock (TC3/8/10/11).
        //      If the original was truly dropped the home accepts the retransmit
        //      fresh; if it was already accepted the home idempotently returns
        //      the cached grant.
        const bool dropRecovery =
            upIt->second.dropWatchdogArmed && !upIt->second.needsRetry;

        if (dropRecovery) {
            // Disarm so the next pending-hold re-arms with a wider backoff
            // window if this retransmit is also dropped. Bound the number of
            // retransmits to avoid storming a persistently faulty link. Once
            // exhausted, fail-stop without polling, resending, or re-arming.
            bool doResend = (upIt->second.dropResendCount
                             < _upgradeRetryMaxResends);
            if (doResend) {
                upIt->second.dropWatchdogArmed = false;
                upIt->second.dropResendCount++;
                upIt->second.retryCount++;   // widen next watchdog window
                warn("[UPGRADE-DIAG] node=%d DROP-recovery resend #%u "
                       "PA=0x%lx (same reqId)\n",
                       _nodeId, upIt->second.dropResendCount, linePa);
            } else {
                upIt->second.dropWatchdogArmed = false;
                upIt->second.retryExhausted = true;
                fatal("[EPRNF-UPGRADE-TERMINAL] node=%d pa=0x%lx "
                      "sourceSocket=%d homeNode=%d epoch=%lu reqId=%lu "
                      "reason=EXHAUSTED_NO_RESPONSE resends=%u "
                      "homeAccepted=%d\n",
                      _nodeId, linePa, upIt->second.sourceSocket,
                      upIt->second.homeNode,
                      upIt->second.epoch, upIt->second.reqId,
                      upIt->second.dropResendCount,
                      upIt->second.homeAccepted ? 1 : 0);
            }
            DPRINTF(RubyCHIGeneric,
                    "EP_RNF node_id=%d: DROP-recovery retry held upgrade "
                    "PA=0x%lx\n", _nodeId, linePa);
            completeHeldUpgrade(linePa, doResend /*forceResend*/);
        } else {
            // TEMP-REJECT retry: fresh OuterUpgradeReq (checkOnly=false because
            // pending txn was cleared by scheduleUpgradeRetryAfterRejection).
            upIt->second.needsRetry = false;
            DPRINTF(RubyCHIGeneric,
                    "EP_RNF node_id=%d: retrying held upgrade PA=0x%lx\n",
                    _nodeId, linePa);
            DPRINTF(RubyEP, "[UPGRADE-DIAG] node=%d retry upgrade PA=0x%lx\n",
                    _nodeId, linePa);
            completeHeldUpgrade(linePa);
        }
    }
}

void
EPRNFController::completeHeldUpgrade(uint64_t linePa, bool dropRecoveryResend)
{
    // Drive a held SnpCleanInvalid-upgrade to completion. Called:
    //   (a) inline from handleSnpCleanInvalid first-arrival, and
    //   (b) event-wise from EPBackend::onUpgradeRespArrived() when the
    //       OuterUpgradeResp finally arrives (async Port path).
    // This replaces the previous busy-wait where each redelivered snoop
    // re-issued a fresh OuterUpgradeReq (livelock; TC16/25/53).
    auto upIt = _upgradePending.find(linePa);
    if (upIt == _upgradePending.end() || !upIt->second.valid) {
        // No held upgrade for this line (already completed or never started).
        return;
    }
    if (upIt->second.ackReceived) {
        // Already completed; nothing to do.
        return;
    }
    if (upIt->second.retryExhausted)
        return;

    EPBackend *backend = EPBackend::getBackendInstance(_nodeId);
    if (!backend) {
        warn("EP_RNF node_id=%d: completeHeldUpgrade PA=0x%lx but no backend\n",
             _nodeId, linePa);
        return;
    }

    int homeNode = upIt->second.homeNode;
    uint64_t epoch = 0;
    uint64_t reqId = 0;
    bool rejected = false;
    bool notSharer = false;
    bool homeDeferred = false;

    // Re-check the upgrade. With checkOnly semantics (an in-flight upgrade is
    // already pending in EPBackend::_pendingUpgradeTxns), this returns true once
    // the cached OuterUpgradeResp is available, false while still pending or
    // rejected. `rejected` distinguishes a hard reject from async-pending;
    // `notSharer` distinguishes a PERMANENT reject (we lost a dual-upgrade race
    // and were invalidated) from a TEMPORARY reject (another op is outstanding).
    bool accepted = backend->notifyLocalWriteUpgrade(
        linePa, homeNode, upIt->second.sourceSocket, 1,
        UpgradeCause::LocalCleanUnique,
        epoch, reqId, &rejected, &notSharer, &homeDeferred,
        dropRecoveryResend /*forceResend: retransmit same reqId on DROP*/);
    fatal_if(epoch == 0 || reqId == 0,
             "EP_RNF node_id=%d: upgrade lacks stable tuple PA=0x%lx "
             "sourceSocket=%d epoch=%lu reqId=%lu", _nodeId, linePa,
             upIt->second.sourceSocket, epoch, reqId);
    upIt->second.epoch = epoch;
    upIt->second.reqId = reqId;

    if (accepted) {
        upIt->second.homeAccepted = true;
    } else if (homeDeferred) {
        upIt->second.dropResendCount = 0;
        upIt->second.dropWatchdogArmed = false;
        inform("[UPGRADE-HOME-DEFERRED] node=%d pa=0x%lx reqId=%lu home=%d\n",
               _nodeId, linePa, reqId, homeNode);
    } else if (rejected && upIt->second.homeAccepted && !notSharer) {
        // Accepted is monotonic for the held tuple. A later temporary reject
        // can only be a delayed response to an earlier duplicate request; the
        // home is already committed to finishing this upgrade. Keep the same
        // reqId and continue AckNotify-loss recovery instead of entering the
        // fresh-transaction retry path.
        warn("[UPGRADE-DIAG] node=%d ignored stale reject after accept "
               "PA=0x%lx reqId=%lu\n",
               _nodeId, linePa, upIt->second.reqId);
        rejected = false;
    }

    // Decide between RETRY and ABANDON on a reject.
    //
    // ABANDON is required when continuing to hold+retry would deadlock. That
    // happens in two cases:
    //   (a) notSharer: the home permanently rejected us — we lost a dual-upgrade
    //       race and were removed from the sharersMask (retry is rejected
    //       forever). [TC16 steady state]
    //   (b) A winner's InvalidateReq for this line is already pending/deferred
    //       against our held snoop. That means another node's upgrade is waiting
    //       for OUR invalidation ack, while we are waiting for our own upgrade to
    //       be granted — a circular wait. The home cannot drain the winner until
    //       we let go, so our upgrade will be rejected (existing outstanding)
    //       forever. We must abandon to break the cycle. [TC16 race]
    // Otherwise (temporary reject, no winner waiting on us) RETRY: the home has
    // some unrelated op outstanding that will drain, after which the retried
    // upgrade succeeds and the held snoop completes normally. [TC53 storm]
    bool mustAbandon = notSharer || backend->hasDeferredInvalidation(linePa);

    if (!accepted && rejected && !mustAbandon) {
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: upgrade TEMP-REJECT PA=0x%lx — hold snoop, "
                "schedule retry (no winner waiting, still a sharer)\n",
                _nodeId, linePa);
        scheduleUpgradeRetryAfterRejection(linePa);
        return;
    }

    if (!accepted && rejected && mustAbandon) {
        // ABANDON — dual-upgrade race LOSER (TC16): retrying would deadlock
        // (circular wait) or be rejected forever (not a sharer). Our SC copy is
        // stale, so an upgrade (S->M) is invalid — the L2 must recall the
        // winner's data via a fresh ReadUnique (I->M). We do NOT send a plain
        // SnpResp_I (that would let the HN-F grant local exclusive without global
        // auth — split brain). Instead we release the held snoop with
        // SnpResp_I(stale=1) so the HN-F completes the CleanUnique as stale and
        // the L2 downgrades to ReadUnique.
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: upgrade ABANDON for PA=0x%lx (notSharer=%d "
                "deferredInval=%d) — stale SnpResp_I drives L2 ReadUnique\n",
                _nodeId, linePa, notSharer,
                backend->hasDeferredInvalidation(linePa));

        backend->clearPendingUpgradeTxn(linePa);
        backend->clearCachedUpgradeResp(linePa, upIt->second.sourceSocket);
        upIt->second.rejected = true;

        // Drive the stale-SnpResp_I abandon. There are two orderings:
        //  (1) InvalidateReq arrives AFTER the reject (TC16): it was deferred
        //      while the snoop was held; re-drive it now so handleInvalidation
        //      Request's rejected branch performs the abandon.
        //  (2) InvalidateReq arrived BEFORE our local CleanUnique/reject (TC53
        //      contention storm): the winner's fanout already ran (invalidating
        //      our copy via startCleanUnique) and no further InvalidateReq will
        //      come. Nothing would ever drive the abandon, so the held snoop —
        //      and thus the L2's re-issued CleanUnique — would deadlock. In that
        //      case we abandon PROACTIVELY here: release the held snoop with a
        //      stale SnpResp_I so the local HN-F completes the CleanUnique as
        //      stale and the L2 downgrades to ReadUnique.
        if (backend->hasDeferredInvalidation(linePa)) {
            backend->flushDeferredInvalidation(linePa);
        } else {
            // No pending/deferred InvalidateReq — proactively abandon.
            MachineID hnfDest = upIt->second.hnfDest;
            uint64_t hnfRaw = ((uint64_t)hnfDest.type << 24) | hnfDest.num;
            clearHeldUpgrade(linePa);
            backend->sendSnpRespIForRejected(linePa, hnfRaw);
        }
        return;
    }

    if (!accepted) {
        // Still pending — keep holding the snoop. In the fault-free case the
        // OuterUpgradeResp arrives shortly and drives completion event-wise via
        // EPBackend::onUpgradeRespArrived() -> completeHeldUpgrade(). TC3/8/10/11
        // rely on this normal async-pending hold.
        //
        // DROP/NO-RESP recovery (TC111): if the OuterUpgradeReq (or its
        // UpgradeResp) was dropped on the wire, onUpgradeRespArrived() will
        // NEVER fire and the snoop would stay held forever (deadlock). To
        // recover we arm a *watchdog* the FIRST time we observe a pending hold
        // for this line: reuse the exponential-backoff retry timer. When the
        // watchdog fires (processUpgradeRetries) it RETRANSMITS the SAME reqId
        // (forceResend) so a dropped request is recovered idempotently while a
        // merely-slow response is not disturbed. The watchdog period (>=5µs) is
        // far longer than the fault-free response latency, so in the common case
        // the response has already arrived and the "resend" idempotently returns
        // the cached grant. We do NOT set needsRetry here (that marks a
        // TEMP-REJECT fresh-reqId retry) and we do NOT give up the snoop (that is
        // only correct for an explicit reject).
        if (!upIt->second.dropWatchdogArmed &&
            !upIt->second.retryExhausted) {
            upIt->second.dropWatchdogArmed = true;
            _upgradeRetryLines.insert(linePa);
            const Cycles delay(
                ep_upgrade_retry_backoff_cycles(upIt->second.retryCount));
            upIt->second.retryReadyTick = clockEdge(delay);
            scheduleEvent(delay);
            inform(
                    "EP_RNF node_id=%d: completeHeldUpgrade PA=0x%lx pending "
                    "— holding snoop, armed DROP watchdog\n",
                    _nodeId, linePa);
        } else {
            DPRINTF(RubyCHIGeneric,
                    "EP_RNF node_id=%d: completeHeldUpgrade PA=0x%lx still "
                    "pending — holding snoop (watchdog already armed)\n",
                    _nodeId, linePa);
        }
        return;
    }

    // upgrade_invalidate_fix D2: only call receiveUpgradeAck() immediately if
    // the ack is ready (targetMask==0). Otherwise it is triggered later via
    // EPBackend::notifyUpgradeAckReady() when all invalidation acks arrive.
    if (backend->lastUpgradeAck().accepted) {
        // Fast path: no other sharers, immediate Ack(true)
        receiveUpgradeAck(linePa);
    } else {
        // Deferred: wait for all invalidation acks to arrive. Keep a watchdog
        // armed even after an accepted-pending UpgradeResp: AckNotify is a
        // separate asynchronous message and may be the message that was lost.
        if (!upIt->second.dropWatchdogArmed &&
            !upIt->second.retryExhausted) {
            upIt->second.dropWatchdogArmed = true;
            _upgradeRetryLines.insert(linePa);
            const Cycles delay(
                ep_upgrade_retry_backoff_cycles(upIt->second.retryCount));
            upIt->second.retryReadyTick = clockEdge(delay);
            scheduleEvent(delay);
        }
        DPRINTF(RubyEP, "[UPGRADE-DIAG] node=%d upgrade deferred ack PA=0x%lx "
               "— waiting for invalidation acks\n",
               _nodeId, linePa);
    }
}

void
EPRNFController::receiveUpgradeAck(uint64_t linePa)
{
    // Called by EPBackend when OuterUpgradeAck(true) is received.
    // Now safe to send deferred SnpResp_I to HN-F (§5.5 t5).

    auto upIt = _upgradePending.find(linePa);
    if (upIt == _upgradePending.end() || !upIt->second.valid) {
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: receiveUpgradeAck for PA=0x%lx but "
                "no upgrade pending\n",
                _nodeId, linePa);
        warn("EP_RNF node_id=%d: receiveUpgradeAck PA=0x%lx lost upgrade "
             "context before deferred SnpResp_I\n",
             _nodeId, linePa);
        return;
    }

    if (upIt->second.ackReceived) {
        // Already processed (e.g. both fast-path and notifyUpgradeAckReady
        // fired). Avoid sending a duplicate SnpResp_I / UpgradeDone.
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: receiveUpgradeAck PA=0x%lx already "
                "processed — skipping duplicate\n",
                _nodeId, linePa);
        return;
    }
    upIt->second.ackReceived = true;

    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: OuterUpgradeAck received for PA=0x%lx "
            "— sending deferred SnpResp_I to HN-F\n",
            _nodeId, linePa);
    DPRINTF(RubyEP, "[UPGRADE-DIAG] node=%d UpgradeAck PA=0x%lx home=%d epoch=%lu reqId=%lu\n",
            _nodeId, linePa, upIt->second.homeNode,
            upIt->second.epoch, upIt->second.reqId);

    // Send deferred SnpResp_I to HN-F
    NetDest dest(m_ruby_system);
    dest.add(upIt->second.hnfDest);
    auto rsp = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        linePa, CHIResponseType_SnpResp_I,
        m_machineID, dest,
        false, false, 0, 0, MessageSizeType_Control);
    const int homeNode = upIt->second.homeNode;
    const uint64_t epoch = upIt->second.epoch;
    const uint64_t reqId = upIt->second.reqId;
    sendResponseReliable(rsp, [this, linePa, homeNode, epoch, reqId]() {
        auto pending = _upgradePending.find(linePa);
        if (pending == _upgradePending.end())
            return;
        pending->second.snpRespSent = true;

        EPBackend *backend = EPBackend::getBackendInstance(_nodeId);
        if (!backend) {
            warn("EP_RNF node_id=%d: cannot send UpgradeDone for PA=0x%lx "
                 "because backend is missing\n", _nodeId, linePa);
            return;
        }

        bool doneOk = trySendUpgradeDone(linePa);
        DPRINTF(RubyEP,
                "[UPGRADE-DIAG] node=%d UpgradeDone PA=0x%lx ok=%d "
                "home=%d epoch=%lu reqId=%lu\n",
                _nodeId, linePa, doneOk, homeNode, epoch, reqId);
        if (!doneOk) {
            warn("EP_RNF node_id=%d: sendUpgradeDone failed for PA=0x%lx "
                 "home=%d epoch=%lu reqId=%lu\n",
                 _nodeId, linePa, homeNode, epoch, reqId);
            scheduleEvent(Cycles(1));
        }
    });
}

bool
EPRNFController::trySendUpgradeDone(uint64_t linePa)
{
    auto it = _upgradePending.find(linePa);
    if (it == _upgradePending.end() || !it->second.ackReceived ||
        !it->second.snpRespSent) {
        return false;
    }

    EPBackend *backend = EPBackend::getBackendInstance(_nodeId);
    if (!backend)
        return false;

    if (!backend->sendUpgradeDone(linePa, it->second.homeNode,
                                  it->second.sourceSocket,
                                  it->second.epoch, it->second.reqId)) {
        return false;
    }

    _upgradePending.erase(it);
    backend->flushDeferredInvalidation(linePa);
    return true;
}

void
EPRNFController::processUpgradeDoneRetries()
{
    std::vector<uint64_t> retryLines;
    for (const auto &[linePa, pending] : _upgradePending) {
        if (pending.valid && pending.ackReceived && pending.snpRespSent)
            retryLines.push_back(linePa);
    }

    bool needRetry = false;
    for (uint64_t linePa : retryLines) {
        if (!trySendUpgradeDone(linePa))
            needRetry = true;
    }
    if (needRetry)
        scheduleEvent(Cycles(1));
}

} // namespace ruby
} // namespace gem5
