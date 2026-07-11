#include "mem/ruby/protocol/chi/ep/EPRNFController.hh"

#include <cassert>
#include <cstdlib>

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
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

static uint64_t eprn_compack_retry() {
    static uint64_t v = 0;
    if (v == 0) {
        const char *e = std::getenv("EPRN_COMPACK_RETRY_CYCLES");
        v = e ? std::strtoull(e, nullptr, 10) : 100000;
    }
    return v;
}

static uint64_t eprn_wakeup_retry() {
    static uint64_t v = 0;
    if (v == 0) {
        const char *e = std::getenv("EPRN_WAKEUP_RETRY_CYCLES");
        v = e ? std::strtoull(e, nullptr, 10) : 1000000;
    }
    return v;
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

    assert(m_net_ptr != nullptr);

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
    assert(seq != nullptr);
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
    _numCacheControllers(0),
    _numSockets(p.downstream_destinations.size()),
    _addrMap(p.num_nodes, _numSockets, 128ULL * 1024 * 1024),
    _chiRequestInFlight(false),
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

    // Retry any pending CompAck sends
    retryPendingCompAcks();

    // Q3: Process deferred CHI requests (cleanup + safety net)
    processDeferredChiReqs();

    if (_backend)
        _backend->wakeup();

    // Process delayed upgrade retries (rejected upgrades waiting for home to drain).
    processUpgradeRetries();
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

    // ---- Per-PA single-flight check (§4.3.3) ----
    auto txnIt = _pendingChiTxns.find(msg->m_addr);
    bool inflight = (txnIt != _pendingChiTxns.end());

    if (inflight) {
        // ---- CHI transaction already in flight for this PA ----
        // Check 1-entry snoop slot: if already occupied, protocol violation
        if (txnIt->second.snoopSlotValid) {
            fatal("EP_RNF node_id=%d: second snoop for PA=0x%lx while "
                  "snoop slot already occupied — protocol violation "
                  "(HN-F single-flight assumption broken)\n",
                  _nodeId, msg->m_addr);
        }

        // Queue this snoop in the 1-entry per-PA snoop slot (§4.3.3)
        txnIt->second.snoopSlotValid = true;
        txnIt->second.queuedSnoopType = msg->m_type;
        txnIt->second.queuedRetToSrc = msg->m_retToSrc;

        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: queued snoop type=%d for PA=0x%lx "
                "(CHI txn in flight)\n",
                _nodeId, static_cast<int>(msg->m_type), msg->m_addr);
        return true;
    }

    // ---- No in-flight CHI transaction — process snoop immediately ----
    return processSnoopImmediate(msg);
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
        printf("[EPRNF-RETRY-DIAG] node=%d type=%s PA=0x%lx chiInFlight=%d pendingFound=%d\n",
               _nodeId, msg->m_type == CHIResponseType_RetryAck ? "RetryAck" : "PCrdGrant",
               msg->m_addr, _chiRequestInFlight,
               it != _pendingChiTxns.end());
    }

    // CompAck from HN-F or other agents: ignore (not tracking req responses)
    if (msg->m_type == CHIResponseType_CompAck) {
        return true;
    }

    // ---- Comp_UC completion (CleanUnique + ReadUnique) ----
    // Comp_UC is the completion token for both CleanUnique and ReadUnique.
    // For CleanUnique: no data, just the token.
    // For ReadUnique: data arrives via CompData first, then Comp_UC finalizes.
    if (msg->m_type == CHIResponseType_Comp_UC) {
        auto it = _pendingChiTxns.find(msg->m_addr);
        printf("[COMPUC-DIAG] node=%d received Comp_UC PA=0x%lx found=%d needsCompAck=%d\n",
               _nodeId, msg->m_addr,
               it != _pendingChiTxns.end(),
               it != _pendingChiTxns.end() ? it->second.needsCompAck : -1);
        if (it != _pendingChiTxns.end() &&
            (it->second.op == PendingChiOp::CleanUnique ||
             it->second.op == PendingChiOp::ReadUnique)) {

            // For ReadUnique: data beats drive completion, not Comp_UC.
            // The last data beat already sent CompAck + finishChiTxn.
            // If we get here, beats may still be pending — defer.
            if (it->second.op == PendingChiOp::ReadUnique) {
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

            if (sendResponseMsg(ack)) {
                // CompAck sent successfully
                DPRINTF(RubyCHIGeneric,
                        "EP_RNF node_id=%d: %s complete for "
                        "PA=0x%lx -- invoking callback\n",
                        _nodeId,
                        (it->second.op == PendingChiOp::CleanUnique)
                            ? "CleanUnique" : "ReadUnique",
                        msg->m_addr);

                finishChiTxn(msg->m_addr, true);
            } else {
                // CompAck failed — will retry
                it->second.needsCompAck = true;
                scheduleEvent(Cycles(1));
                DPRINTF(RubyCHIGeneric,
                        "EP_RNF node_id=%d: %s complete for "
                        "PA=0x%lx but CompAck failed, will retry\n",
                        _nodeId,
                        (it->second.op == PendingChiOp::CleanUnique)
                            ? "CleanUnique" : "ReadUnique",
                        msg->m_addr);
            }

            return true;
        }

        DPRINTF(RubyCHIGeneric,
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
        it->second.recallDataBlk = msg->getdataBlk();
        it->second.recallDataValid = true;

        // Send CompAck only on last beat (HN-F expects exactly 1 per txn)
        if (it->second.beatsReceived >= it->second.beatsExpected) {
            NetDest destNet(m_ruby_system);
            destNet.add(msg->m_responder);
            auto ack = std::make_shared<CHIResponseMsg>(
                curTick(), cacheLineSize, m_ruby_system,
                msg->m_addr, CHIResponseType_CompAck,
                m_machineID, destNet,
                false, false, 0, 0, MessageSizeType_Control);
            if (!sendResponseMsg(ack)) {
                it->second.needsCompAck = true;
                scheduleEvent(Cycles(1));
                return true;
            }
            finishChiTxn(msg->m_addr, true);
        }
        return true;
    }

    // ---- v4: ReadUnique data beat handling (§4.3.2, §5.3) ----
    // ReadUnique: HN-F returns CompData (dirty/clean data from old owner)
    // followed by Comp_UC (completion token).  Data beats arrive first;
    // completion is finalized when Comp_UC arrives and all beats counted.
    // FV risk P1-R5: ReadUnique completes on last data beat (relaxed completion).
    // TODO strict: wait for Comp_UC+CompAck before callback
    if (it->second.op == PendingChiOp::ReadUnique) {
        it->second.hnfDest = msg->m_responder;
        it->second.beatsReceived++;
        it->second.recallDataBlk = msg->getdataBlk();
        it->second.recallDataValid = true;

        // Only send CompAck on last beat. HN-F expects exactly 1 per txn.
        if (it->second.beatsReceived >= it->second.beatsExpected) {
            NetDest destNet(m_ruby_system);
            destNet.add(msg->m_responder);
            auto ack = std::make_shared<CHIResponseMsg>(
                curTick(), cacheLineSize, m_ruby_system,
                msg->m_addr, CHIResponseType_CompAck,
                m_machineID, destNet,
                false, false, 0, 0, MessageSizeType_Control);
            if (!sendResponseMsg(ack)) {
                it->second.needsCompAck = true;
                scheduleEvent(Cycles(1));
                return true;
            }
            finishChiTxn(msg->m_addr, true);
        }
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
    sendResponseMsg(rsp);

    _delayedResolvedCount++;

    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: M6 delayed HN response sent "
            "PA=0x%lx (resolved=%d)\n",
            _nodeId, linePa, _delayedResolvedCount);
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

    // Check if upgrade is pending for this PA (set by the first snoop arrival)
    auto upIt = _upgradePending.find(msg->m_addr);
    if (upIt != _upgradePending.end() && upIt->second.valid) {
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
            printf("[SELF-SNOOP] node=%d SnpCleanInvalid PA=0x%lx "
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
        printf("[RECALL-SNOOP] node=%d SnpCleanInvalid PA=0x%lx "
               "during active recall — immediate SnpResp_I\n",
               _nodeId, msg->m_addr);
        backend->clearActiveRecall(msg->m_addr);
        return sendSnpRespI(msg);
    }

    if (isDsmLine) {
        int homeNode = backend->homeNodeCrossNode(msg->m_addr);
        uint64_t epoch = 0;
        uint64_t reqId = 0;

        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: SnpCleanInvalid first-arrival upgrade path "
                "for PA=0x%lx home=%d — issuing OuterUpgradeReq\n",
                _nodeId, msg->m_addr, homeNode);
        printf("[UPGRADE-DIAG] node=%d first SnpCleanInvalid PA=0x%lx home=%d\n",
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
        sendResponseMsg(rsp);
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
    sendResponseMsg(rsp);
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
    sendResponseMsg(rsp);
    return true;
}

bool
EPRNFController::sendSnpRespDataSC(const CHIRequestMsg *msg)
{
    // SnpRespData_SC: shared clean data response.
    // Send SnpResp_SC first (response), then SnpRespData_SC (data).
    NetDest dest(m_ruby_system);
    dest.add(msg->m_requestor);

    // Response: SnpResp_SC
    auto rsp = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        msg->m_addr, CHIResponseType_SnpResp_SC,
        m_machineID, dest,
        false, false, 0, 0, MessageSizeType_Control);
    sendResponseMsg(rsp);

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
        sendDataMsg(dat);
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
    printf("[EPRNF-FINISH] node=%d PA=0x%lx success=%d\n",
           _nodeId, linePa, success);
    auto txnIt = _pendingChiTxns.find(linePa);
    if (txnIt == _pendingChiTxns.end()) {
        return;
    }

    auto cb = txnIt->second.onComplete;
    bool hadQueuedSnoop = txnIt->second.snoopSlotValid;

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

    // Clear in-flight flag
    _chiRequestInFlight = false;

    // §4.3.3: Queued snoop has higher priority than deferred CHI requests
    if (hadQueuedSnoop) {
        processQueuedSnoop(linePa);
    }

    // Process retry queue entries (outbound CHI requests with strongest-op)
    processRetryQueue();

    // Process any remaining deferred CHI requests
    processDeferredChiReqs();
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
    if (_chiRequestInFlight) {
        // Defer: queue the request for later processing
        printf("[EPRNF-DEFER] node=%d PA=0x%lx type=%d — queued\n",
               _nodeId, linePa, static_cast<int>(reqType));
        DeferredChiRequest d;
        d.linePa = linePa;
        d.reqType = reqType;
        d.proxyOp = proxyOp;
        d.startTick = curTick();
        _deferredChiReqs.push_back(d);
        return true;  // Report success to caller (will be sent later)
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
        _chiRequestInFlight = true;
    } else {
        warn("EP_RNF node_id=%d: sendChiRequest failed for addr=0x%lx "
             "(reqOut full)\n", _nodeId, linePa);
    }

    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: sendChiRequest addr=0x%lx type=%d "
            "homeSocket=%d dest=(type=%d num=%d) sent=%d inFlight=%d\n",
            _nodeId, linePa, static_cast<int>(reqType),
            homeSocket, hnfId.getType(), hnfId.getNum(), sent, _chiRequestInFlight);

    return sent;
}

void
EPRNFController::sendCompAck(uint64_t linePa, MachineID dest)
{
    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: sendCompAck addr=0x%lx\n",
            _nodeId, linePa);

    NetDest destNet(m_ruby_system);
    destNet.add(dest);
    auto ack = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        linePa, CHIResponseType_CompAck,
        m_machineID, destNet,
        false, false, 0, 0, MessageSizeType_Control);

    if (!sendResponseMsg(ack)) {
        warn("EP_RNF node_id=%d: sendCompAck failed for addr=0x%lx "
             "(rspOut full)\n", _nodeId, linePa);
    }
}

void
EPRNFController::retryPendingCompAcks()
{
    bool needRetry = false;
    for (auto it = _pendingChiTxns.begin();
         it != _pendingChiTxns.end(); ) {
        if (!it->second.needsCompAck) {
            ++it;
            continue;
        }

        // Try to send pending CompAck
        NetDest destNet(m_ruby_system);
        destNet.add(it->second.hnfDest);
        auto ack = std::make_shared<CHIResponseMsg>(
            curTick(), cacheLineSize, m_ruby_system,
            it->second.linePa, CHIResponseType_CompAck,
            m_machineID, destNet,
            false, false, 0, 0, MessageSizeType_Control);

        if (sendResponseMsg(ack)) {
            // CompAck sent — invoke callback
            DPRINTF(RubyCHIGeneric,
                    "EP_RNF node_id=%d: retryPendingCompAcks "
                    "succeeded for PA=0x%lx -- invoking callback\n",
                    _nodeId, it->second.linePa);

            uint64_t linePa = it->second.linePa;
            ++it;
            finishChiTxn(linePa, true);
        } else {
            needRetry = true;
            ++it;
        }
    }

    if (needRetry) {
        scheduleEvent(Cycles(1));
    }
}

// ---- Q3: Process deferred CHI requests ----
void
EPRNFController::processDeferredChiReqs()
{
    while (!_deferredChiReqs.empty() && !_chiRequestInFlight) {
        DeferredChiRequest d = _deferredChiReqs.front();
        _deferredChiReqs.pop_front();
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: processing deferred CHI request "
                "addr=0x%lx type=%d proxyOp=%d (queued at tick=%lu)\n",
                _nodeId, d.linePa,
                static_cast<int>(d.reqType),
                static_cast<int>(d.proxyOp), d.startTick);
        sendChiRequest(d.linePa, d.reqType, d.proxyOp);
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
        DPRINTF(RubyCHIGeneric,
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
        DPRINTF(RubyCHIGeneric,
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

    if (_pendingChiTxns.find(linePa) != _pendingChiTxns.end()) {
        DPRINTF(RubyCHIGeneric,
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

    bool sent = sendChiRequest(linePa, CHIRequestType_ReadUnique,
                               EpProxyOp_RecallUnique);
    printf("[EPRNF-RECALL] node=%d startReadUnique PA=0x%lx sent=%d\n",
           _nodeId, linePa, sent);
    if (!sent) {
        _pendingChiTxns.erase(linePa);
        DPRINTF(RubyCHIGeneric,
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
        printf("[CLEANUNIQUE-DIAG] node=%d PA=0x%lx DUPLICATE — already has pending txn op=%d\n",
               _nodeId, linePa, (int)_pendingChiTxns[linePa].op);
        DPRINTF(RubyCHIGeneric,
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
    txn.needsCompAck = true;  // F6: must send CompAck to unblock HN-F WaitCompAck
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
    printf("[CLEANUNIQUE-DIAG] node=%d PA=0x%lx sendChiRequest sent=%d\n",
           _nodeId, linePa, sent);
    if (!sent) {
        // Send failed — clean up pending txn and notify caller
        _pendingChiTxns.erase(linePa);
        DPRINTF(RubyCHIGeneric,
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
            DPRINTF(RubyCHIGeneric,
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
    sendResponseMsg(rsp);
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
        // Clear pending txn in EPBackend so notifyLocalWriteUpgrade allocates
        // a fresh reqId instead of reusing the rejected one (checkOnly).
        EPBackend *backend = EPBackend::getBackendInstance(_nodeId);
        if (backend) {
            backend->clearPendingUpgradeTxn(linePa);
            backend->clearCachedUpgradeResp(linePa);
        }
    }
    // Schedule the retry. The interval must be long enough for the other
    // upgrade to fully drain at the home (InvalidateAck → commit →
    // UpgradeAckNotify → UpgradeDone). ~1M cycles (500µs @2GHz) is generous.
    _upgradeRetryLines.insert(linePa);
    scheduleEvent(Cycles(eprn_wakeup_retry()));
}

void
EPRNFController::processUpgradeRetries()
{
    if (_upgradeRetryLines.empty())
        return;
    printf("[RETRY-DIAG] node=%d processUpgradeRetries lines=%zu\n",
           _nodeId, _upgradeRetryLines.size());
    auto lines = _upgradeRetryLines;
    _upgradeRetryLines.clear();
    for (uint64_t linePa : lines) {
        auto upIt = _upgradePending.find(linePa);
        if (upIt == _upgradePending.end() || !upIt->second.valid) {
            continue;
        }
        if (upIt->second.ackReceived) {
            continue;
        }
        // For needsRetry: clear the flag so completeHeldUpgrade proceeds
        // normally (issues a fresh OuterUpgradeReq, not checkOnly).
        upIt->second.needsRetry = false;
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: retrying held upgrade PA=0x%lx\n",
                _nodeId, linePa);
        printf("[UPGRADE-DIAG] node=%d retry upgrade PA=0x%lx\n",
               _nodeId, linePa);
        completeHeldUpgrade(linePa);
    }
}

void
EPRNFController::completeHeldUpgrade(uint64_t linePa)
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

    // Re-check the upgrade. With checkOnly semantics (an in-flight upgrade is
    // already pending in EPBackend::_pendingUpgradeTxns), this returns true once
    // the cached OuterUpgradeResp is available, false while still pending or
    // rejected. `rejected` distinguishes a hard reject from async-pending;
    // `notSharer` distinguishes a PERMANENT reject (we lost a dual-upgrade race
    // and were invalidated) from a TEMPORARY reject (another op is outstanding).
    bool accepted = backend->notifyLocalWriteUpgrade(
        linePa, homeNode, 1,
        UpgradeCause::LocalCleanUnique,
        epoch, reqId, &rejected, &notSharer);

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
        backend->clearCachedUpgradeResp(linePa);
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
        // Still pending — keep holding the snoop. Will be retried when the
        // UpgradeResp arrives (onUpgradeRespArrived) or acks complete.
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: completeHeldUpgrade PA=0x%lx still pending "
                "— holding snoop\n",
                _nodeId, linePa);
        return;
    }

    // Accepted: record the real epoch/reqId for the deferred SnpResp_I /
    // UpgradeDone path.
    upIt->second.epoch = epoch;
    upIt->second.reqId = reqId;

    // upgrade_invalidate_fix D2: only call receiveUpgradeAck() immediately if
    // the ack is ready (targetMask==0). Otherwise it is triggered later via
    // EPBackend::notifyUpgradeAckReady() when all invalidation acks arrive.
    if (backend->lastUpgradeAck().accepted) {
        // Fast path: no other sharers, immediate Ack(true)
        receiveUpgradeAck(linePa);
    } else {
        // Deferred: wait for all invalidation acks to arrive
        printf("[UPGRADE-DIAG] node=%d upgrade deferred ack PA=0x%lx "
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
    printf("[UPGRADE-DIAG] node=%d UpgradeAck PA=0x%lx home=%d epoch=%lu reqId=%lu\n",
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
    sendResponseMsg(rsp);

    EPBackend *backend = EPBackend::getBackendInstance(_nodeId);
    if (!backend) {
        warn("EP_RNF node_id=%d: cannot send UpgradeDone for PA=0x%lx "
             "because backend is missing\n",
             _nodeId, linePa);
        _upgradePending.erase(upIt);
        return;
    }

    bool doneOk = backend->sendUpgradeDone(
        linePa, upIt->second.homeNode, upIt->second.epoch,
        upIt->second.reqId);
    printf("[UPGRADE-DIAG] node=%d UpgradeDone PA=0x%lx ok=%d home=%d epoch=%lu reqId=%lu\n",
           _nodeId, linePa, doneOk, upIt->second.homeNode,
           upIt->second.epoch, upIt->second.reqId);
    if (!doneOk) {
        warn("EP_RNF node_id=%d: sendUpgradeDone failed for PA=0x%lx "
             "home=%d epoch=%lu reqId=%lu\n",
             _nodeId, linePa, upIt->second.homeNode,
             upIt->second.epoch, upIt->second.reqId);
    }

    // Clear upgrade pending state
    _upgradePending.erase(upIt);

    // Process any InvalidateReq deferred while the snoop was held. In the
    // accepted path the upgrade has completed and SnpResp_I was already sent
    // above, so the local copy state is consistent.
    if (backend)
        backend->flushDeferredInvalidation(linePa);
}

} // namespace ruby
} // namespace gem5
