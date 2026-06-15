#include "mem/ruby/protocol/chi/ep/EPRNFController.hh"

#include <cassert>

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
    _hnfVersion(-1),
    _chiRequestInFlight(false),
    _pendingHnResponseCount(0),
    _delayedResolvedCount(0)
{
    // Derive HN-F version from the first downstream destination controller
    if (!p.downstream_destinations.empty()) {
        _hnfVersion = p.downstream_destinations[0]->getVersion();
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

    // Compute count of other Cache-type controllers for reference
    _numCacheControllers = m_ruby_system->m_num_controllers[MachineType_Cache];

    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: init done, cacheControllers=%d, "
            "hnfVersion=%d\n",
            _nodeId, _numCacheControllers, _hnfVersion);

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

            // For ReadUnique: only complete if all data beats received
            if (it->second.op == PendingChiOp::ReadUnique &&
                it->second.beatsReceived < it->second.beatsExpected) {
                // Still waiting for data beats — defer completion
                DPRINTF(RubyCHIGeneric,
                        "EP_RNF node_id=%d: Comp_UC for ReadUnique at "
                        "PA=0x%lx but waiting for data beats (%d/%d), "
                        "deferring\n",
                        _nodeId, msg->m_addr,
                        it->second.beatsReceived, it->second.beatsExpected);
                // Mark that Comp_UC arrived; completion when data beats done
                it->second.callbackPayloadStable = true;
                it->second.hnfDest = msg->m_responder;
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
        // ReadShared can return multiple CompData beats.  Ack each beat and
        // only finish the txn after the final beat, otherwise the HN-F keeps
        // waiting for missing CompAck(s) and later CleanUnique/upgrade traffic
        // on the same sharer line deadlocks.
        it->second.hnfDest = msg->m_responder;
        it->second.beatsReceived++;

        // F2: Capture recall data from the most recent CompData beat.
        it->second.recallDataBlk = msg->getdataBlk();
        it->second.recallDataValid = true;

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
            DPRINTF(RubyCHIGeneric,
                    "EP_RNF node_id=%d: ReadShared CompAck failed for "
                    "PA=0x%lx beat=%d/%d, will retry\n",
                    _nodeId, msg->m_addr,
                    it->second.beatsReceived, it->second.beatsExpected);
            return true;
        }

        if (it->second.beatsReceived >= it->second.beatsExpected) {
            DPRINTF(RubyCHIGeneric,
                    "EP_RNF node_id=%d: ReadShared complete for PA=0x%lx "
                    "after %d beat(s) -- invoking callback\n",
                    _nodeId, msg->m_addr, it->second.beatsReceived);
            finishChiTxn(msg->m_addr, true);
        }

        return true;
    }

    // ---- v4: ReadUnique data beat handling (§4.3.2, §5.3) ----
    // ReadUnique: HN-F returns CompData (dirty/clean data from old owner)
    // followed by Comp_UC (completion token).  Data beats arrive first;
    // completion is finalized when Comp_UC arrives and all beats counted.
    if (it->second.op == PendingChiOp::ReadUnique) {
        it->second.hnfDest = msg->m_responder;
        it->second.beatsReceived++;

        // F2: Capture recall data from first/most recent CompData beat
        it->second.recallDataBlk = msg->getdataBlk();
        it->second.recallDataValid = true;

        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: ReadUnique data beat %d/%d for "
                "PA=0x%lx\n",
                _nodeId, it->second.beatsReceived,
                it->second.beatsExpected, msg->m_addr);

        // Send CompAck for each data beat (CHI requires per-beat CompAck)
        NetDest destNet(m_ruby_system);
        destNet.add(msg->m_responder);
        auto ack = std::make_shared<CHIResponseMsg>(
            curTick(), cacheLineSize, m_ruby_system,
            msg->m_addr, CHIResponseType_CompAck,
            m_machineID, destNet,
            false, false, 0, 0, MessageSizeType_Control);

        if (!sendResponseMsg(ack)) {
            // CompAck failed — will retry
            it->second.needsCompAck = true;
            scheduleEvent(Cycles(1));
            DPRINTF(RubyCHIGeneric,
                    "EP_RNF node_id=%d: ReadUnique CompAck failed for "
                    "PA=0x%lx, will retry\n",
                    _nodeId, msg->m_addr);
        }

        // If all beats received AND Comp_UC already arrived, complete now
        if (it->second.beatsReceived >= it->second.beatsExpected &&
            it->second.callbackPayloadStable) {
            DPRINTF(RubyCHIGeneric,
                    "EP_RNF node_id=%d: ReadUnique all data + Comp_UC "
                    "received for PA=0x%lx -- invoking callback\n",
                    _nodeId, msg->m_addr);
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
            // F4 diagnostic: these should be unreachable but init-phase
            // page-table setup triggers them on EP-RNF.  Use preserving
            // response to unblock testing while root cause is traced.
            // TODO: restore fatal after fixing init-phase EP-RNF-as-owner.
            warn("EP_RNF node_id=%d: SnpShared/SnpSharedFwd at PA=0x%lx "
                  "— defensive SnpResp_SC (F4 diagnostic)\n",
                  _nodeId, msg->m_addr);
            sendSnpRespSC(msg);
            return true;
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

        bool accepted = backend->notifyLocalWriteUpgrade(
            msg->m_addr, homeNode, 1,
            UpgradeCause::LocalCleanUnique,
            epoch, reqId);

        if (!accepted) {
            DPRINTF(RubyCHIGeneric,
                    "EP_RNF node_id=%d: OuterUpgradeReq not accepted for "
                    "PA=0x%lx — deferring SnpResp_I for retry\n",
                    _nodeId, msg->m_addr);
            printf("[UPGRADE-DIAG] node=%d OuterUpgradeReq rejected PA=0x%lx\n",
                   _nodeId, msg->m_addr);
            return false;
        }

        UpgradePending pending;
        pending.valid = true;
        pending.linePa = msg->m_addr;
        pending.homeNode = homeNode;
        pending.epoch = epoch;
        pending.reqId = reqId;
        pending.hnfDest = msg->m_requestor;
        _upgradePending[msg->m_addr] = pending;

        // upgrade_invalidate_fix D2: only call receiveUpgradeAck() immediately
        // if the ack is ready (targetMask==0, lastUpgradeAck().accepted==true).
        // If targetMask!=0, the ack will be triggered later via
        // EPBackend::notifyUpgradeAckReady() when all invalidation acks arrive.
        if (backend->lastUpgradeAck().accepted) {
            // Fast path: no other sharers, immediate Ack(true)
            receiveUpgradeAck(msg->m_addr);
        } else {
            // Deferred: wait for all invalidation acks to arrive
            printf("[UPGRADE-DIAG] node=%d upgrade deferred ack PA=0x%lx "
                   "— waiting for invalidation acks\n",
                   _nodeId, msg->m_addr);
        }
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
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: sendChiRequest addr=0x%lx type=%d "
                "DEFERRED (request already in flight)\n",
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

    // Use HN-F version from config (no dynamic mapping)
    MachineID hnfId;
    hnfId.type = MachineType_Cache;
    hnfId.num = _hnfVersion;
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
            "dest=(type=%d num=%d) sent=%d inFlight=%d\n",
            _nodeId, linePa, static_cast<int>(reqType),
            hnfId.getType(), hnfId.getNum(), sent, _chiRequestInFlight);

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
    txn.callbackPayloadStable = false;
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
    txn.callbackPayloadStable = false;
    txn.startTick = curTick();
    txn.onComplete = onComplete;
    _pendingChiTxns[linePa] = txn;

    bool sent = sendChiRequest(linePa, CHIRequestType_ReadUnique,
                               EpProxyOp_RecallUnique);
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
    txn.callbackPayloadStable = false;
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
}

} // namespace ruby
} // namespace gem5
