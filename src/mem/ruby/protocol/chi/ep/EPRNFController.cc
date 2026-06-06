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

    selfTest();
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
    DPRINTF(RubyCHIGeneric, "EP_RNF node_id=%d recvSnoopMsg type=%s addr=0x%lx\n",
            _nodeId, msg->m_type, msg->m_addr);

    // M4: Increment snoop counter for test verification
    _snoopCount++;
    if (_backend) {
        _backend->incrementEpRnfSnoopCount();
        _backend->checkAddr(msg->m_addr);
    }

    // ---- M6: Check if outer txn is pending ----
    // If there is an in-flight outer transaction for this line,
    // we must delay the HN response until the outer txn completes.
    bool outerPending = isOuterTxnPending(msg->m_addr);

    if (outerPending) {
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: M6 delaying HN response for PA=0x%lx "
                "(outer txn pending)\n",
                _nodeId, msg->m_addr);

        // Allocate pending response context
        PendingHnResponse pending;
        pending.valid = true;
        pending.linePa = msg->m_addr;
        pending.respType = CHIResponseType_SnpResp_I;
        pending.destMachine = msg->m_requestor;
        pending.snoopTick = curTick();
        pending.outerTxnComplete = false;

        _pendingHnResponses[msg->m_addr] = pending;
        _pendingHnResponseCount++;

        // Do NOT immediately respond to HN. The response will be
        // sent when signalOuterTxnComplete() is called.
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: M6 pending HN response queued "
                "PA=0x%lx (count=%d)\n",
                _nodeId, msg->m_addr, _pendingHnResponseCount);

        return true; // message consumed, response delayed
    }

    // ---- Immediate response (no outer txn pending) ----
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
EPRNFController::recvResponseMsg(const CHIResponseMsg *msg)
{
    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d recvResponseMsg type=%s addr=0x%lx\n",
            _nodeId, CHIResponseType_to_string(msg->m_type), msg->m_addr);

    // CompAck from HN-F or other agents: ignore (not tracking req responses)
    if (msg->m_type == CHIResponseType_CompAck) {
        return true;
    }

    // ---- Q3: Comp_UC → CleanUnique completion ----
    if (msg->m_type == CHIResponseType_Comp_UC) {
        auto it = _pendingChiTxns.find(msg->m_addr);
        if (it != _pendingChiTxns.end() &&
            it->second.type == PendingChiTxn::TXN_CLEANUNIQUE) {

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
                        "EP_RNF node_id=%d: CleanUnique complete for "
                        "PA=0x%lx -- invoking callback\n",
                        _nodeId, msg->m_addr);

                auto cb = it->second.onComplete;
                _pendingChiTxns.erase(it);
                if (cb) cb(true);

                // Q3: Clear in-flight and process deferred requests
                _chiRequestInFlight = false;
                processDeferredChiReqs();
            } else {
                // CompAck failed — will retry
                it->second.needsCompAck = true;
                scheduleEvent(Cycles(1));
                DPRINTF(RubyCHIGeneric,
                        "EP_RNF node_id=%d: CleanUnique complete for "
                        "PA=0x%lx but CompAck failed, will retry\n",
                        _nodeId, msg->m_addr);
            }

            return true;
        }

        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: Comp_UC for PA=0x%lx but no pending "
                "CleanUnique txn\n",
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

    // ---- Q3: CompData → ReadShared completion ----
    // Only accept CompData response types
    if (msg->m_type != CHIDataType_CompData_I &&
        msg->m_type != CHIDataType_CompData_SC &&
        msg->m_type != CHIDataType_CompData_UC &&
        msg->m_type != CHIDataType_CompData_UD_PD &&
        msg->m_type != CHIDataType_CompData_SD_PD) {
        return true;
    }

    auto it = _pendingChiTxns.find(msg->m_addr);
    if (it != _pendingChiTxns.end() &&
        (it->second.type == PendingChiTxn::TXN_READSHARED ||
         it->second.type == PendingChiTxn::TXN_READONCE)) {

        // Use msg->m_responder: the HN-F that sent CompData to us
        it->second.hnfDest = msg->m_responder;

        // Build CompAck message and try to send
        NetDest destNet(m_ruby_system);
        destNet.add(msg->m_responder);
        auto ack = std::make_shared<CHIResponseMsg>(
            curTick(), cacheLineSize, m_ruby_system,
            msg->m_addr, CHIResponseType_CompAck,
            m_machineID, destNet,
            false, false, 0, 0, MessageSizeType_Control);

        if (sendResponseMsg(ack)) {
            // CompAck sent successfully — invoke callback
            DPRINTF(RubyCHIGeneric,
                    "EP_RNF node_id=%d: ReadShared complete for PA=0x%lx "
                    "-- invoking callback\n",
                    _nodeId, msg->m_addr);

            auto cb = it->second.onComplete;
            _pendingChiTxns.erase(it);
            if (cb) cb(true);

            // Q3: Clear in-flight and process deferred requests
            _chiRequestInFlight = false;
            processDeferredChiReqs();
        } else {
            // CompAck failed — will retry
            it->second.needsCompAck = true;
            scheduleEvent(Cycles(1));
            DPRINTF(RubyCHIGeneric,
                    "EP_RNF node_id=%d: ReadShared complete for PA=0x%lx "
                    "but CompAck failed, will retry\n",
                    _nodeId, msg->m_addr);
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

// ---- Q3: CHI Request to HN-F ----
bool
EPRNFController::sendChiRequest(uint64_t linePa, CHIRequestType reqType)
{
    // Q3: Serialize CHI requests to prevent TBE reservation exhaustion
    // in the HN-F.  When the HN-F processes multiple requests in the same
    // event-processing cycle, allocateRequestTBE can call decrementReserved
    // twice for a single incrementReserved, triggering assertion failure.
    if (_chiRequestInFlight) {
        // Defer: queue the request for later processing
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: sendChiRequest addr=0x%lx type=%s "
                "DEFERRED (request already in flight)\n",
                _nodeId, linePa, CHIRequestType_to_string(reqType));
        DeferredChiRequest d;
        d.linePa = linePa;
        d.reqType = reqType;
        d.startTick = curTick();
        _deferredChiReqs.push_back(d);
        return true;  // Report success to caller (will be sent later)
    }

    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: sendChiRequest addr=0x%lx type=%s\n",
            _nodeId, linePa, CHIRequestType_to_string(reqType));

    // Create CHI request message
    auto req = std::make_shared<CHIRequestMsg>(
        curTick(), cacheLineSize, m_ruby_system);
    req->m_addr = linePa;
    req->m_type = reqType;
    req->m_requestor = m_machineID;
    req->m_allowRetry = true;
    req->m_MessageSize = MessageSizeType_Control;

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
            "EP_RNF node_id=%d: sendChiRequest addr=0x%lx type=%s "
            "dest=(type=%d num=%d) sent=%d inFlight=%d\n",
            _nodeId, linePa, CHIRequestType_to_string(reqType),
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

            auto cb = it->second.onComplete;
            auto eraseIt = it;
            ++it;
            _pendingChiTxns.erase(eraseIt);
            if (cb) cb(true);

            // Q3: Clear in-flight and process deferred requests
            _chiRequestInFlight = false;
            processDeferredChiReqs();
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
                "addr=0x%lx type=%s (queued at tick=%lu)\n",
                _nodeId, d.linePa,
                CHIRequestType_to_string(d.reqType), d.startTick);
        sendChiRequest(d.linePa, d.reqType);
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
    txn.type = PendingChiTxn::TXN_READSHARED;
    txn.completed = false;
    txn.startTick = curTick();
    txn.onComplete = onComplete;
    _pendingChiTxns[linePa] = txn;

    bool sent = sendChiRequest(linePa, CHIRequestType_ReadShared);
    if (!sent) {
        _pendingChiTxns.erase(linePa);
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: startReadShared addr=0x%lx "
                "send failed\n", _nodeId, linePa);
        if (onComplete) onComplete(false);
    }
}

void
EPRNFController::startReadOnce(uint64_t linePa,
                               std::function<void(bool)> onComplete)
{
    DPRINTF(RubyCHIGeneric,
            "EP_RNF node_id=%d: startReadOnce addr=0x%lx\n",
            _nodeId, linePa);

    if (_pendingChiTxns.find(linePa) != _pendingChiTxns.end()) {
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: startReadOnce addr=0x%lx "
                "already has pending txn\n",
                _nodeId, linePa);
        if (onComplete) onComplete(false);
        return;
    }

    PendingChiTxn txn;
    txn.linePa = linePa;
    txn.type = PendingChiTxn::TXN_READONCE;
    txn.completed = false;
    txn.startTick = curTick();
    txn.onComplete = onComplete;
    _pendingChiTxns[linePa] = txn;

    bool sent = sendChiRequest(linePa, CHIRequestType_ReadOnce);
    if (!sent) {
        _pendingChiTxns.erase(linePa);
        DPRINTF(RubyCHIGeneric,
                "EP_RNF node_id=%d: startReadOnce addr=0x%lx "
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
    txn.type = PendingChiTxn::TXN_CLEANUNIQUE;
    txn.completed = false;
    txn.startTick = curTick();
    txn.onComplete = onComplete;
    _pendingChiTxns[linePa] = txn;

    // Send CleanUnique to HN-F via reqOut
    bool sent = sendChiRequest(linePa, CHIRequestType_CleanUnique);
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

} // namespace ruby
} // namespace gem5
