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
    panic("EPController doesn't expect functionalRead");
}

int
EPController::functionalWrite(
    const Addr& param_addr, Packet* param_pkt)
{
    panic("EPController doesn't expect functionalRead");
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
    _pendingHnResponseCount(0),
    _delayedResolvedCount(0)
{
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
    selfTest();
}

void
EPRNFController::wakeup()
{
    EPController::wakeup();
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
    NetDest dest;
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
    DPRINTF(RubyCHIGeneric, "EP_RNF node_id=%d recvResponseMsg\n", _nodeId);
    return true;
}

bool
EPRNFController::recvDataMsg(const CHIDataMsg *msg)
{
    DPRINTF(RubyCHIGeneric, "EP_RNF node_id=%d recvDataMsg\n", _nodeId);
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
    NetDest dest;
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

} // namespace ruby
} // namespace gem5
