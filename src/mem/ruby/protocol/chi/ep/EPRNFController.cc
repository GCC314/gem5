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
  : EPController(p), _backend(p.ep_backend)
{
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

} // namespace ruby
} // namespace gem5
