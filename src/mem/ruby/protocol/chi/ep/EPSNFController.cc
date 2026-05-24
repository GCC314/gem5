#include "mem/ruby/protocol/chi/ep/EPSNFController.hh"

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "mem/ruby/protocol/CHI/CHIDataMsg.hh"
#include "mem/ruby/protocol/CHI/CHIRequestMsg.hh"
#include "mem/ruby/protocol/CHI/CHIResponseMsg.hh"
#include "params/EPSNFController.hh"

namespace gem5
{

namespace ruby
{

using namespace CHI;

EPSNFController::EPSNFController(const Params &p)
  : EPController(p), _backend(p.ep_backend)
{
}

void
EPSNFController::init()
{
    EPController::init();
    fatal_if(!_backend, "EP_SNF node_id=%d: no backend attached", _nodeId);
    selfTest();
}

void
EPSNFController::selfTest()
{
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d selfTest begin\n", _nodeId);

    auto test_req = std::make_shared<CHIRequestMsg>(
        curTick(), cacheLineSize, m_ruby_system);
    test_req->m_addr = 0x10000000;
    test_req->m_type = CHIRequestType_ReadNoSnp;
    test_req->m_requestor = m_machineID;

    auto *req_buf = reqIn;
    if (req_buf->areNSlotsAvailable(1, curTick())) {
        req_buf->enqueue(test_req, curTick());
        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d selfTest: injected ReadNoSnp\n", _nodeId);
    }

    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d selfTest end\n", _nodeId);
}

void
EPSNFController::wakeup()
{
    EPController::wakeup();
    if (_backend)
        _backend->wakeup();
}

void
EPSNFController::print(std::ostream& out) const
{
    out << "[EP_SNF node_id=" << _nodeId << " v=" << m_version << "]";
}

bool
EPSNFController::recvRequestMsg(const CHIRequestMsg *msg)
{
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d recvRequestMsg type=%s addr=0x%lx\n",
            _nodeId, msg->m_type, msg->m_addr);

    if (_backend)
        _backend->checkAddr(msg->m_addr);

    NetDest dest;
    dest.add(msg->m_requestor);

    auto rsp = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        msg->m_addr, CHIResponseType_RespSepData,
        m_machineID, dest,
        false, false, 0, 0, MessageSizeType_Control);
    sendResponseMsg(rsp);

    auto dat = std::make_shared<CHIDataMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        msg->m_addr, CHIDataType_CompData_I,
        m_machineID, dest,
        DataBlock(cacheLineSize),
        WriteMask(cacheLineSize),
        false, 0, MessageSizeType_Data);
    sendDataMsg(dat);

    return true;
}

bool
EPSNFController::recvSnoopMsg(const CHIRequestMsg *msg)
{
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d recvSnoopMsg\n", _nodeId);
    if (_backend)
        _backend->checkAddr(msg->m_addr);
    return true;
}

bool
EPSNFController::recvResponseMsg(const CHIResponseMsg *msg)
{
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d recvResponseMsg\n", _nodeId);
    return true;
}

bool
EPSNFController::recvDataMsg(const CHIDataMsg *msg)
{
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d recvDataMsg\n", _nodeId);
    return true;
}

} // namespace ruby
} // namespace gem5
