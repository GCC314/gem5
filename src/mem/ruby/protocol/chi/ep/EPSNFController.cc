#include "mem/ruby/protocol/chi/ep/EPSNFController.hh"

#include "debug/RubyCHIGeneric.hh"
#include "mem/ruby/protocol/CHI/CHIDataMsg.hh"
#include "mem/ruby/protocol/CHI/CHIRequestMsg.hh"
#include "mem/ruby/protocol/CHI/CHIResponseMsg.hh"
#include "params/EPSNFController.hh"

namespace gem5
{

namespace ruby
{

EPSNFController::EPSNFController(const Params &p)
  : EPController(p)
{
}

void
EPSNFController::init()
{
    EPController::init();
    fatal_if(!_backend, "EP_SNF node_id=%d: no backend attached", _nodeId);
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
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d recvRequestMsg\n", _nodeId);
    return true;
}

bool
EPSNFController::recvSnoopMsg(const CHIRequestMsg *msg)
{
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d recvSnoopMsg\n", _nodeId);
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
