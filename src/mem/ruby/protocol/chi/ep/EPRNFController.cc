/*
 * Copyright (c) 2024 UBCC Project
 *
 * EP-RNF Sentinel Controller (skeleton)
 */

#include "mem/ruby/protocol/chi/ep/EPRNFController.hh"

#include "debug/RubyCHIGeneric.hh"
#include "mem/ruby/protocol/CHI/CHIRequestType.hh"
#include "mem/ruby/protocol/CHI/CHIResponseType.hh"
#include "mem/ruby/system/RubySystem.hh"

namespace gem5
{

namespace ruby
{

using namespace CHI;

EPRNFController::EPRNFController(const Params &p)
  : CHIGenericController(p),
    nodeId(p.node_id),
    snoopsReceived(0),
    responsesSent(0)
{
    m_machineID.type = MachineType_Cache;
    m_machineID.num = m_version;
}

void
EPRNFController::init()
{
    CHIGenericController::init();
}

void
EPRNFController::wakeup()
{
    CHIGenericController::wakeup();
}

bool
EPRNFController::recvRequestMsg(const CHIRequestMsg *msg)
{
    return true;
}

bool
EPRNFController::recvSnoopMsg(const CHIRequestMsg *msg)
{
    snoopsReceived++;
    sendSnoopResp(msg);
    return true;
}

bool
EPRNFController::recvResponseMsg(const CHIResponseMsg *msg)
{
    return true;
}

bool
EPRNFController::recvDataMsg(const CHIDataMsg *msg)
{
    return true;
}

void
EPRNFController::sendSnoopResp(const CHIRequestMsg *snoop)
{
    // Return Comp_I (clean I-state, no data) as fixed snoop response
    NetDest dest;
    dest.add(snoop->getrequestor());

    auto resp = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        snoop->getaddr(),
        CHIResponseType_Comp_I,
        m_machineID,
        dest,
        false,
        false,
        0, 0,
        MessageSizeType_Response_Control);

    if (sendResponseMsg(resp)) {
        responsesSent++;
    }
}

// === M4: Sentinel Registration API ===

void
EPRNFController::registerExternalSharer(Addr addr)
{
    sentinelTracker.insertSharer(addr, nodeId);
    DPRINTF(RubyCHIGeneric,
            "EPRNFController[%d] registered ExternalSharer for addr=0x%lx\n",
            m_version, addr);
}

void
EPRNFController::registerExternalOwner(Addr addr)
{
    sentinelTracker.insertOwner(addr, nodeId);
    DPRINTF(RubyCHIGeneric,
            "EPRNFController[%d] registered ExternalOwner for addr=0x%lx\n",
            m_version, addr);
}

void
EPRNFController::unregisterSentinel(Addr addr)
{
    sentinelTracker.remove(addr);
    DPRINTF(RubyCHIGeneric,
            "EPRNFController[%d] unregistered sentinel for addr=0x%lx\n",
            m_version, addr);
}

bool
EPRNFController::hasSentinel(Addr addr) const
{
    return sentinelTracker.isPresent(addr);
}

SentinelState
EPRNFController::getSentinelState(Addr addr) const
{
    return sentinelTracker.getState(addr);
}

int
EPRNFController::getSentinelCount() const
{
    return sentinelTracker.count();
}

void
EPRNFController::dumpSentinels() const
{
    sentinelTracker.dump();
}

} // namespace ruby
} // namespace gem5
