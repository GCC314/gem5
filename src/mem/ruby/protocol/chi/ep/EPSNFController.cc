/*
 * Copyright (c) 2024 UBCC Project
 *
 * EP-SNF Controller (skeleton)
 */

#include "mem/ruby/protocol/chi/ep/EPSNFController.hh"

#include <cstring>

#include "debug/RubyCHIGeneric.hh"
#include "mem/ruby/common/DataBlock.hh"
#include "mem/ruby/protocol/CHI/CHIDataType.hh"
#include "mem/ruby/protocol/CHI/CHIRequestType.hh"
#include "mem/ruby/protocol/CHI/CHIResponseType.hh"
#include "mem/ruby/system/RubySystem.hh"

namespace gem5
{

namespace ruby
{

using namespace CHI;

EPSNFController::EPSNFController(const Params &p)
  : CHIGenericController(p),
    nodeId(p.node_id),
    readNoSnpReceived(0),
    dataSentCount(0)
{
    m_machineID.type = MachineType_Cache;
    m_machineID.num = m_version;
}

void
EPSNFController::init()
{
    CHIGenericController::init();
}

void
EPSNFController::wakeup()
{
    CHIGenericController::wakeup();
}

bool
EPSNFController::recvRequestMsg(const CHIRequestMsg *msg)
{
    if (msg->gettype() == CHIRequestType_ReadNoSnp) {
        handleReadNoSnp(msg);
    }
    return true;
}

bool
EPSNFController::recvSnoopMsg(const CHIRequestMsg *msg)
{
    return true;
}

bool
EPSNFController::recvResponseMsg(const CHIResponseMsg *msg)
{
    return true;
}

bool
EPSNFController::recvDataMsg(const CHIDataMsg *msg)
{
    return true;
}

void
EPSNFController::handleReadNoSnp(const CHIRequestMsg *msg)
{
    readNoSnpReceived++;
    sendFakeDataResp(msg);
}

void
EPSNFController::sendFakeDataResp(const CHIRequestMsg *req)
{
    // Return Comp_UC + data (UC = Unique Clean, provides data)
    NetDest dest;
    dest.add(req->getrequestor());

    auto resp = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        req->getaddr(),
        CHIResponseType_Comp_UC,
        m_machineID,
        dest,
        false,
        false,
        0, 0,
        MessageSizeType_Response_Data);

    if (!sendResponseMsg(resp)) {
        return;
    }

    // Create data block with fake data pattern (0xAA)
    DataBlock dataBlk(cacheLineSize);
    for (int i = 0; i < cacheLineSize; i++) {
        dataBlk.setByte(i, 0xAA);
    }

    // Create write mask (all bytes valid)
    WriteMask mask(cacheLineSize);
    for (int i = 0; i < cacheLineSize; i++) {
        mask.setMask(i, 1, true);
    }

    auto dataMsg = std::make_shared<CHIDataMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        req->getaddr(),
        CHIDataType_CompData_UC,
        m_machineID,
        dest,
        dataBlk,
        mask,
        false,
        0,
        MessageSizeType_Data);

    if (sendDataMsg(dataMsg)) {
        dataSentCount++;
    }
}

} // namespace ruby
} // namespace gem5
