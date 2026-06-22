#include "mem/ruby/protocol/chi/ep/MetaRNFController.hh"

#include <algorithm>

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "mem/ruby/protocol/CHI/CHIProtocolInfo.hh"

namespace gem5
{

namespace ruby
{

using namespace CHI;

std::map<std::pair<int,int>, MetaRNFController*> MetaRNFController::_instances;

MetaRNFController::MetaRNFController(const Params &p)
  : EPController(p),
    _metadataRange(p.metadata_private_range),
    _hnfVersion(-1),
    _maxFlights(p.flight_slots)
{
    if (!p.downstream_destinations.empty()) {
        _hnfVersion = p.downstream_destinations[0]->getVersion();
    }

    for (int i = 0; i < 8; ++i)
        _flightSlots[i] = FlightSlot();

    _instances[{_nodeId, p.socket_id}] = this;
}

MetaRNFController::~MetaRNFController()
{
    _instances.erase({_nodeId, 0});
}

MetaRNFController*
MetaRNFController::getInstance(int node_id, int socket_id)
{
    auto it = _instances.find({node_id, socket_id});
    return (it == _instances.end()) ? nullptr : it->second;
}

void
MetaRNFController::init()
{
    EPController::init();
    fatal_if(_hnfVersion < 0,
             "MetaRNF node_id=%d: downstream HN-F missing", _nodeId);
}

void
MetaRNFController::wakeup()
{
    EPController::wakeup();
}

void
MetaRNFController::print(std::ostream& out) const
{
    out << "[MetaRNF node_id=" << _nodeId << " v=" << m_version
        << " flights=" << activeFlightCount() << "/" << _maxFlights << "]";
}

int
MetaRNFController::findFreeSlot() const
{
    for (int i = 0; i < _maxFlights; ++i) {
        if (_flightSlots[i].state == SlotState::Free)
            return i;
    }
    return -1;
}

int
MetaRNFController::activeFlightCount() const
{
    int n = 0;
    for (int i = 0; i < 8; ++i)
        if (_flightSlots[i].state != SlotState::Free)
            ++n;
    return n;
}

bool
MetaRNFController::inMetadataRange(uint64_t pa) const
{
    return _metadataRange.contains(pa);
}

void
MetaRNFController::issueRead(uint64_t metadataPa, ReadCallback cb)
{
    MetaLine zero{};
    if (!inMetadataRange(metadataPa)) {
        if (cb) cb(false, zero);
        return;
    }

    auto sbIt = _scoreboard.find(metadataPa);
    if (sbIt != _scoreboard.end()) {
        QueuedOp q;
        q.op = OpType::Read;
        q.pa = metadataPa;
        q.readCb = cb;
        _waitQueues[metadataPa].push_back(q);
        return;
    }

    int slot = findFreeSlot();
    if (slot < 0) {
        if (cb) cb(false, zero);
        return;
    }

    FlightSlot &fs = _flightSlots[slot];
    fs.state = SlotState::Allocated;
    fs.op = OpType::Read;
    fs.pa = metadataPa;
    fs.readCb = cb;
    _scoreboard[metadataPa] = slot;

    if (!sendReadOnce(metadataPa)) {
        _scoreboard.erase(metadataPa);
        fs.reset();
        if (cb) cb(false, zero);
    }
}

void
MetaRNFController::issueWrite(uint64_t metadataPa, const MetaLine &line,
                              WriteCallback cb)
{
    if (!inMetadataRange(metadataPa)) {
        if (cb) cb(false);
        return;
    }

    auto sbIt = _scoreboard.find(metadataPa);
    if (sbIt != _scoreboard.end()) {
        QueuedOp q;
        q.op = OpType::Write;
        q.pa = metadataPa;
        q.writeCb = cb;
        q.writeData.setData(line.data(), 0, 64);
        _waitQueues[metadataPa].push_back(q);
        return;
    }

    int slot = findFreeSlot();
    if (slot < 0) {
        if (cb) cb(false);
        return;
    }

    FlightSlot &fs = _flightSlots[slot];
    fs.state = SlotState::Allocated;
    fs.op = OpType::Write;
    fs.pa = metadataPa;
    fs.writeCb = cb;
    fs.writeData.setData(line.data(), 0, 64);
    _scoreboard[metadataPa] = slot;

    if (!sendWriteUnique(metadataPa)) {
        _scoreboard.erase(metadataPa);
        fs.reset();
        if (cb) cb(false);
    }
}

void
MetaRNFController::issueDelete(uint64_t metadataPa, WriteCallback cb)
{
    MetaLine zero{};
    if (!inMetadataRange(metadataPa)) {
        if (cb) cb(false);
        return;
    }

    auto sbIt = _scoreboard.find(metadataPa);
    if (sbIt != _scoreboard.end()) {
        QueuedOp q;
        q.op = OpType::Delete;
        q.pa = metadataPa;
        q.writeCb = cb;
        q.writeData.setData(zero.data(), 0, 64);
        _waitQueues[metadataPa].push_back(q);
        return;
    }

    int slot = findFreeSlot();
    if (slot < 0) {
        if (cb) cb(false);
        return;
    }

    FlightSlot &fs = _flightSlots[slot];
    fs.state = SlotState::Allocated;
    fs.op = OpType::Delete;
    fs.pa = metadataPa;
    fs.writeCb = cb;
    fs.writeData.setData(zero.data(), 0, 64);
    _scoreboard[metadataPa] = slot;

    if (!sendWriteUnique(metadataPa)) {
        _scoreboard.erase(metadataPa);
        fs.reset();
        if (cb) cb(false);
    }
}

void
MetaRNFController::drainWaitQueue(uint64_t metadataPa)
{
    auto wit = _waitQueues.find(metadataPa);
    if (wit == _waitQueues.end() || wit->second.empty())
        return;

    QueuedOp q = wit->second.front();
    wit->second.pop_front();
    if (wit->second.empty())
        _waitQueues.erase(metadataPa);

    MetaLine ml{};
    switch (q.op) {
      case OpType::Read:
        issueRead(metadataPa, q.readCb);
        break;
      case OpType::Write:
        for (int i = 0; i < 64; ++i)
            ml[i] = q.writeData.getByte(i);
        issueWrite(metadataPa, ml, q.writeCb);
        break;
      case OpType::Delete:
        issueDelete(metadataPa, q.writeCb);
        break;
    }
}

bool
MetaRNFController::sendReadOnce(uint64_t pa)
{
    auto req = std::make_shared<CHIRequestMsg>(curTick(), cacheLineSize,
                                                m_ruby_system);
    req->m_addr = pa;
    req->m_type = CHIRequestType_ReadOnce;
    req->m_requestor = m_machineID;
    req->m_allowRetry = true;
    req->m_MessageSize = MessageSizeType_Control;
    MachineID hnfId;
    hnfId.type = MachineType_Cache;
    hnfId.num = _hnfVersion;
    req->m_Destination.clear();
    req->m_Destination.add(hnfId);

    bool sent = sendRequestMsg(req);
    if (sent) {
        auto sbIt = _scoreboard.find(pa);
        if (sbIt != _scoreboard.end())
            _flightSlots[sbIt->second].state = SlotState::Sent;
    }
    return sent;
}

bool
MetaRNFController::sendWriteUnique(uint64_t pa)
{
    auto req = std::make_shared<CHIRequestMsg>(curTick(), cacheLineSize,
                                                m_ruby_system);
    req->m_addr = pa;
    req->m_type = CHIRequestType_WriteUniqueFull;
    req->m_requestor = m_machineID;
    req->m_allowRetry = true;
    req->m_MessageSize = MessageSizeType_Control;
    MachineID hnfId;
    hnfId.type = MachineType_Cache;
    hnfId.num = _hnfVersion;
    req->m_Destination.clear();
    req->m_Destination.add(hnfId);

    bool sent = sendRequestMsg(req);
    if (sent) {
        auto sbIt = _scoreboard.find(pa);
        if (sbIt != _scoreboard.end())
            _flightSlots[sbIt->second].state = SlotState::Sent;
    }
    return sent;
}

bool
MetaRNFController::sendWriteData(uint64_t pa, MachineID dst, uint64_t dbid)
{
    auto sbIt = _scoreboard.find(pa);
    if (sbIt == _scoreboard.end())
        return false;

    int slot = sbIt->second;
    FlightSlot &fs = _flightSlots[slot];

    auto dat = std::make_shared<CHIDataMsg>(curTick(), cacheLineSize,
                                             m_ruby_system);
    dat->m_addr = pa;
    dat->m_type = CHIDataType_NCBWrData;
    dat->m_responder = m_machineID;
    dat->m_Destination.clear();
    dat->m_Destination.add(dst);
    dat->m_dataBlk = fs.writeData;
    WriteMask wm(cacheLineSize);
    wm.setMask(0, cacheLineSize);
    dat->m_bitMask = wm;
    dat->m_usesTxnId = true;
    dat->m_txnId = dbid;
    dat->m_MessageSize = MessageSizeType_Data;
    return sendDataMsg(dat);
}

bool
MetaRNFController::sendCompAck(uint64_t pa, MachineID dst)
{
    NetDest dest(m_ruby_system);
    dest.add(dst);
    auto rsp = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        pa, CHIResponseType_CompAck,
        m_machineID, dest,
        false, false, 0, 0, MessageSizeType_Control);
    return sendResponseMsg(rsp);
}

void
MetaRNFController::completeRead(int slotIdx, bool success,
                                const DataBlock *data)
{
    FlightSlot &fs = _flightSlots[slotIdx];
    auto cb = fs.readCb;
    uint64_t pa = fs.pa;

    MetaLine line{};
    if (success && data) {
        for (int i = 0; i < 64; ++i)
            line[i] = data->getByte(i);
    }

    _scoreboard.erase(pa);
    fs.reset();

    if (cb) cb(success, line);

    drainWaitQueue(pa);
}

void
MetaRNFController::completeWrite(int slotIdx, bool success)
{
    FlightSlot &fs = _flightSlots[slotIdx];
    auto cb = fs.writeCb;
    uint64_t pa = fs.pa;

    _scoreboard.erase(pa);
    fs.reset();

    if (cb) cb(success);

    drainWaitQueue(pa);
}

bool
MetaRNFController::recvRequestMsg(const CHIRequestMsg *msg)
{
    DPRINTF(RubyCHIGeneric,
            "MetaRNF node=%d recvRequest type=%d addr=0x%lx\n",
            _nodeId, static_cast<int>(msg->m_type), msg->m_addr);
    return true;
}

bool
MetaRNFController::recvSnoopMsg(const CHIRequestMsg *msg)
{
    DPRINTF(RubyCHIGeneric,
            "MetaRNF node=%d recvSnoop type=%d addr=0x%lx\n",
            _nodeId, static_cast<int>(msg->m_type), msg->m_addr);
    return true;
}

bool
MetaRNFController::recvResponseMsg(const CHIResponseMsg *msg)
{
    auto sbIt = _scoreboard.find(msg->m_addr);
    if (sbIt == _scoreboard.end())
        return true;

    int slot = sbIt->second;
    FlightSlot &fs = _flightSlots[slot];

    if (msg->m_type == CHIResponseType_RetryAck ||
        msg->m_type == CHIResponseType_PCrdGrant) {
        return true;
    }

    if (fs.op == OpType::Read) {
        if (msg->m_type == CHIResponseType_Comp_I ||
            msg->m_type == CHIResponseType_Comp_UC ||
            msg->m_type == CHIResponseType_Comp_SC) {
            completeRead(slot, false, nullptr);
            return true;
        }
        return true;
    }

    if (fs.op == OpType::Write || fs.op == OpType::Delete) {
        if (msg->m_type == CHIResponseType_CompDBIDResp ||
            msg->m_type == CHIResponseType_DBIDResp) {
            if (!sendWriteData(msg->m_addr, msg->m_responder, msg->m_dbid)) {
                completeWrite(slot, false);
                return true;
            }

            if (msg->m_type == CHIResponseType_CompDBIDResp) {
                completeWrite(slot, true);
            } else {
                fs.waitingCompAfterDbid = true;
            }
            return true;
        }
        if (msg->m_type == CHIResponseType_Comp &&
            fs.waitingCompAfterDbid) {
            completeWrite(slot, true);
            return true;
        }
        return true;
    }

    return true;
}

bool
MetaRNFController::recvDataMsg(const CHIDataMsg *msg)
{
    auto sbIt = _scoreboard.find(msg->m_addr);
    if (sbIt == _scoreboard.end())
        return true;

    int slot = sbIt->second;
    FlightSlot &fs = _flightSlots[slot];

    if (fs.op != OpType::Read)
        return true;

    if (msg->m_type != CHIDataType_CompData_I &&
        msg->m_type != CHIDataType_CompData_UC &&
        msg->m_type != CHIDataType_CompData_SC &&
        msg->m_type != CHIDataType_CompData_UD_PD &&
        msg->m_type != CHIDataType_CompData_SD_PD) {
        return true;
    }

    sendCompAck(msg->m_addr, msg->m_responder);
    DataBlock db = msg->getdataBlk();
    completeRead(slot, true, &db);
    return true;
}

} // namespace ruby
} // namespace gem5
