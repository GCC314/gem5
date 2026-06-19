#include "mem/ruby/protocol/chi/ep/MetaRNFController.hh"

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
    _requestInFlight(false)
{
    if (!p.downstream_destinations.empty()) {
        _hnfVersion = p.downstream_destinations[0]->getVersion();
    }
    // v4-dual-socket: register with (node_id, socket_id) key
    _instances[{_nodeId, p.socket_id}] = this;
}

MetaRNFController::~MetaRNFController()
{
    _instances.erase({_nodeId, 0});  // v4-dual-socket: socket_id not stored as member, but default 0
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
    out << "[MetaRNF node_id=" << _nodeId << " v=" << m_version << "]";
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
    if (!inMetadataRange(metadataPa) || _pending.count(metadataPa) != 0) {
        if (cb) {
            cb(false, zero);
        }
        return;
    }

    PendingTxn t;
    t.op = OpType::Read;
    t.pa = metadataPa;
    t.readCb = cb;
    _pending[metadataPa] = t;

    if (!sendReadOnce(metadataPa)) {
        _pending.erase(metadataPa);
        if (cb) {
            cb(false, zero);
        }
    }
}

void
MetaRNFController::issueWrite(uint64_t metadataPa, const MetaLine &line,
                              WriteCallback cb)
{
    if (!inMetadataRange(metadataPa) || _pending.count(metadataPa) != 0) {
        if (cb) {
            cb(false);
        }
        return;
    }

    PendingTxn t;
    t.op = OpType::Write;
    t.pa = metadataPa;
    t.writeCb = cb;
    t.writeData.setData(line.data(), 0, 64);
    _pending[metadataPa] = t;

    if (!sendWriteUnique(metadataPa)) {
        _pending.erase(metadataPa);
        if (cb) {
            cb(false);
        }
    }
}

void
MetaRNFController::issueDelete(uint64_t metadataPa, WriteCallback cb)
{
    MetaLine zero{};
    if (!inMetadataRange(metadataPa) || _pending.count(metadataPa) != 0) {
        if (cb) {
            cb(false);
        }
        return;
    }

    PendingTxn t;
    t.op = OpType::Delete;
    t.pa = metadataPa;
    t.writeCb = cb;
    t.writeData.setData(zero.data(), 0, 64);
    _pending[metadataPa] = t;

    if (!sendWriteUnique(metadataPa)) {
        _pending.erase(metadataPa);
        if (cb) {
            cb(false);
        }
    }
}

bool
MetaRNFController::sendReadOnce(uint64_t pa)
{
    if (_requestInFlight) {
        return false;
    }

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
        _requestInFlight = true;
    }
    return sent;
}

bool
MetaRNFController::sendWriteUnique(uint64_t pa)
{
    if (_requestInFlight) {
        return false;
    }

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
        _requestInFlight = true;
    }
    return sent;
}

bool
MetaRNFController::sendWriteData(uint64_t pa, MachineID dst, uint64_t dbid)
{
    auto it = _pending.find(pa);
    if (it == _pending.end()) {
        return false;
    }

    auto dat = std::make_shared<CHIDataMsg>(curTick(), cacheLineSize,
                                            m_ruby_system);
    dat->m_addr = pa;
    dat->m_type = CHIDataType_NCBWrData;
    dat->m_responder = m_machineID;
    dat->m_Destination.clear();
    dat->m_Destination.add(dst);
    dat->m_dataBlk = it->second.writeData;
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
MetaRNFController::completeRead(uint64_t pa, bool success, const DataBlock *data)
{
    auto it = _pending.find(pa);
    if (it == _pending.end()) {
        return;
    }

    MetaLine line{};
    if (success && data) {
        for (int i = 0; i < 64; ++i) {
            line[i] = data->getByte(i);
        }
    }
    auto cb = it->second.readCb;
    _pending.erase(it);
    _requestInFlight = false;
    if (cb) {
        cb(success, line);
    }
}

void
MetaRNFController::completeWrite(uint64_t pa, bool success)
{
    auto it = _pending.find(pa);
    if (it == _pending.end()) {
        return;
    }

    auto cb = it->second.writeCb;
    _pending.erase(it);
    _requestInFlight = false;
    if (cb) {
        cb(success);
    }
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
    auto it = _pending.find(msg->m_addr);
    if (it == _pending.end()) {
        return true;
    }

    if (msg->m_type == CHIResponseType_RetryAck ||
        msg->m_type == CHIResponseType_PCrdGrant) {
        return true;
    }

    if (it->second.op == OpType::Read) {
        if (msg->m_type == CHIResponseType_Comp_I ||
            msg->m_type == CHIResponseType_Comp_UC ||
            msg->m_type == CHIResponseType_Comp_SC) {
            completeRead(msg->m_addr, false, nullptr);
            return true;
        }
        return true;
    }

    if (it->second.op == OpType::Write || it->second.op == OpType::Delete) {
        if (msg->m_type == CHIResponseType_CompDBIDResp ||
            msg->m_type == CHIResponseType_DBIDResp) {
            if (!sendWriteData(msg->m_addr, msg->m_responder, msg->m_dbid)) {
                completeWrite(msg->m_addr, false);
                return true;
            }

            if (msg->m_type == CHIResponseType_CompDBIDResp) {
                completeWrite(msg->m_addr, true);
            } else {
                it->second.waitingCompAfterDbid = true;
            }
            return true;
        }
        if (msg->m_type == CHIResponseType_Comp &&
            it->second.waitingCompAfterDbid) {
            completeWrite(msg->m_addr, true);
            return true;
        }
        return true;
    }

    return true;
}

bool
MetaRNFController::recvDataMsg(const CHIDataMsg *msg)
{
    auto it = _pending.find(msg->m_addr);
    if (it == _pending.end()) {
        return true;
    }

    if (it->second.op != OpType::Read) {
        return true;
    }

    if (msg->m_type != CHIDataType_CompData_I &&
        msg->m_type != CHIDataType_CompData_UC &&
        msg->m_type != CHIDataType_CompData_SC &&
        msg->m_type != CHIDataType_CompData_UD_PD &&
        msg->m_type != CHIDataType_CompData_SD_PD) {
        return true;
    }

    sendCompAck(msg->m_addr, msg->m_responder);
    DataBlock db = msg->getdataBlk();
    completeRead(msg->m_addr, true, &db);
    return true;
}

} // namespace ruby
} // namespace gem5
