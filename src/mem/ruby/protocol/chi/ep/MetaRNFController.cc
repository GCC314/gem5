#include "mem/ruby/protocol/chi/ep/MetaRNFController.hh"

#include <algorithm>

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "debug/RubyEP.hh"
#include "mem/ruby/protocol/CHI/CHIProtocolInfo.hh"

namespace gem5
{

namespace ruby
{

using namespace CHI;

std::map<std::pair<int,int>, MetaRNFController*> MetaRNFController::_instances;

namespace
{

bool
tracePageOne(uint64_t pa)
{
    return pa >= 0x28000a00 && pa < 0x28000b00;
}

} // anonymous namespace

MetaRNFController::MetaRNFController(const Params &p)
  : EPController(p),
    _metadataRange(p.metadata_private_range),
    _hnfVersion(-1),
    _maxFlights(p.flight_slots)
{
    fatal_if(_maxFlights > kMaxLineFlightSlots,
             "MetaRNF node_id=%d: flight_slots=%d exceeds physical array [%d]",
             _nodeId, _maxFlights, kMaxLineFlightSlots);
    if (_maxFlights < 1) _maxFlights = 1;

    if (!p.downstream_destinations.empty()) {
        _hnfVersion = p.downstream_destinations[0]->getVersion();
    }

    for (int i = 0; i < kMaxLineFlightSlots; ++i)
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
    processPendingOutputs();
    completeDeferredReads();
    completeDeferredWrites();
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
        if (tracePageOne(metadataPa)) {
            inform(
                         "[META-TRACE] node=%d op=read-queue pa=0x%lx slot=%d active=%d\n",
                         _nodeId, metadataPa, sbIt->second, activeFlightCount());
        }
        QueuedOp q;
        q.op = OpType::Read;
        q.pa = metadataPa;
        q.readCb = cb;
        _waitQueues[metadataPa].push_back(q);
        return;
    }

    int slot = findFreeSlot();
    if (slot < 0) {
        if (tracePageOne(metadataPa)) {
            inform(
                         "[META-TRACE] node=%d op=read-no-slot pa=0x%lx active=%d\n",
                         _nodeId, metadataPa, activeFlightCount());
        }
        if (cb) cb(false, zero);
        return;
    }

    FlightSlot &fs = _flightSlots[slot];
    fs.state = SlotState::Allocated;
    fs.op = OpType::Read;
    fs.pa = metadataPa;
    fs.readCb = cb;
    _scoreboard[metadataPa] = slot;

    if (tracePageOne(metadataPa)) {
        inform(
                     "[META-TRACE] node=%d op=read-issue pa=0x%lx slot=%d active=%d\n",
                     _nodeId, metadataPa, slot, activeFlightCount());
    }

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
        if (tracePageOne(metadataPa)) {
            inform(
                         "[META-TRACE] node=%d op=write-queue pa=0x%lx slot=%d active=%d\n",
                         _nodeId, metadataPa, sbIt->second, activeFlightCount());
        }
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
        // Phase D7: queue writes when all flight slots are full.
        // Deduplicate by PA: replace older queued write for same PA.
        for (auto it = _pendingWrites.begin(); it != _pendingWrites.end(); ++it) {
            if (it->pa == metadataPa) {
                DPRINTF(RubyEP,
                    "[METARNF-WRITE-COALESCE] node=%d pa=0x%lx "
                    "queueDepth=%zu\n",
                    _nodeId, metadataPa, _pendingWrites.size());
                it->data = line;
                it->cb = cb;
                return;
            }
        }
        if ((int)_pendingWrites.size() >= kMaxPendingWrites) {
            // Queue full — backpressure caller
            if (cb) cb(false);
            return;
        }
        _pendingWrites.push_back({metadataPa, line, cb});
        int qd = (int)_pendingWrites.size();
        if (qd > _pendingWritesHighwater) _pendingWritesHighwater = qd;
        DPRINTF(RubyEP,
            "[METARNF-WRITE-QUEUE] node=%d pa=0x%lx depth=%d\n",
            _nodeId, metadataPa, qd);
        return;
    }

    FlightSlot &fs = _flightSlots[slot];
    fs.state = SlotState::Allocated;
    fs.op = OpType::Write;
    fs.pa = metadataPa;
    fs.writeCb = cb;
    fs.writeData.setData(line.data(), 0, 64);
    _scoreboard[metadataPa] = slot;

    if (tracePageOne(metadataPa)) {
        inform(
                     "[META-TRACE] node=%d op=write-issue pa=0x%lx slot=%d active=%d\n",
                     _nodeId, metadataPa, slot, activeFlightCount());
    }

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

// ---- Phase 2: 64B line read with typed status and bounded queues ----

void
MetaRNFController::issueReadLine(uint64_t linePa, LineReadCallback cb)
{
    MetaLine zero{};
    if (!inMetadataRange(linePa)) {
        ++_lineRangeErrors;
        if (cb) cb(MetaRNFLineStatus::RangeError, zero);
        return;
    }

    auto sbIt = _scoreboard.find(linePa);
    if (sbIt != _scoreboard.end()) {
        // Same-address already in flight: check per-address bound
        int perAddrCount = _perAddressPendingCount[linePa];
        if (perAddrCount >= kMaxLineOpsPerAddress) {
            if (cb) cb(MetaRNFLineStatus::RetryableBusy, zero);
            ++_lineOpsRejected;
            return;
        }
        PendingLineOp op;
        op.pa = linePa;
        op.readCb = cb;
        if ((int)_pendingLineOps.size() >= kMaxPendingLineOps) {
            if (cb) cb(MetaRNFLineStatus::RetryableBusy, zero);
            ++_lineOpsRejected;
            return;
        }
        _pendingLineOps.push_back(op);
        _perAddressPendingCount[linePa]++;
        int qd = (int)_pendingLineOps.size();
        if (qd > _pendingLineOpsHighwater) _pendingLineOpsHighwater = qd;
        return;
    }

    int slot = findFreeSlot();
    if (slot < 0) {
        // No flight slot: queue if bounded limits permit
        int perAddrCount = _perAddressPendingCount[linePa];
        if (perAddrCount >= kMaxLineOpsPerAddress) {
            if (cb) cb(MetaRNFLineStatus::RetryableBusy, zero);
            ++_lineOpsRejected;
            return;
        }
        if ((int)_pendingLineOps.size() >= kMaxPendingLineOps) {
            if (cb) cb(MetaRNFLineStatus::RetryableBusy, zero);
            ++_lineOpsRejected;
            return;
        }
        PendingLineOp op;
        op.pa = linePa;
        op.readCb = cb;
        _pendingLineOps.push_back(op);
        _perAddressPendingCount[linePa]++;
        int qd = (int)_pendingLineOps.size();
        if (qd > _pendingLineOpsHighwater) _pendingLineOpsHighwater = qd;
        return;
    }

    FlightSlot &fs = _flightSlots[slot];
    fs.state = SlotState::Allocated;
    fs.op = OpType::Read;
    fs.pa = linePa;
    fs.isLineOp = true;
    fs.lineReadCb = cb;
    _scoreboard[linePa] = slot;

    // Phase 2 line trace: DEBUG-gated only, not [META-TRACE].
    DPRINTF(RubyCHIGeneric,
            "[DEBUG-PHASE2] node=%d op=readLine-issue pa=0x%lx slot=%d active=%d\n",
            _nodeId, linePa, slot, activeFlightCount());

    if (!sendReadOnce(linePa)) {
        // Send failed (TBE full, port busy). Queue for later retry
        // instead of failing immediately. drainPendingLineOps will
        // retry when a flight slot or TBE becomes free.
        _scoreboard.erase(linePa);
        fs.reset();
        PendingLineOp op;
        op.pa = linePa;
        op.readCb = cb;
        op.isWrite = false;
        _pendingLineOps.push_back(op);
        _perAddressPendingCount[linePa]++;
        int qd = (int)_pendingLineOps.size();
        if (qd > _pendingLineOpsHighwater) _pendingLineOpsHighwater = qd;
    }
}

// ---- Phase 2: 64B line write with typed status and bounded queues ----

void
MetaRNFController::issueWriteLine(uint64_t linePa, const MetaLine &line,
                                  LineWriteCallback cb)
{
    if (!inMetadataRange(linePa)) {
        ++_lineRangeErrors;
        if (cb) cb(MetaRNFLineStatus::RangeError);
        return;
    }

    auto sbIt = _scoreboard.find(linePa);
    if (sbIt != _scoreboard.end()) {
        int perAddrCount = _perAddressPendingCount[linePa];
        if (perAddrCount >= kMaxLineOpsPerAddress) {
            if (cb) cb(MetaRNFLineStatus::RetryableBusy);
            ++_lineOpsRejected;
            return;
        }
        if ((int)_pendingLineOps.size() >= kMaxPendingLineOps) {
            if (cb) cb(MetaRNFLineStatus::RetryableBusy);
            ++_lineOpsRejected;
            return;
        }
        PendingLineOp op;
        op.pa = linePa;
        op.data = line;
        op.writeCb = cb;
        _pendingLineOps.push_back(op);
        _perAddressPendingCount[linePa]++;
        int qd = (int)_pendingLineOps.size();
        if (qd > _pendingLineOpsHighwater) _pendingLineOpsHighwater = qd;
        return;
    }

    int slot = findFreeSlot();
    if (slot < 0) {
        int perAddrCount = _perAddressPendingCount[linePa];
        if (perAddrCount >= kMaxLineOpsPerAddress) {
            if (cb) cb(MetaRNFLineStatus::RetryableBusy);
            ++_lineOpsRejected;
            return;
        }
        if ((int)_pendingLineOps.size() >= kMaxPendingLineOps) {
            if (cb) cb(MetaRNFLineStatus::RetryableBusy);
            ++_lineOpsRejected;
            return;
        }
        PendingLineOp op;
        op.pa = linePa;
        op.data = line;
        op.writeCb = cb;
        _pendingLineOps.push_back(op);
        _perAddressPendingCount[linePa]++;
        int qd = (int)_pendingLineOps.size();
        if (qd > _pendingLineOpsHighwater) _pendingLineOpsHighwater = qd;
        return;
    }

    FlightSlot &fs = _flightSlots[slot];
    fs.state = SlotState::Allocated;
    fs.op = OpType::Write;
    fs.pa = linePa;
    fs.isLineOp = true;
    fs.lineWriteCb = cb;
    fs.writeData.setData(line.data(), 0, 64);
    _scoreboard[linePa] = slot;

    // Phase 2 line trace: DEBUG-gated only, not [META-TRACE].
    DPRINTF(RubyCHIGeneric,
            "[DEBUG-PHASE2] node=%d op=writeLine-issue pa=0x%lx slot=%d active=%d\n",
            _nodeId, linePa, slot, activeFlightCount());

    if (!sendWriteUnique(linePa)) {
        // Send failed — queue for retry, don't fail immediately
        _scoreboard.erase(linePa);
        fs.reset();
        PendingLineOp op;
        op.pa = linePa;
        op.data = line;
        op.writeCb = cb;
        op.isWrite = true;
        _pendingLineOps.push_back(op);
        _perAddressPendingCount[linePa]++;
        int qd = (int)_pendingLineOps.size();
        if (qd > _pendingLineOpsHighwater) _pendingLineOpsHighwater = qd;
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

// Phase D7: drain queued writes when a flight slot frees up
void
MetaRNFController::drainPendingWrites()
{
    while (!_pendingWrites.empty()) {
        int slot = findFreeSlot();
        if (slot < 0) break;
        PendingWrite pw = _pendingWrites.front();
        _pendingWrites.pop_front();
        DPRINTF(RubyEP,
            "[METARNF-WRITE-DEQUEUE] node=%d pa=0x%lx depth=%zu\n",
            _nodeId, pw.pa, _pendingWrites.size());
        issueWrite(pw.pa, pw.data, pw.cb);
    }
}

bool
MetaRNFController::sendReadOnce(uint64_t pa)
{
    auto req = std::make_shared<CHIRequestMsg>(curTick(), cacheLineSize,
                                                m_ruby_system);
    req->m_addr = pa;
    // HN-F recognizes MetaRNF ReadOnce and caches it locally while leaving
    // MetaRNF out of dir_sharers; MetaRNF has no data-bearing cache copy.
    req->m_type = CHIRequestType_ReadOnce;
    req->m_requestor = m_machineID;
    req->m_accAddr = pa;
    req->m_accSize = cacheLineSize;
    req->m_allowRetry = true;
    req->m_MessageSize = MessageSizeType_Control;
    MachineID hnfId;
    hnfId.type = MachineType_Cache;
    hnfId.num = _hnfVersion;
    req->m_Destination.clear();
    req->m_Destination.add(hnfId);

    bool sent = sendRequestReliable(req, pa);
    if (tracePageOne(pa)) {
        inform(
                     "[META-TRACE] node=%d op=read-send pa=0x%lx sent=%d\n",
                     _nodeId, pa, sent ? 1 : 0);
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
    req->m_accAddr = pa;
    req->m_accSize = cacheLineSize;
    req->m_allowRetry = true;
    req->m_MessageSize = MessageSizeType_Control;
    MachineID hnfId;
    hnfId.type = MachineType_Cache;
    hnfId.num = _hnfVersion;
    req->m_Destination.clear();
    req->m_Destination.add(hnfId);

    bool sent = sendRequestReliable(req, pa);
    if (tracePageOne(pa)) {
        inform(
                     "[META-TRACE] node=%d op=write-send pa=0x%lx sent=%d\n",
                     _nodeId, pa, sent ? 1 : 0);
    }
    return sent;
}

bool
MetaRNFController::sendRequestReliable(CHIRequestMsgPtr msg, uint64_t pa)
{
    if (_pendingRequestSends.empty() && sendRequestMsg(msg)) {
        auto sbIt = _scoreboard.find(pa);
        if (sbIt != _scoreboard.end())
            _flightSlots[sbIt->second].state = SlotState::Sent;
        return true;
    }

    _pendingRequestSends.push_back({std::move(msg), pa});
    scheduleEvent(Cycles(1));
    return true;
}

void
MetaRNFController::sendWriteData(uint64_t pa, MachineID dst, uint64_t dbid,
                                 std::function<void()> onSent)
{
    auto sbIt = _scoreboard.find(pa);
    if (sbIt == _scoreboard.end())
        return;

    int slot = sbIt->second;
    FlightSlot &fs = _flightSlots[slot];

    for (int offset = 0; offset < cacheLineSize; offset += dataChannelSize) {
        const int bytes = std::min(dataChannelSize, cacheLineSize - offset);
        auto dat = std::make_shared<CHIDataMsg>(curTick(), cacheLineSize,
                                                 m_ruby_system);
        dat->m_addr = pa;
        dat->m_type = CHIDataType_NCBWrData;
        dat->m_responder = m_machineID;
        dat->m_Destination.clear();
        dat->m_Destination.add(dst);
        dat->m_dataBlk = fs.writeData;
        WriteMask wm(cacheLineSize);
        wm.setMask(offset, bytes);
        dat->m_bitMask = wm;
        dat->m_txnId = dbid;
        dat->m_MessageSize = MessageSizeType_Data;
        const bool lastBeat = offset + bytes >= cacheLineSize;
        sendDataReliable(dat, lastBeat ? onSent : nullptr);
    }
    if (tracePageOne(pa)) {
        inform(
                     "[META-TRACE] node=%d op=write-data pa=0x%lx dbid=%lu dst=%d queued=1\n",
                     _nodeId, pa, dbid, dst.num);
    }
}

void
MetaRNFController::sendCompAck(uint64_t pa, MachineID dst,
                               std::function<void()> onSent)
{
    NetDest dest(m_ruby_system);
    dest.add(dst);
    auto rsp = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        pa, CHIResponseType_CompAck,
        m_machineID, dest,
        false, false, 0, 0, MessageSizeType_Control);
    sendResponseReliable(rsp, std::move(onSent));
}

void
MetaRNFController::sendResponseReliable(CHIResponseMsgPtr msg,
                                        std::function<void()> onSent)
{
    if (_pendingResponseSends.empty() && sendResponseMsg(msg)) {
        if (onSent)
            onSent();
        return;
    }

    _pendingResponseSends.push_back({std::move(msg), std::move(onSent)});
    scheduleEvent(Cycles(1));
}

void
MetaRNFController::sendDataReliable(CHIDataMsgPtr msg,
                                    std::function<void()> onSent)
{
    if (_pendingDataSends.empty() && sendDataMsg(msg)) {
        if (onSent)
            onSent();
        return;
    }

    _pendingDataSends.push_back({std::move(msg), std::move(onSent)});
    scheduleEvent(Cycles(1));
}

void
MetaRNFController::processPendingOutputs()
{
    while (!_pendingRequestSends.empty()) {
        auto &pending = _pendingRequestSends.front();
        if (!sendRequestMsg(pending.msg))
            break;
        auto sbIt = _scoreboard.find(pending.pa);
        if (sbIt != _scoreboard.end())
            _flightSlots[sbIt->second].state = SlotState::Sent;
        _pendingRequestSends.pop_front();
    }

    while (!_pendingResponseSends.empty()) {
        auto &pending = _pendingResponseSends.front();
        if (!sendResponseMsg(pending.msg))
            break;
        auto onSent = std::move(pending.onSent);
        _pendingResponseSends.pop_front();
        if (onSent)
            onSent();
    }

    while (!_pendingDataSends.empty()) {
        auto &pending = _pendingDataSends.front();
        if (!sendDataMsg(pending.msg))
            break;
        auto onSent = std::move(pending.onSent);
        _pendingDataSends.pop_front();
        if (onSent)
            onSent();
    }

    if (!_pendingRequestSends.empty() || !_pendingResponseSends.empty() ||
        !_pendingDataSends.empty())
        scheduleEvent(Cycles(1));
}

void
MetaRNFController::completeRead(int slotIdx, bool success,
                                const DataBlock *data)
{
    FlightSlot &fs = _flightSlots[slotIdx];
    uint64_t pa = fs.pa;

    if (fs.isLineOp) {
        MetaRNFLineStatus st = success ? MetaRNFLineStatus::Ok
                                        : MetaRNFLineStatus::IoError;
        completeReadLine(slotIdx, st, success ? data : nullptr);
        return;
    }

    auto cb = fs.readCb;

    if (tracePageOne(pa)) {
        inform(
                     "[META-TRACE] node=%d op=read-complete pa=0x%lx slot=%d success=%d\n",
                     _nodeId, pa, slotIdx, success ? 1 : 0);
    }

    MetaLine line{};
    if (success && data) {
        for (int i = 0; i < 64; ++i)
            line[i] = data->getByte(i);
    }

    _scoreboard.erase(pa);
    fs.reset();

    if (cb) cb(success, line);

    drainWaitQueue(pa);
    drainPendingWrites();  // Phase D7
}

void
MetaRNFController::completeReadLine(int slotIdx, MetaRNFLineStatus st,
                                    const DataBlock *data)
{
    FlightSlot &fs = _flightSlots[slotIdx];
    auto cb = fs.lineReadCb;
    uint64_t pa = fs.pa;

    MetaLine line{};
    if (st == MetaRNFLineStatus::Ok && data) {
        for (int i = 0; i < 64; ++i)
            line[i] = data->getByte(i);
    }

    _scoreboard.erase(pa);
    // NOTE: _perAddressPendingCount is NOT decremented here.
    // It is decremented only in drainPendingLineOps() when a queued item
    // for this PA is actually issued. The count tracks items still in the
    // pending queue, not completed flights.
    fs.reset();

    if (cb) cb(st, line);

    drainPendingLineOps();
    drainPendingWrites();  // Legacy path may also drain
}

void
MetaRNFController::completeWrite(int slotIdx, bool success)
{
    FlightSlot &fs = _flightSlots[slotIdx];
    uint64_t pa = fs.pa;

    if (fs.isLineOp) {
        MetaRNFLineStatus st = success ? MetaRNFLineStatus::Ok
                                        : MetaRNFLineStatus::IoError;
        completeWriteLine(slotIdx, st);
        return;
    }

    auto cb = fs.writeCb;

    if (tracePageOne(pa)) {
        inform(
                     "[META-TRACE] node=%d op=write-complete pa=0x%lx slot=%d success=%d\n",
                     _nodeId, pa, slotIdx, success ? 1 : 0);
    }

    _scoreboard.erase(pa);
    fs.reset();

    if (cb) cb(success);

    drainWaitQueue(pa);
    drainPendingWrites();  // Phase D7
}

void
MetaRNFController::completeWriteLine(int slotIdx, MetaRNFLineStatus st)
{
    FlightSlot &fs = _flightSlots[slotIdx];
    auto cb = fs.lineWriteCb;
    uint64_t pa = fs.pa;

    _scoreboard.erase(pa);
    // NOTE: _perAddressPendingCount is NOT decremented here.
    // See completeReadLine for rationale.
    fs.reset();

    if (cb) cb(st);

    drainPendingLineOps();
    drainPendingWrites();  // Legacy path may also drain
}

// Phase 2: drain pending line ops (per-PA FIFO, bounded).
//
// Scans the queue once: issues every ready item (PA not in scoreboard) while
// flight slots remain. Items blocked on a PA that is still in-flight are
// re-queued at the tail so that unrelated PAs can proceed. Same-PA FIFO is
// preserved because items for a single PA stay consecutive in the queue and
// are only re-queued when all of them are blocked.
void
MetaRNFController::drainPendingLineOps()
{
    if (_pendingLineOps.empty()) return;

    std::deque<PendingLineOp> remaining;
    int issued = 0;

    while (!_pendingLineOps.empty()) {
        int slot = findFreeSlot();
        if (slot < 0) {
            // No more flight slots: re-queue everything that's left.
            remaining.insert(remaining.end(),
                             std::make_move_iterator(_pendingLineOps.begin()),
                             std::make_move_iterator(_pendingLineOps.end()));
            break;
        }

        PendingLineOp op = std::move(_pendingLineOps.front());
        _pendingLineOps.pop_front();

        if (_scoreboard.find(op.pa) != _scoreboard.end()) {
            // PA is still in-flight — keep this op in the backlog.
            remaining.push_back(std::move(op));
            continue;
        }

        // Ready to issue: decrement per-address pending count.
        auto pcIt = _perAddressPendingCount.find(op.pa);
        if (pcIt != _perAddressPendingCount.end() && pcIt->second > 0) {
            pcIt->second--;
            if (pcIt->second == 0)
                _perAddressPendingCount.erase(pcIt);
        }

        if (op.readCb) {
            issueReadLine(op.pa, op.readCb);
        } else if (op.writeCb) {
            issueWriteLine(op.pa, op.data, op.writeCb);
        }
        ++issued;
    }

    _pendingLineOps = std::move(remaining);

    if (issued > 0) {
        DPRINTF(RubyCHIGeneric,
                "MetaRNF node=%d: drainPendingLineOps issued=%d remaining=%zu\n",
                _nodeId, issued, _pendingLineOps.size());
    }
}

void
MetaRNFController::completeDeferredReads()
{
    const Tick now = curTick();
    for (auto it = _deferredReadCompletions.begin();
         it != _deferredReadCompletions.end();) {
        if (it->ready > now) {
            ++it;
            continue;
        }

        const int slot = it->slot;
        DataBlock data = it->data;
        it = _deferredReadCompletions.erase(it);
        if (_flightSlots[slot].state != SlotState::Free)
            completeRead(slot, true, &data);
    }
}

void
MetaRNFController::completeDeferredWrites()
{
    const Tick now = curTick();
    for (auto it = _deferredWriteCompletions.begin();
         it != _deferredWriteCompletions.end();) {
        if (it->second > now) {
            ++it;
            continue;
        }

        const int slot = it->first;
        it = _deferredWriteCompletions.erase(it);
        if (_flightSlots[slot].state != SlotState::Free)
            completeWrite(slot, true);
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
    auto sbIt = _scoreboard.find(msg->m_addr);
    if (sbIt == _scoreboard.end())
        return true;

    int slot = sbIt->second;
    FlightSlot &fs = _flightSlots[slot];

    if (tracePageOne(msg->m_addr)) {
        inform(
                     "[META-TRACE] node=%d op=recv-rsp pa=0x%lx slot=%d type=%d flight-op=%d\n",
                     _nodeId, msg->m_addr, slot, static_cast<int>(msg->m_type),
                     static_cast<int>(fs.op));
    }

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
            if (msg->m_type == CHIResponseType_CompDBIDResp) {
                sendWriteData(msg->m_addr, msg->m_responder, msg->m_dbid,
                    [this, slot]() {
                        if (_flightSlots[slot].state != SlotState::Free)
                            completeWrite(slot, true);
                    });
            } else {
                fs.waitingCompAfterDbid = true;
                sendWriteData(msg->m_addr, msg->m_responder, msg->m_dbid,
                              nullptr);
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

    if (tracePageOne(msg->m_addr)) {
        inform(
                     "[META-TRACE] node=%d op=recv-data pa=0x%lx slot=%d type=%d flight-op=%d\n",
                     _nodeId, msg->m_addr, slot, static_cast<int>(msg->m_type),
                     static_cast<int>(fs.op));
    }

    if (fs.op != OpType::Read)
        return true;

    if (msg->m_type != CHIDataType_CompData_I &&
        msg->m_type != CHIDataType_CompData_UC &&
        msg->m_type != CHIDataType_CompData_SC &&
        msg->m_type != CHIDataType_CompData_UD_PD &&
        msg->m_type != CHIDataType_CompData_SD_PD) {
        return true;
    }

    const WriteMask &mask = msg->m_bitMask;
    fatal_if(fs.readValid.isOverlap(mask),
             "MetaRNF node=%d: duplicate CompData bytes for %#x",
             _nodeId, msg->m_addr);
    fs.readData.copyPartial(msg->m_dataBlk, mask);
    fs.readValid.orMask(mask);
    if (!fs.readValid.isFull())
        return true;

    // One 64B CHI transaction completes only after every data-channel flit
    // has arrived. The HN-F expects exactly one CompAck for that transaction.
    DataBlock db = fs.readData;
    sendCompAck(msg->m_addr, msg->m_responder,
        [this, slot, db]() {
            if (_flightSlots[slot].state != SlotState::Free)
                completeRead(slot, true, &db);
        });
    return true;
}

} // namespace ruby
} // namespace gem5
