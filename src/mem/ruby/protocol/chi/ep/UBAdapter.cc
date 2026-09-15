#include "mem/ruby/protocol/chi/ep/UBAdapter.hh"

#include <algorithm>
#include <chrono>
#include <limits>
#include <thread>

#include "base/logging.hh"
#include "debug/RubyEP.hh"
#include "framework/iface/Message.hh"
#include "framework/iface/Port.hh"
#include "protocol/TracePerfPolicy.hh"
#include "mem/ruby/protocol/chi/ep/EPBackend.hh"
#include "mem/ruby/protocol/chi/ep/MetaRNFController.hh"
#include "sim/core.hh"
#include "sim/cur_tick.hh"
#include "sim/sync_wait.hh"
#include "sim/system.hh"

namespace gem5
{
namespace ruby
{

std::vector<UBAdapter *> UBAdapter::_clockAdapters;
UBAdapter *UBAdapter::_clockOwner = nullptr;
bool UBAdapter::_clockPumpRunning = false;

namespace
{

const CoherenceMessage *
coherencePayload(const framework::Message *message)
{
    if (!message || framework::GetMessagePayloadSize(message) !=
            sizeof(CoherenceMessage)) {
        return nullptr;
    }
    const void *payload = framework::GetMessagePayloadData(message);
    return payload ? reinterpret_cast<const CoherenceMessage *>(payload) :
                     nullptr;
}

bool
sendCoherenceMessage(framework::Port *port, const CoherenceMessage &coherence,
                      uint64_t timestamp, uint64_t requestId,
                      uint32_t sourceId, uint32_t targetId,
                      uint64_t *wireTimestamp = nullptr)
{
    if (!port)
        return false;

    framework::Message *message =
        framework::AllocateSendMessage(port, timestamp);
    if (!message)
        return false;

    framework::SetMessageRequestId(message, requestId);
    framework::SetMessageSourceId(message, sourceId);
    framework::SetMessageTargetId(message, targetId);
    framework::SetMessagePayload(message, &coherence, sizeof(coherence));
    if (wireTimestamp)
        *wireTimestamp = framework::GetMessageTimestamp(message);
    return framework::SendMessage(port, message);
}

} // anonymous namespace

UBAdapter::UBAdapter(const Params &p)
    : SimObject(p),
      _nodeId(p.node_id),
      _socketId(p.socket_id),
      _numNodes(p.num_nodes),
      _numSockets(p.num_sockets),
      _localNode(p.local_node),
      _addrMap(p.num_nodes, p.num_sockets, 128ULL * 1024 * 1024),
      _responseCheckEvent([this]{ wakeup(); }, name() + ".responseCheck"),
      _deferredControls(p.num_nodes * p.num_sockets * 4),
      _controlReceipts(p.num_nodes * p.num_sockets),
      _controlOutputs(new std::array<LeasedControlOutput, 4>[p.num_nodes * p.num_sockets]{}),
      _permissionReplies(new MetadataReply[p.num_nodes * p.num_sockets * 64]{})
{
    fatal_if(sizeof(CoherenceMessage) > framework::GetMaxPayloadSize(),
              "UBAdapter: CoherenceMessage size (%zu) exceeds framework payload (%zu)",
              sizeof(CoherenceMessage), framework::GetMaxPayloadSize());
    DPRINTF(RubyEP, "UBAdapter node=%d socket=%d created\n", _nodeId, _socketId);
    inform("[EP-CONTROL-STORAGE] node=%d socket=%d peers=%u slots=%lu input_bytes=%lu output_bytes=%lu lease_bytes=%lu response_bank_bytes=%lu progress_bytes=%lu ordinary_output_bytes=%lu",
           _nodeId, _socketId, _controlReceipts.peerCount(),
           static_cast<unsigned long>(_deferredControls.capacity()),
           static_cast<unsigned long>(_deferredControls.capacity() * sizeof(cc::glob::ControlFrame)),
           static_cast<unsigned long>(_controlReceipts.peerCount() * 4 * sizeof(LeasedControlOutput)),
           static_cast<unsigned long>(_controlReceipts.storageBytes()),
           static_cast<unsigned long>(sizeof(_readyResponses)),
           static_cast<unsigned long>(sizeof(_clearOutputs) + sizeof(_haAckOutputs)),
           static_cast<unsigned long>(sizeof(_reliableOutputs)));
}

UBAdapter::~UBAdapter()
{
    unregisterClockAdapter(this);
    if (_port) {
        if (_portExitState && !_portExitState->terminated) {
            framework::TerminatePort(_port);
            _portExitState->terminated = true;
        }
        if (_portExitState)
            _portExitState->port = nullptr;
        framework::DestroyPort(_port);
        _port = nullptr;
    }
}

void
UBAdapter::init()
{
    SimObject::init();

    // Phase 1: Store SimObject param into file-local static.
    // Param always takes effect (no env fallback).

    // Create Port directly (guarantees all nodes get one regardless of init order).
    // _localNode selects which node this process owns (-1 = all nodes bind, legacy
    // single-process mode). Port binds when this node is enabled.
    {
        int enableNode = _localNode;
        if (enableNode < 0 || _nodeId == enableNode) {
            // Socket-plane model: each (node, socket) UBAdapter binds to its own
            // ubio process, identified by the global module id gid=node*K+socket.
            // Previously all sockets of a node used gem5UbioPort(node), so only
            // socket 0 bound (duplicate endpoint) and socket-1 traffic deadlocked.
            int gid = _nodeId * _numSockets + _socketId;
            framework::PortConfig config;
            config.selfRole = "gem5";
            config.peerRole = "ubio";
            config.channelName = "coherence";
            config.nodeId = _nodeId;
            config.socketId = _socketId;
            config.numNodes = _numNodes;
            config.numSockets = _numSockets;
            _port = framework::CreatePort(config);
            if (!_port) {
                warn("[UBAdapter] node=%d socket=%d Port init failed",
                     _nodeId, _socketId);
            } else {
                inform("[Port gem5_ubio] n=%d s=%d gid=%d channel=coherence",
                       _nodeId, _socketId, gid);
                inform("STEP5 Port enabled node=%d socket=%d gid=%d",
                       _nodeId, _socketId, gid);
                registerClockAdapter(this);
                // Multi-process split: when this gem5 node's simulation ends
                // (process exit), notify ubio with a best-effort TERMINATE so
                // the distributed clock treats this node as "done" (+inf) rather
                // than a frozen peer. Otherwise a node that finishes early (e.g.
                // an idle node) would cap min(safeTs) forever and freeze the
                // still-running nodes. See Port::safeTs PEER_LOST handling.
                _portExitState = std::make_shared<PortExitState>();
                _portExitState->port = _port;
                auto exitState = _portExitState;
                int nodeForLog = _nodeId;
                registerExitCallback([exitState, nodeForLog]() {
                    if (!exitState->port || exitState->terminated)
                        return;
                    inform("[UBADAPTER-EXIT] node=%d sending TERMINATE to ubio",
                           nodeForLog);
                    framework::TerminatePort(exitState->port);
                    exitState->terminated = true;
                });

                // Multi-process split: register cross-node barrier callback
                // with the System's SyncWaitManager. When the workload calls
                // sync_wait with a mask spanning other nodes, the SyncWaitManager
                // calls our callback to send BARRIER_REACHED to ubio (which
                // forwards to the barrier_manager / other ubios). We receive
                // BARRIER_RELEASE in wakeup() and call releaseBarrier().
                // Per-socket barrier: each socket independently registers
                // with SyncWaitManager. When the local barrier completes, all
                // sockets fire BarrierReached with their own barrierBit
                // (node*numSockets + socket).
                int localNode = _localNode;
                if (localNode >= 0 && !System::systemList.empty()) {
                    int barrierBit = _nodeId * _numSockets + _socketId;
                    System *sys = System::systemList[0];
                    sys->syncWait.registerSocket(_socketId, barrierBit);
                    sys->syncWait.registerSocketFn(_socketId,
                        [this](uint32_t mask, uint32_t srcBit, uint32_t seq) {
                            sendBarrierReached(mask, srcBit, seq);
                        });
                    DPRINTF(RubyEP,
                        "[DEBUG-UBADAPTER-BARRIER] node=%d socket=%d "
                        "registered IPC barrier callback (barrierBit=%d)\n",
                        _nodeId, _socketId, barrierBit);
                }
            }
        }
    }

}

void
UBAdapter::sendBarrierReached(uint32_t mask, uint32_t srcBit, uint32_t seq)
{
    if (!_port) return;
    // Barrier is carried as a PAYLOAD CoherenceMessage (BarrierReached); the
    // transport layer no longer has a dedicated BARRIER_REACHED type.
    CoherenceMessage bmsg;
    bmsg.h.type = CoherenceMessageType::BarrierReached;
    bmsg.h.srcNode = static_cast<uint16_t>(_nodeId);
    bmsg.h.srcSocket = static_cast<uint16_t>(_socketId);
    bmsg.h.dstNode = static_cast<uint16_t>(_nodeId);
    bmsg.h.dstSocket = static_cast<uint16_t>(_socketId);
    bmsg.b.barrier.mask = mask;
    bmsg.b.barrier.seq = seq;   // TC90 fix: barrier generation
    const uint32_t sourceGid =
        static_cast<uint32_t>(_nodeId * _numSockets + _socketId);
    if (srcBit != sourceGid) {
        warn("[UBADAPTER-BARRIER] node=%d socket=%d registeredBit=%u "
             "sourceGid=%u; using adapter GID for transport envelope",
             _nodeId, _socketId, srcBit, sourceGid);
    }
    // First hop is always gem5 -> its local UBIO plane. A non-leader UBIO
    // rewrites both envelope endpoints before forwarding through networksim.
    bool ok = sendCoherenceMessage(
        _port, bmsg, curTick(), mask, sourceGid, sourceGid);
    if (!ok)
        warn("[UBADAPTER-BARRIER] node=%d sendBarrierReached failed mask=0x%x seq=%u",
             _nodeId, mask, seq);
    DPRINTF(RubyEP,
        "[DEBUG-UBADAPTER-BARRIER-SEND] node=%d mask=0x%x seq=%u ok=%d\n",
        _nodeId, mask, seq, ok);
}

void
UBAdapter::startup()
{
    SimObject::startup();

    if (_port) {
        if (this == _clockOwner)
            armClockPump(curTick());
        DPRINTF(RubyEP,
                "[DEBUG-UBADAPTER-STARTUP] node=%d socket=%d schedule sync wakeup @%lu\n",
                _nodeId, _socketId, curTick());
        // Startup diagnostic: confirm this socket-plane adapter is alive and
        // its Port is bound. Paired with STEP5 log from init() so the launcher
        // can verify every (node,socket) plane before starting ubio peers.
        inform("[UBADAPTER-STARTUP] node=%d socket=%d port=%s armed=%d curTick=%lu",
               _nodeId, _socketId, _port ? "bound" : "MISSING",
               _clockOwner && _clockOwner->_eventArmed ? 1 : 0, curTick());
    } else if (!_port) {
        warn("[UBADAPTER-STARTUP] node=%d socket=%d NO PORT — adapter will not poll for messages",
             _nodeId, _socketId);
    }
}

bool
UBAdapter::reserveResponse(const CoherenceMessage &msg, bool cancel)
{
    CoherenceMessageType response;
    switch (msg.h.type) {
      case CoherenceMessageType::ReadReq: response = CoherenceMessageType::ReadResp; break;
      case CoherenceMessageType::WritebackReq: response = CoherenceMessageType::WritebackResp; break;
      case CoherenceMessageType::EvictReq:
        if (msg.b.evictReq.receiptAck) return true;
        response = CoherenceMessageType::EvictResp; break;
      case CoherenceMessageType::UpgradeReq: response = CoherenceMessageType::UpgradeResp; break;
      case CoherenceMessageType::UpgradeDoneReq: response = CoherenceMessageType::UpgradeDoneResp; break;
      case CoherenceMessageType::ClearReq:
        if (msg.b.clearReq.reason == 1) return true;
        response = CoherenceMessageType::ClearResp; break;
      case CoherenceMessageType::QueryLineMetaReq: response = CoherenceMessageType::QueryLineMetaResp; break;
      case CoherenceMessageType::HAPermissionReq: response = CoherenceMessageType::HAPermissionResp; break;
      case CoherenceMessageType::HAPresenceProbeReq: response = CoherenceMessageType::HAPresenceProbeResp; break;
      default: return true;
    }
    if (cancel) {
        _readyResponses.erase(PendingKey{response, msg.h.reqId});
        return true;
    }
    return _readyResponses.reserve({response, msg.h.reqId}, msg, 64);
}

void
UBAdapter::cacheResponse(const CoherenceMessage &msg)
{
    if (msg.h.type == CoherenceMessageType::ReadResp &&
        (msg.h.flags & static_cast<uint32_t>(CFLAG_DATA_FORWARDED))) {
        for (auto &child : _directChildren) {
            const auto &r = child.request;
            if (!child.live || r.reqId != msg.h.reqId ||
                r.homeLinePa != msg.h.homeLinePa || r.dstNode != msg.h.homeNode ||
                r.dstSocket != msg.h.homeSocket || r.srcNode != msg.h.dstNode ||
                r.srcSocket != msg.h.dstSocket) continue;
            // Provisional data is not permission. Home must subsequently
            // authenticate its source/epoch before this child can be consumed.
            if (!child.ready) {
                child.dataHeader = msg.h;
                memcpy(child.data.data(), msg.b.readResp.grantData, 64);
                child.ready = true;
                DPRINTF(RubyEP, "[C4-CHILD-RECEIVED] reqId=%lu owner=%u home=%u permission=0\n",
                        msg.h.reqId, msg.h.srcNode, msg.h.homeNode);
            }
            return;
        }
        return; // retired parent or wrong route cannot allocate a child
    }
    const bool delivered = _readyResponses.deliver({msg.h.type, msg.h.reqId}, msg);
    if (delivered && _backend &&
        _backend->boundaryTransactions().creditUsage(BoundaryPool::Ordinary) ==
        _backend->boundaryTransactions().demandLimit()) {
        inform("[EP-CREDIT-FULL-RESPONSE] node=%d socket=%d reqId=%lu type=%s ordinary=%u reserved=1",
               _nodeId, _socketId, msg.h.reqId, coherenceMsgTypeName(msg.h.type),
               _backend->boundaryTransactions().demandLimit());
    }
    if (!delivered) {
        DPRINTF(RubyEP, "[RSP-NO-RESERVATION] type=%s reqId=%lu pa=%#lx\n",
                coherenceMsgTypeName(msg.h.type), msg.h.reqId, msg.h.homeLinePa);
    }
}

bool
UBAdapter::transportSend(const CoherenceMessage &msg)
{
    if (!reserveResponse(msg)) return false;
    if (_port) {
        uint64_t sendTs = 0;
        if (!sendCoherenceMessage(_port, msg, curTick(), msg.h.reqId,
                                  static_cast<uint32_t>(
                                      _nodeId * _numSockets + _socketId),
                                  static_cast<uint32_t>(
                                      msg.h.dstNode * _numSockets +
                                      msg.h.dstSocket),
                                  &sendTs)) {
            warn("UBAdapter node=%d socket=%d: transportSend port send failed (reqId=%lu)",
                 _nodeId, _socketId, msg.h.reqId);
            // Retried Read/Clear/Upgrade may still have an older wire copy
            // outstanding. Its promise must survive a failed retransmission.
            if (msg.h.type == CoherenceMessageType::QueryLineMetaReq ||
                msg.h.type == CoherenceMessageType::UpgradeDoneReq ||
                msg.h.type == CoherenceMessageType::EvictReq)
                reserveResponse(msg, true);
            return false;
        }

        static int _tscount = 0;
        if (msg.h.type == CoherenceMessageType::ReadReq && ++_tscount <= 3)
            inform("[GEM5-SEND] node=%d type=ReadReq reqId=%lu gem5_tick=%lu buf_ts=%lu",
                   _nodeId, msg.h.reqId, curTick(), sendTs);
        if (TracePerfPolicy::get().shouldEmit("gem5")) {
            inform("[TRACE-PERF] %lu|%d|gem5|%lu|0x%lx|SEND|%s|dst=%d",
                   sendTs, _nodeId, msg.h.reqId, msg.h.homeLinePa,
                   coherenceMsgTypeName(msg.h.type), msg.h.dstNode);
        }
        if (msg.h.type == CoherenceMessageType::UpgradeReq) {
            inform("[UPGRADE-FORENSIC] stage=GEM5_REQ_SEND node=%d socket=%d "
                   "pa=0x%lx reqId=%lu epoch=%lu dst=%d:%d simTs=%lu",
                   _nodeId, _socketId, msg.h.homeLinePa, msg.h.reqId,
                   msg.h.epoch, msg.h.dstNode, msg.h.dstSocket, sendTs);
        }
        return true;
    }

    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: transportSend called with no router bound\n",
              _nodeId, _socketId);
    }
    fatal("transportSend: no port (router removed)");
    return true;
}

bool
UBAdapter::transportSendReliable(const CoherenceMessage &msg)
{
    if (msg.h.type == CoherenceMessageType::RecallResp ||
        msg.h.type == CoherenceMessageType::InvalidateAck ||
        msg.h.type == CoherenceMessageType::HAPresenceProbeResp)
        return sendControlReply(msg);
    if ((msg.h.type == CoherenceMessageType::ClearReq && msg.b.clearReq.reason == 1) ||
        msg.h.type == CoherenceMessageType::HAPermissionAck) {
        if (msg.h.type == CoherenceMessageType::HAPermissionAck) {
            for (auto &out : _haAckOutputs) {
                const auto &h = out.reply.h;
                if (!out.reserved || h.reqId != msg.h.reqId ||
                    h.homeLinePa != msg.h.homeLinePa || h.dstNode != msg.h.dstNode ||
                    h.dstSocket != msg.h.dstSocket) continue;
                if (!out.queued) { out.reply = msg; out.queued = true; }
                scheduleResponseCheck();
                return true;
            }
            return false;
        }
        auto &outputs = msg.h.type == CoherenceMessageType::ClearReq ?
            _clearOutputs : _haAckOutputs;
        for (const auto &out : outputs)
            if (out.queued && out.reply.h.reqId == msg.h.reqId &&
                out.reply.h.homeLinePa == msg.h.homeLinePa) return true;
        for (auto &out : outputs) {
            if (out.queued) continue;
            out.reply = msg; out.queued = true;
            scheduleResponseCheck();
            return true;
        }
        return false;
    }
    if (!reserveResponse(msg)) return false;
    for (const auto &queued : _reliableOutputs) {
        if (queued.h.type == msg.h.type && queued.h.reqId == msg.h.reqId &&
            queued.h.epoch == msg.h.epoch && queued.h.homeLinePa == msg.h.homeLinePa &&
            queued.h.dstNode == msg.h.dstNode && queued.h.dstSocket == msg.h.dstSocket)
            return true;
    }
    if (!_reliableOutputs.empty()) {
        if (!_reliableOutputs.push_back(msg)) return false;
        scheduleResponseCheck();
        return true;
    }
    if (transportSend(msg)) {
        if (_backend && msg.h.type == CoherenceMessageType::ClearReq &&
            msg.b.clearReq.reason == 1) {
            _backend->notifyOneWayClearHandedOff(
                msg.h.homeLinePa, _socketId, msg.h.reqId);
        }
        return true;
    }
    if (!_reliableOutputs.push_back(msg)) return false;
    scheduleResponseCheck();
    return true;
}

void
UBAdapter::drainReliableOutputs()
{
    while (!_directOutputs.empty()) {
        if (!transportSend(*_directOutputs.begin())) break;
        _directOutputs.erase(_directOutputs.begin());
    }
    for (auto &out : _metadataReplies)
        if (out.ready && transportSend(out.message)) out = {};
    for (auto &out : _pageReplies)
        if (out.ready && transportSend(out.message)) out = {};
    for (unsigned i = 0; i < _numNodes * _numSockets * 64; ++i) {
        auto &out = _permissionReplies[i];
        if (out.ready && transportSend(out.message)) out = {};
    }
    for (auto &out : _haAckOutputs) {
        if (!out.queued || !transportSend(out.reply)) continue;
        const auto h = out.reply.h;
        out = {};
        if (_backend) _backend->notifyHAAckHandedOff(h.homeLinePa, h.reqId, _socketId);
    }
    for (auto &out : _clearOutputs) {
        if (!out.queued || !transportSend(out.reply)) continue;
        out.queued = false;
        if (_backend) _backend->notifyOneWayClearHandedOff(
            out.reply.h.homeLinePa, _socketId, out.reply.h.reqId);
    }
    for (unsigned peer = 0; peer < _controlReceipts.peerCount(); ++peer) {
        for (unsigned slot = 0; slot < 4; ++slot) {
            auto &out = _controlOutputs[peer][slot];
            if (!out.queued || !transportSend(out.reply.expand())) continue;
            out.queued = false;
            _controlReceipts.complete(peer, out.reply.h.seqNum);
        }
    }
    while (!_reliableOutputs.empty()) {
        const CoherenceMessage message = *_reliableOutputs.begin();
        if (!transportSend(message))
            break;
        _reliableOutputs.erase(_reliableOutputs.begin());
        if (_backend && message.h.type == CoherenceMessageType::ClearReq &&
            message.b.clearReq.reason == 1) {
            _backend->notifyOneWayClearHandedOff(
                message.h.homeLinePa, _socketId, message.h.reqId);
        }
    }
}

bool
UBAdapter::reserveMetadataReply(const CoherenceMessage &request)
{
    auto &outputs = (request.h.type == CoherenceMessageType::MetaRNFReadReq ||
        request.h.type == CoherenceMessageType::MetaRNFWriteReq) ?
        _pageReplies : _metadataReplies;
    for (const auto &out : outputs)
        if (out.reserved && out.message.h.reqId == request.h.reqId &&
            (out.ready ? out.message.h.dstNode : out.message.h.srcNode) == request.h.srcNode &&
            (out.ready ? out.message.h.dstSocket : out.message.h.srcSocket) == request.h.srcSocket)
            return false; // exact retransmission joins the admitted callback
    for (auto &out : outputs) {
        if (out.reserved) continue;
        out.message.h = request.h;
        out.reserved = true;
        return true;
    }
    return false;
}

bool
UBAdapter::sendMetadataReply(const CoherenceMessage &reply)
{
    auto &outputs = (reply.h.type == CoherenceMessageType::MetaRNFReadResp ||
        reply.h.type == CoherenceMessageType::MetaRNFWriteResp) ?
        _pageReplies : _metadataReplies;
    for (auto &out : outputs) {
        if (!out.reserved || out.message.h.reqId != reply.h.reqId ||
            (out.ready ? out.message.h.dstNode : out.message.h.srcNode) != reply.h.dstNode ||
            (out.ready ? out.message.h.dstSocket : out.message.h.srcSocket) != reply.h.dstSocket)
            continue;
        out.message = reply;
        out.ready = true;
        scheduleResponseCheck();
        return true;
    }
    return false;
}

bool
UBAdapter::sendControlReply(CoherenceMessage msg)
{
    const unsigned peer = msg.h.dstNode * _numSockets + msg.h.dstSocket;
    if (peer >= _controlReceipts.peerCount()) return false;
    for (unsigned slot = 0; slot < 4; ++slot) {
        const auto *receipt = _controlReceipts.get(peer, slot);
        const auto &h = receipt->header;
        if (!receipt->active || h.reqId != msg.h.reqId ||
            h.epoch != msg.h.epoch || h.homeLinePa != msg.h.homeLinePa)
            continue;
        const bool typeMatches =
            (h.type == CoherenceMessageType::RecallReq && msg.h.type == CoherenceMessageType::RecallResp) ||
            (h.type == CoherenceMessageType::InvalidateReq && msg.h.type == CoherenceMessageType::InvalidateAck) ||
            (h.type == CoherenceMessageType::HAPresenceProbeReq && msg.h.type == CoherenceMessageType::HAPresenceProbeResp);
        if (!typeMatches) continue;
        auto &out = _controlOutputs[peer][slot];
        msg.h.seqNum = h.seqNum;
        if (out.queued) return true;
        // The receive lease reserved this exact output cell before invoking
        // native callbacks. Ordinary/output congestion cannot reject it.
        out.reply = cc::glob::ControlReply(msg);
        out.queued = true;
        drainReliableOutputs();
        scheduleResponseCheck();
        return true;
    }
    return false;
}

int
UBAdapter::sendHAPermissionReq(uint64_t linePa, HAOperation operation,
                                 uint64_t permissionEpoch,
                                 uint64_t byteMask, const uint8_t *writeData,
                                 int dstNode,
                                int dstSocket, uint64_t &ioReqId,
                                UBHAPermissionRespBody &outResp)
{
    if (!_port)
        return -1;
    fatal_if((operation == HAOperation::Read &&
              (byteMask != 0 || writeData != nullptr)) ||
             (operation == HAOperation::Write &&
              (byteMask == 0 || writeData == nullptr)),
             "UBAdapter node=%d socket=%d: invalid HA %s mask/data "
             "PA=0x%lx mask=0x%lx", _nodeId, _socketId,
             haOperationName(operation), linePa, byteMask);
    if (ioReqId != 0) {
        PendingKey key{CoherenceMessageType::HAPermissionResp, ioReqId};
        auto ready = _readyResponses.find(key);
        if (ready != _readyResponses.end()) {
            outResp = ready->second.b.haPermissionResp;
            if (outResp.status != HAStatus::Ok) {
                for (auto &out : _haAckOutputs)
                    if (out.reserved && out.reply.h.reqId == ioReqId) out = {};
            }
            _readyResponses.erase(ready);
            _inflightHAPermissionReqs.erase(ioReqId);
            return 1;
        }
        return -2;
    }

    ControlOutput *ackReservation = nullptr;
    for (auto &out : _haAckOutputs)
        if (!out.reserved) { ackReservation = &out; break; }
    if (!ackReservation) return -2;
    ioReqId = allocLocalReqId();
    CoherenceMessage req;
    req.h.type = CoherenceMessageType::HAPermissionReq;
    req.h.srcNode = _nodeId; req.h.srcSocket = _socketId;
    req.h.dstNode = dstNode; req.h.dstSocket = dstSocket;
    req.h.homeLinePa = linePa; req.h.localLinePa = linePa;
    req.h.reqId = ioReqId; req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = req.h.readyTick = curTick();
    req.b.haPermissionReq.operation = operation;
    req.b.haPermissionReq.permissionEpoch = permissionEpoch;
    req.b.haPermissionReq.byteMask = byteMask;
    if (operation == HAOperation::Write && writeData)
        memcpy(req.b.haPermissionReq.data, writeData, 64);
    ackReservation->reply.h = req.h;
    ackReservation->reserved = true;
    if (!transportSendReliable(req)) {
        *ackReservation = {};
        _readyResponses.erase(PendingKey{CoherenceMessageType::HAPermissionResp, ioReqId});
        ioReqId = 0;
        return -2;
    }
    _inflightHAPermissionReqs.insert(ioReqId);
    return -2;
}

bool
UBAdapter::sendHAPermissionAck(uint64_t linePa, HAOperation operation,
                               HAStatus status, uint64_t permissionEpoch,
                               int dstNode, int dstSocket, uint64_t reqId)
{
    CoherenceMessage ack;
    ack.h.type = CoherenceMessageType::HAPermissionAck;
    ack.h.srcNode = _nodeId; ack.h.srcSocket = _socketId;
    ack.h.dstNode = dstNode; ack.h.dstSocket = dstSocket;
    ack.h.homeLinePa = linePa; ack.h.localLinePa = linePa;
    ack.h.reqId = reqId; ack.h.seqNum = _nextSeq++;
    ack.h.enqueueTick = ack.h.readyTick = curTick();
    ack.b.haPermissionAck.operation = operation;
    ack.b.haPermissionAck.status = status;
    ack.b.haPermissionAck.permissionEpoch = permissionEpoch;
    return transportSendReliable(ack);
}

int
UBAdapter::sendHAPresenceProbeReq(uint64_t linePa, HAProbeAction action,
                                   uint64_t expectedEpoch, int dstNode,
                                   int dstSocket, uint64_t &ioReqId,
                                   UBHAPresenceProbeRespBody &outResp)
{
    if (!_port)
        return -1;
    if (ioReqId != 0) {
        PendingKey key{CoherenceMessageType::HAPresenceProbeResp, ioReqId};
        auto ready = _readyResponses.find(key);
        if (ready != _readyResponses.end()) {
            outResp = ready->second.b.haPresenceProbeResp;
            _readyResponses.erase(ready);
            _inflightHAPresenceProbeReqs.erase(ioReqId);
            return 1;
        }
        return -2;
    }

    ioReqId = allocLocalReqId();
    CoherenceMessage req;
    req.h.type = CoherenceMessageType::HAPresenceProbeReq;
    req.h.srcNode = _nodeId; req.h.srcSocket = _socketId;
    req.h.dstNode = dstNode; req.h.dstSocket = dstSocket;
    req.h.homeLinePa = linePa; req.h.localLinePa = linePa;
    req.h.reqId = ioReqId; req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = req.h.readyTick = curTick();
    req.b.haPresenceProbeReq.action = action;
    req.b.haPresenceProbeReq.expectedEpoch = expectedEpoch;
    if (!transportSendReliable(req)) {
        _readyResponses.erase(PendingKey{CoherenceMessageType::HAPresenceProbeResp, ioReqId});
        ioReqId = 0;
        return -2;
    }
    _inflightHAPresenceProbeReqs.insert(ioReqId);
    return -2;
}

bool
UBAdapter::sendHAPermissionResp(const CoherenceMessage &request,
                                 const UBHAPermissionRespBody &body)
{
    CoherenceMessage resp;
    resp.h.type = CoherenceMessageType::HAPermissionResp;
    resp.h.srcNode = _nodeId; resp.h.srcSocket = _socketId;
    resp.h.dstNode = request.h.srcNode; resp.h.dstSocket = request.h.srcSocket;
    resp.h.homeLinePa = request.h.homeLinePa;
    resp.h.localLinePa = request.h.localLinePa;
    resp.h.reqId = request.h.reqId; resp.h.epoch = request.h.epoch;
    resp.h.seqNum = _nextSeq++;
    resp.h.enqueueTick = resp.h.readyTick = curTick();
    resp.b.haPermissionResp = body;
    const unsigned peer = request.h.srcNode * _numSockets + request.h.srcSocket;
    if (peer >= unsigned(_numNodes * _numSockets)) return false;
    for (unsigned i = peer * 64; i < (peer + 1) * 64; ++i) {
        auto &out = _permissionReplies[i];
        if (!out.reserved || out.message.h.reqId != request.h.reqId) continue;
        out.message = resp;
        out.ready = true;
        scheduleResponseCheck();
        return true;
    }
    return false;
}

bool
UBAdapter::sendHAPresenceProbeResp(const CoherenceMessage &request,
                                    const UBHAPresenceProbeRespBody &body)
{
    CoherenceMessage resp;
    resp.h.type = CoherenceMessageType::HAPresenceProbeResp;
    resp.h.srcNode = _nodeId; resp.h.srcSocket = _socketId;
    resp.h.dstNode = request.h.srcNode; resp.h.dstSocket = request.h.srcSocket;
    resp.h.homeLinePa = request.h.homeLinePa;
    resp.h.localLinePa = request.h.localLinePa;
    resp.h.reqId = request.h.reqId; resp.h.epoch = request.h.epoch;
    resp.h.seqNum = _nextSeq++;
    resp.h.enqueueTick = resp.h.readyTick = curTick();
    resp.b.haPresenceProbeResp = body;
    return transportSendReliable(resp);
}

bool
UBAdapter::transportRecv(CoherenceMessageType expectedType, uint64_t expectedReqId)
{
    if (!_port) {
        if (!_lastResponseValid) {
            return false;
        }
        if (_lastResponse.h.type != expectedType) {
            return false;
        }
        return (expectedReqId == 0 || _lastResponse.h.reqId == expectedReqId);
    }

    static constexpr int kMaxPollIters = 200000;
    const uint64_t visible = std::numeric_limits<uint64_t>::max();

    for (int i = 0; i < kMaxPollIters; ++i) {
        framework::ReceiveStatus status;
        const framework::Message *m =
            framework::ReceiveMessage(_port, visible, &status);
        if (!m || status != framework::ReceiveStatus::Message) {
            continue;
        }

        const auto msgType = framework::GetMessageType(m);
        if (msgType == framework::MessageType::ControlSync) {
            continue;
        }
        if (msgType != framework::MessageType::Payload) {
            warn("UBAdapter node=%d socket=%d: transportRecv unexpected Message type=%u",
                 _nodeId, _socketId, static_cast<unsigned>(msgType));
            continue;
        }

        const CoherenceMessage *coh = coherencePayload(m);
        if (!coh) {
            warn("UBAdapter node=%d socket=%d: transportRecv bad payload size=%u",
                 _nodeId, _socketId,
                 static_cast<unsigned>(framework::GetMessagePayloadSize(m)));
            continue;
        }

        recvFromRouter(*coh);
        if (_lastResponseValid && _lastResponse.h.type == expectedType &&
            (expectedReqId == 0 || _lastResponse.h.reqId == expectedReqId)) {
            return true;
        }
    }

    warn("UBAdapter node=%d socket=%d: transportRecv timeout waiting type=%s reqId=%lu",
         _nodeId, _socketId, coherenceMsgTypeName(expectedType), expectedReqId);
    return false;
}

// ---- Phase 2: Synchronous Read Request ----

int
UBAdapter::sendReadReq(
    uint64_t homePa, int reqType, bool writeIntent,
    int requesterNode, uint64_t epoch, uint64_t reqId,
    int homeNode, int ingressSocket, int homeSocket,
    Tick *outGrantVisibleTick, Tick *outSentinelVisibleTick,
    bool *outRecallNeeded, int *outRecallOwnerNode,
    GrantDataSource *outDataSource, uint64_t *outAuthEpoch,
    uint64_t *outGrantEpoch,
    int *outPendingInvCount, uint64_t *outPendingInvMask,
    uint64_t *outCommittedEpoch,
    DataBlock *outGrantData, bool *outGrantDataValid)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sendReadReq homePa=0x%lx "
            "reqType=%d writeIntent=%d reqNode=%d epoch=%lu reqId=%lu "
            "homeNode=%d ingressSocket=%d homeSocket=%d\n",
            _nodeId, _socketId, homePa, reqType, writeIntent,
            requesterNode, epoch, reqId, homeNode, ingressSocket, homeSocket);

    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendReadReq called with no transport bound\n",
              _nodeId, _socketId);
    }

    // Build ReadReq UBMsg
    CoherenceMessage req;
    req.h.type = CoherenceMessageType::ReadReq;
    req.h.srcNode = _nodeId;
    req.h.srcSocket = _socketId;
    req.h.dstNode = homeNode;
    req.h.dstSocket = homeSocket;
    req.h.homeNode = homeNode;
    req.h.homeSocket = homeSocket;
    req.h.ingressSocket = ingressSocket;
    req.h.requesterNode = requesterNode;
    req.h.targetNode = homeNode;
    req.h.flags = writeIntent ? static_cast<uint32_t>(CFLAG_WRITE_INTENT) : 0;
    req.h.homeLinePa = homePa;
    req.h.localLinePa = 0;
    req.h.epoch = epoch;
    req.h.reqId = reqId;
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();

    req.b.readReq.neededPerm = (reqType == 0) ? 0 : 1;

    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sending ReadReq %s\n",
            _nodeId, _socketId, ubMsgToString(req).c_str());

    // Port async path: check ready-response cache keyed by (ReadResp, reqId)
    if (_port) {
        PendingKey rkey{CoherenceMessageType::ReadResp, reqId};
        auto rit = _readyResponses.find(rkey);
        if (rit != _readyResponses.end()) {
            CoherenceMessage resp = rit->second;
            if (resp.b.readResp.recallNeeded && resp.b.readResp.recallOwnerNode >= 0) {
                OuterRecallMsg relay;
                relay.linePa = homePa;
                relay.ownerNode = resp.b.readResp.recallOwnerNode;
                relay.homeNode = homeNode;
                relay.ownerLocalPa = _addrMap.buildDsmPA(relay.ownerNode, homeNode,
                    _addrMap.dsmOffset(homePa), homeSocket);
                relay.epoch = resp.b.readResp.committedEpoch;
                relay.reqId = reqId;
                relay.requesterNode = _nodeId;
                relay.requesterSocket = _socketId;
                relay.sourceSocket = _socketId;
                relay.isReadRequest = reqType == 0;
                relay.dataNeeded = true;
                if (!sendRecallReqToOwner(relay.ownerNode, relay, homeSocket)) return -2;
                // Only consume the response after the original intent is
                // retained. The backend must not generate a second relay.
                resp.b.readResp.recallNeeded = false;
            }
            for (auto &child : _directChildren) {
                if (!child.live || child.request.reqId != reqId) continue;
                const bool authenticated = child.ready && child.dataHeader.srcNode == resp.h.targetNode &&
                    child.dataHeader.srcSocket == resp.h.homeSocket &&
                    child.dataHeader.epoch == resp.b.readResp.committedEpoch;
                const bool directGrant = resp.h.flags & static_cast<uint32_t>(CFLAG_DIRECT_GRANT);
                if (directGrant && !authenticated) {
                    if (!child.grantWaitStart) child.grantWaitStart = curTick();
                    if (curTick() - child.grantWaitStart >= ReadReqRetryTicks) {
                        // Poll the exact Home root for authoritative fallback.
                        // No new reqId, no premature Clear, no second child.
                        if (transportSend(req)) {
                            _readyResponses.erase(rit);
                            child.grantWaitStart = curTick();
                        }
                    }
                    return -2;
                }
                if (authenticated) {
                    fatal_if(!directGrant && memcmp(child.data.data(), resp.b.readResp.grantData, 64),
                              "authenticated C4 data differs from Home completion reqId=%lu", reqId);
                    memcpy(resp.b.readResp.grantData, child.data.data(), 64);
                    resp.h.flags |= static_cast<uint32_t>(CFLAG_HAS_DATA);
                    if (directGrant)
                        inform("[C4-DATA-ONLY-COMPLETE] reqId=%lu owner=%u epoch=%lu home_payload=0",
                               reqId, resp.h.targetNode, child.dataHeader.epoch);
                    DPRINTF(RubyEP, "[C4-CHILD-AUTHENTICATED] reqId=%lu owner=%u epoch=%lu\n",
                            reqId, resp.h.targetNode, child.dataHeader.epoch);
                }
                child = {};
                break;
            }
            if (outGrantVisibleTick) *outGrantVisibleTick = resp.b.readResp.grantVisibleTick;
            if (outSentinelVisibleTick) *outSentinelVisibleTick = resp.b.readResp.sentinelVisibleTick;
            if (outRecallNeeded) *outRecallNeeded = resp.b.readResp.recallNeeded;
            if (outRecallOwnerNode) *outRecallOwnerNode = resp.b.readResp.recallOwnerNode;
            if (outDataSource) *outDataSource = static_cast<GrantDataSource>(resp.b.readResp.dataSource);
            if (outAuthEpoch) *outAuthEpoch = resp.b.readResp.authEpoch;
            if (outGrantEpoch) *outGrantEpoch = resp.b.readResp.grantEpoch;
            if (outPendingInvCount) *outPendingInvCount = resp.b.readResp.pendingInvCount;
            if (outPendingInvMask) *outPendingInvMask = resp.b.readResp.pendingInvMask;
            if (outCommittedEpoch) *outCommittedEpoch = resp.b.readResp.committedEpoch;
            if (outGrantData && outGrantDataValid) {
                // In split-mode, gem5 local physMem has no valid DSM data.
                // All grant data comes from ubio via ReadResp payload.
                // Accept payload whenever CFLAG_HAS_DATA is set.
                *outGrantDataValid =
                    (resp.h.flags & static_cast<uint32_t>(CFLAG_HAS_DATA)) != 0;
                if (*outGrantDataValid) memcpy(outGrantData->getDataMod(0), resp.b.readResp.grantData, 64);
            }
            _inflightReadReqs.erase(reqId);
            _readyResponses.erase(rit);
            return static_cast<int>(resp.b.readResp.grantType);
        }
        auto inflight = _inflightReadReqs.find(reqId);
        if (inflight != _inflightReadReqs.end()) {
            if (curTick() < inflight->second)
                return -2;
        }
    }

    _lastResponseValid = false;
    if (_port && !_inflightReadReqs.count(reqId) &&
        _inflightReadReqs.size() >= MaxInflightReadReqs) {
        warn("UBAdapter node=%d: bounded ReadReq table full (%zu), "
             "reqId=%lu stays BUSY\n",
             _nodeId, _inflightReadReqs.size(), reqId);
        return -1;
    }
    DirectChild *childReservation = nullptr;
    for (auto &child : _directChildren)
        if (child.live && child.request.reqId == reqId) { childReservation = &child; break; }
    if (!childReservation) {
        for (auto &child : _directChildren)
            if (!child.live) { childReservation = &child; break; }
        if (!childReservation) return -2;
        childReservation->request = req.h;
        childReservation->live = true;
    }
    if (!transportSend(req)) {
        return -1;
    }

    // Port async path: schedule check, return pending
    if (_port) {
        // Dedup guard: mark this reqId in-flight so subsequent retries of the
        // SAME outer request (same reqId, driven by the local CPU/L2 miss
        // re-issue at ~10ns cadence) hit the `_inflightReadReqs.count(reqId)`
        // early-return above and do NOT re-transmit a duplicate ReadReq. The
        // marker is cleared when the matching ReadResp arrives (ready-response
        // consumption above, or the recvFromRouter callback). Without this
        // insert the guard was dead (only erase/count, never insert), causing
        // the TC98 retry storm: one line-hot reqId sent ~100k times, home
        // BUSY-rejecting each and flooding a 300MB+ log.
        _inflightReadReqs[reqId] = curTick() + ReadReqRetryTicks;
        scheduleResponseCheck();
        return -2;
    }

    if (!transportRecv(CoherenceMessageType::ReadResp, reqId)) {
        warn("UBAdapter node=%d: sendReadReq: no response received PA=0x%lx\n",
             _nodeId, homePa);
        return -1;
    }
    return static_cast<int>(_lastResponse.b.readResp.grantType);
}

// ---- Writeback Request ----

int
UBAdapter::sendWritebackReq(uint64_t homePa, int requesterNode,
                             uint64_t epochVal, UBWritebackKind kind,
                             UBWriteDisposition disposition, uint64_t byteMask,
                             int homeNode, int homeSocket,
                             const uint8_t *dirtyData, uint64_t *ioReqId,
                             uint64_t parentReqId, uint64_t parentEpoch,
                             uint64_t *mergedRecallReqId)
{
    if (mergedRecallReqId) *mergedRecallReqId = 0;
    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendWritebackReq called with no transport bound\n",
              _nodeId, _socketId);
    }

    uint64_t reqId = ioReqId ? *ioReqId : 0;
    if (reqId == 0) {
        fatal_if(kind == UBWritebackKind::StoreCommit ||
                     kind == UBWritebackKind::InternalPublication,
                  "UBAdapter node=%d socket=%d: memory publication requires caller "
                 "supplied stable wire reqId", _nodeId, _socketId);
        reqId = allocLocalReqId();
        if (ioReqId)
            *ioReqId = reqId;
    }
    PendingKey responseKey{CoherenceMessageType::WritebackResp, reqId};
    auto ready = _readyResponses.find(responseKey);
    if (ready != _readyResponses.end()) {
        bool success = ready->second.b.writebackResp.success;
        fatal_if(ready->second.h.homeLinePa != homePa ||
                     ready->second.h.epoch != epochVal ||
                     ready->second.h.srcNode != homeNode ||
                     ready->second.h.srcSocket != homeSocket,
                 "WritebackResp identity mismatch reqId=%lu", reqId);
        if (mergedRecallReqId)
            *mergedRecallReqId = ready->second.b.writebackResp.mergedRecallReqId;
        const bool deferred = ready->second.h.flags &
            static_cast<uint32_t>(CFLAG_DEFERRED);
        _readyResponses.erase(ready);
        _inflightWritebackReqs.erase(reqId);
        if (deferred) {
            fatal_if(success, "deferred writeback cannot be successful");
            return -2;
        }
        return success ? 1 : 0;
    }
    if (_inflightWritebackReqs.count(reqId))
        return -2;

    CoherenceMessage req;
    req.h.type = CoherenceMessageType::WritebackReq;
    req.h.srcNode = _nodeId;
    req.h.srcSocket = _socketId;
    req.h.dstNode = homeNode;
    req.h.dstSocket = homeSocket;
    req.h.homeNode = homeNode;
    req.h.homeSocket = homeSocket;
    req.h.ingressSocket = _socketId;
    req.h.requesterNode = requesterNode;
    req.h.homeLinePa = homePa;
    req.h.epoch = epochVal;
    req.h.reqId = reqId;
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();
    if (disposition == UBWriteDisposition::KeepClean)
        req.h.flags |= static_cast<uint32_t>(CFLAG_KEEP_AS_CLEAN);
    req.b.writebackReq.kind = kind;
    req.b.writebackReq.disposition = disposition;
    req.b.writebackReq.byteMask = byteMask;
    req.b.writebackReq.parentInvalidateReqId = parentReqId;
    req.b.writebackReq.parentInvalidateEpoch = parentEpoch;
    fatal_if(kind == UBWritebackKind::OwnerWriteback &&
                 (byteMask != ~0ULL ||
                  (disposition != UBWriteDisposition::DropOwner &&
                   disposition != UBWriteDisposition::KeepClean)),
             "UBAdapter node=%d socket=%d: invalid OwnerWriteback reqId=%lu",
             _nodeId, _socketId, reqId);
    fatal_if(kind == UBWritebackKind::StoreCommit &&
                 (disposition != UBWriteDisposition::MemoryOnly ||
                  !dirtyData || byteMask == 0),
             "UBAdapter node=%d socket=%d: invalid StoreCommit reqId=%lu",
              _nodeId, _socketId, reqId);
    fatal_if(kind == UBWritebackKind::InternalPublication &&
                 (disposition != UBWriteDisposition::MemoryOnly ||
                  requesterNode != -1 || epochVal != 0 || !dirtyData ||
                  byteMask == 0),
             "UBAdapter node=%d socket=%d: invalid internal publication reqId=%lu",
             _nodeId, _socketId, reqId);
    // Carry dirty cacheline data so ubio can persist to DsmDataStore
    if (dirtyData) {
        req.b.writebackReq.hasData = true;
        std::memcpy(req.b.writebackReq.data, dirtyData, 64);
    }
    // ── Phase C4 trace point 3: WriteBackReq send ──
    {
        uint64_t off = homePa & 0x1FFFULL;
        uint64_t ckOff = homePa & 0xFFFFFULL;
        if (ckOff < 0x80000ULL && (off % 64 == 0)) {
            uint64_t w0;
            std::memcpy(&w0, dirtyData ? dirtyData : req.b.writebackReq.data, 8);
            DPRINTF(RubyEP,
                    "[C4-WBREQ-SEND] node=%d pa=0x%lx off=0x%lx hasData=%d w0=0x%016lx",
                    _nodeId, homePa, off, dirtyData ? 1 : 0, w0);
        }
    }

    _lastResponseValid = false;
    if (!transportSend(req)) {
        return -1;
    }
    _inflightWritebackReqs.insert(reqId);

    // Port async path: schedule check, return pending
    if (_port) {
        scheduleResponseCheck();
        return -2;
    }

    if (!transportRecv(CoherenceMessageType::WritebackResp, req.h.reqId)) {
        _inflightWritebackReqs.erase(reqId);
        warn("UBAdapter node=%d: sendWritebackReq: no response PA=0x%lx\n",
             _nodeId, homePa);
        return -1;
    }

    const CoherenceMessage &resp = _lastResponse;
    if (resp.h.type != CoherenceMessageType::WritebackResp) {
        _inflightWritebackReqs.erase(reqId);
        warn("UBAdapter node=%d: sendWritebackReq: unexpected response type %s\n",
             _nodeId, coherenceMsgTypeName(resp.h.type));
        return -1;
    }

    _inflightWritebackReqs.erase(reqId);
    if (mergedRecallReqId)
        *mergedRecallReqId = resp.b.writebackResp.mergedRecallReqId;
    return resp.b.writebackResp.success ? 1 : 0;
}

// ---- Evict Request ----

int
UBAdapter::sendConditionalRelease(uint64_t homePa, uint64_t lease,
    int homeNode, int homeSocket, uint64_t reqId)
{
    if (!reqId || !lease || (homePa & 63) || homeNode < 0 || homeSocket < 0)
        return -1;
    if (_releaseReqId && (_releaseReqId != reqId || _releasePa != homePa ||
        _releaseLease != lease || _releaseHome != homeNode ||
        _releaseSocket != homeSocket)) return -2;
    const PendingKey key{CoherenceMessageType::EvictResp, reqId};
    const auto found = _readyResponses.find(key);
    if (found != _readyResponses.end()) {
        const auto &r = found->second;
        if (!_releaseReqId || r.h.homeLinePa != homePa || r.h.epoch != lease ||
            r.h.srcNode != homeNode || r.h.srcSocket != homeSocket ||
            r.h.dstNode != _nodeId || r.h.dstSocket != _socketId)
            return -1;
        const auto outcome = r.b.evictResp.result;
        const bool typedSuccess = outcome == UBReleaseResult::Applied ||
            outcome == UBReleaseResult::Duplicate ||
            outcome == UBReleaseResult::AlreadyRetiredMatch;
        const bool ok = typedSuccess ||
            (outcome == UBReleaseResult::Legacy && r.b.evictResp.success);
        if (typedSuccess) {
            CoherenceMessage ack;
            ack.h.type = CoherenceMessageType::EvictReq;
            ack.h.srcNode = _nodeId; ack.h.srcSocket = _socketId;
            ack.h.dstNode = homeNode; ack.h.dstSocket = homeSocket;
            ack.h.homeNode = homeNode; ack.h.homeSocket = homeSocket;
            ack.h.requesterNode = _nodeId;
            ack.h.homeLinePa = homePa; ack.h.epoch = lease;
            ack.h.reqId = reqId; ack.h.seqNum = _nextSeq++;
            ack.b.evictReq.receiptAck = true;
            if (!transportSend(ack)) return -2;
        }
        _readyResponses.erase(found);
        _releaseReqId = 0;
        if (outcome == UBReleaseResult::AlreadyRetiredMatch) return 2;
        if (outcome == UBReleaseResult::Stale) return 3;
        return ok ? 1 : 0;
    }
    if (_releaseReqId) return -2;
    CoherenceMessage request;
    request.h.type = CoherenceMessageType::EvictReq;
    request.h.srcNode = _nodeId; request.h.srcSocket = _socketId;
    request.h.dstNode = homeNode; request.h.dstSocket = homeSocket;
    request.h.homeNode = homeNode; request.h.homeSocket = homeSocket;
    request.h.requesterNode = _nodeId;
    request.h.homeLinePa = homePa; request.h.epoch = lease;
    request.h.reqId = reqId;
    request.h.seqNum = _nextSeq++;
    if (!transportSend(request)) return -1;
    _releaseReqId = reqId; _releasePa = homePa; _releaseLease = lease;
    _releaseHome = homeNode; _releaseSocket = homeSocket;
    scheduleResponseCheck();
    return -2;
}

int
UBAdapter::sendEvictReq(uint64_t homePa, int evictingNode, uint64_t epochVal,
                          int homeNode, int homeSocket)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sendEvictReq homePa=0x%lx "
            "evictingNode=%d epoch=%lu homeNode=%d homeSocket=%d\n",
            _nodeId, _socketId, homePa, evictingNode, epochVal,
            homeNode, homeSocket);

    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendEvictReq called with no transport bound\n",
              _nodeId, _socketId);
    }

    // Legacy no-ID eviction is explicitly one serial transaction per adapter.
    // Never consult a controller-global last response from another line.
    const PendingKey legacyEvict{CoherenceMessageType::EvictResp, 0};
    auto evict = _readyResponses.find(legacyEvict);
    if (evict != _readyResponses.end()) {
        if (evict->second.h.homeLinePa != homePa) return -2;
        const bool success = evict->second.b.evictResp.success;
        _readyResponses.erase(evict);
        return success ? 1 : 0;
    }
    if (_readyResponses.reserved(legacyEvict)) return -2;

    CoherenceMessage req;
    req.h.type = CoherenceMessageType::EvictReq;
    req.h.srcNode = _nodeId;
    req.h.srcSocket = _socketId;
    req.h.dstNode = homeNode;
    req.h.dstSocket = homeSocket;
    req.h.homeNode = homeNode;
    req.h.homeSocket = homeSocket;
    req.h.ingressSocket = _socketId;
    req.h.requesterNode = evictingNode;
    req.h.homeLinePa = homePa;
    req.h.epoch = epochVal;
    req.h.reqId = 0;
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();

    _lastResponseValid = false;
    if (!transportSend(req)) {
        return -1;
    }

    // Port async path: schedule check, return pending
    if (_port) {
        scheduleResponseCheck();
        return -2;
    }

    if (!transportRecv(CoherenceMessageType::EvictResp, req.h.reqId)) {
        warn("UBAdapter node=%d: sendEvictReq: no response PA=0x%lx\n",
             _nodeId, homePa);
        return -1;
    }

    const CoherenceMessage &resp = _lastResponse;
    if (resp.h.type != CoherenceMessageType::EvictResp) {
        warn("UBAdapter node=%d: sendEvictReq: unexpected response type %s\n",
             _nodeId, coherenceMsgTypeName(resp.h.type));
        return -1;
    }

    return resp.b.evictResp.success ? 1 : 0;
}

// ---- Upgrade Request ----

int
UBAdapter::sendUpgradeReq(uint64_t homePa, int requesterNode,
                            uint64_t epoch, uint64_t reqId,
                            int desiredPerm, int cause,
                            uint64_t *outUpgradeTargetMask,
                             uint64_t *outCommittedEpoch,
                             int homeNode, int homeSocket,
                             bool checkOnly, bool forceWire)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sendUpgradeReq homePa=0x%lx "
            "reqNode=%d epoch=%lu reqId=%lu desiredPerm=%d homeNode=%d homeSocket=%d\n",
            _nodeId, _socketId, homePa, requesterNode, epoch, reqId,
            desiredPerm, homeNode, homeSocket);

    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendUpgradeReq called with no transport bound\n",
              _nodeId, _socketId);
    }

    // Port async: check ready-response cache keyed by (UpgradeResp, reqId).
    // Using the map cache (not the single-slot _lastResponse) so that a retry
    // with the SAME reqId hits the cached UpgradeResp once it arrives, instead
    // of sending a duplicate UpgradeReq that the home rejects (existing
    // outstanding) — the death loop that hung TC3/8/10/11.
    if (_port) {
        PendingKey rkey{CoherenceMessageType::UpgradeResp, reqId};
        if (forceWire) {
            _readyResponses.erase(rkey);
        }
        auto rit = _readyResponses.find(rkey);
        if (!forceWire && rit != _readyResponses.end()) {
            const CoherenceMessage &resp = rit->second;
            inform("[UPGRADE-FORENSIC] stage=GEM5_RESP_CONSUME node=%d socket=%d "
                   "pa=0x%lx reqId=%lu epoch=%lu flags=0x%x curT=%lu",
                   _nodeId, _socketId, resp.h.homeLinePa, resp.h.reqId,
                   resp.h.epoch, resp.h.flags, curTick());
            if (outUpgradeTargetMask)
                *outUpgradeTargetMask = resp.b.upgradeResp.upgradeTargetMask;
            if (outCommittedEpoch)
                *outCommittedEpoch = resp.b.upgradeResp.committedEpoch;
            bool accepted =
                (resp.h.flags & static_cast<uint32_t>(CFLAG_ACCEPTED)) != 0;
            bool permanent =
                (resp.h.flags & static_cast<uint32_t>(CFLAG_BUSY)) != 0;
            bool deferred =
                (resp.h.flags & static_cast<uint32_t>(CFLAG_DEFERRED)) != 0;
            _readyResponses.erase(rit);
            return accepted ? 1 : (permanent ? -3 : (deferred ? -4 : 0));
        }
        // checkOnly: a previous UpgradeReq for this reqId is already in flight
        // (async pending). Do NOT re-send — a duplicate would make the home emit
        // a rejected UpgradeResp that could overwrite the accepted one in the
        // cache. Just report still-pending.
        if (checkOnly)
            return -2;
    }

    CoherenceMessage req;
    req.h.type = CoherenceMessageType::UpgradeReq;
    req.h.srcNode = _nodeId;
    req.h.srcSocket = _socketId;
    req.h.dstNode = homeNode;
    req.h.dstSocket = homeSocket;
    req.h.homeNode = homeNode;
    req.h.homeSocket = homeSocket;
    req.h.ingressSocket = _socketId;
    req.h.requesterNode = requesterNode;
    req.h.homeLinePa = homePa;
    req.h.epoch = epoch;
    req.h.reqId = reqId;
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();

    req.b.upgradeReq.desiredPerm = static_cast<uint8_t>(desiredPerm);
    req.b.upgradeReq.cause = static_cast<uint8_t>(cause);

    _lastResponseValid = false;
    if (!transportSend(req)) {
        return -1;
    }

    // Port async path: schedule check, return pending
    if (_port) {
        scheduleResponseCheck();
        return -2;
    }

    if (!transportRecv(CoherenceMessageType::UpgradeResp, reqId)) {
        warn("UBAdapter node=%d: sendUpgradeReq: no response PA=0x%lx\n",
             _nodeId, homePa);
        return -1;
    }

    const CoherenceMessage &resp = _lastResponse;
    if (resp.h.type != CoherenceMessageType::UpgradeResp) {
        warn("UBAdapter node=%d: sendUpgradeReq: unexpected response type %s\n",
             _nodeId, coherenceMsgTypeName(resp.h.type));
        return -1;
    }

    bool accepted = (resp.h.flags & static_cast<uint32_t>(CFLAG_ACCEPTED)) != 0;
    bool permanent = (resp.h.flags & static_cast<uint32_t>(CFLAG_BUSY)) != 0;
    bool deferred = (resp.h.flags & static_cast<uint32_t>(CFLAG_DEFERRED)) != 0;
    if (outUpgradeTargetMask)
        *outUpgradeTargetMask = resp.b.upgradeResp.upgradeTargetMask;
    if (outCommittedEpoch)
        *outCommittedEpoch = resp.b.upgradeResp.committedEpoch;

    DPRINTF(RubyEP,
            "UBAdapter node=%d: sendUpgradeReq result accepted=%d permanent=%d targetMask=0x%lx\n",
            _nodeId, accepted, permanent, resp.b.upgradeResp.upgradeTargetMask);

    // 1=accepted, -3=permanent reject (not sharer → abandon), 0=temporary (retry).
    return accepted ? 1 : (permanent ? -3 : (deferred ? -4 : 0));
}

// ---- Upgrade Done Request ----

int
UBAdapter::sendUpgradeDoneReq(uint64_t homePa, int requesterNode,
                                uint64_t epoch, uint64_t reqId,
                                int homeNode, int homeSocket)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sendUpgradeDoneReq homePa=0x%lx "
            "reqNode=%d epoch=%lu reqId=%lu homeNode=%d homeSocket=%d\n",
            _nodeId, _socketId, homePa, requesterNode, epoch, reqId,
            homeNode, homeSocket);

    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendUpgradeDoneReq called with no transport bound\n",
              _nodeId, _socketId);
    }

    // Port async: check cached response first
    const PendingKey doneKey{CoherenceMessageType::UpgradeDoneResp, reqId};
    auto done = _readyResponses.find(doneKey);
    if (done != _readyResponses.end()) {
        const bool accepted = done->second.b.upgradeDoneResp.accepted;
        _readyResponses.erase(done);
        return accepted ? 1 : 0;
    }
    if (_readyResponses.reserved(doneKey)) return -2;

    CoherenceMessage req;
    req.h.type = CoherenceMessageType::UpgradeDoneReq;
    req.h.srcNode = _nodeId;
    req.h.srcSocket = _socketId;
    req.h.dstNode = homeNode;
    req.h.dstSocket = homeSocket;
    req.h.homeNode = homeNode;
    req.h.homeSocket = homeSocket;
    req.h.ingressSocket = _socketId;
    req.h.requesterNode = requesterNode;
    req.h.homeLinePa = homePa;
    req.h.epoch = epoch;
    req.h.reqId = reqId;
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();

    _lastResponseValid = false;
    if (!transportSend(req)) {
        return -1;
    }

    // Port async path: schedule check, return pending
    if (_port) {
        scheduleResponseCheck();
        return -2;
    }

    if (!transportRecv(CoherenceMessageType::UpgradeDoneResp, reqId)) {
        warn("UBAdapter node=%d: sendUpgradeDoneReq: no response PA=0x%lx\n",
             _nodeId, homePa);
        return -1;
    }

    const CoherenceMessage &resp = _lastResponse;
    if (resp.h.type != CoherenceMessageType::UpgradeDoneResp) {
        warn("UBAdapter node=%d: sendUpgradeDoneReq: unexpected response type %s\n",
             _nodeId, coherenceMsgTypeName(resp.h.type));
        return -1;
    }

    return resp.b.upgradeDoneResp.accepted ? 1 : 0;
}

// ---- Clear Request ----

bool
UBAdapter::sendClearReqOneWay(uint64_t linePa, int srcNode,
                               uint64_t epoch, uint64_t reqId,
                               int homeNode, int homeSocket)
{
    fatal_if(!_port,
             "UBAdapter node=%d socket=%d: one-way Clear has no transport",
             _nodeId, _socketId);
    CoherenceMessage req;
    req.h.type = CoherenceMessageType::ClearReq;
    req.h.srcNode = _nodeId;
    req.h.srcSocket = _socketId;
    req.h.dstNode = homeNode;
    req.h.dstSocket = homeSocket;
    req.h.homeNode = homeNode;
    req.h.homeSocket = homeSocket;
    req.h.ingressSocket = _socketId;
    req.h.requesterNode = srcNode;
    req.h.homeLinePa = linePa;
    req.h.epoch = epoch;
    req.h.reqId = reqId;
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = req.h.readyTick = curTick();
    // Wire marker: Home commits normally but suppresses ClearResp; fault
    // injection also recognizes this as an explicitly lossless Clear.
    req.b.clearReq.reason = 1;
    return transportSendReliable(req);
}

int
UBAdapter::sendClearReq(uint64_t linePa, int srcNode,
                         uint64_t epoch, uint64_t reqId,
                         int homeNode, int homeSocket)
{
    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendClearReq called with no transport bound\n",
              _nodeId, _socketId);
    }

    // Port async: check ready-response cache keyed by (ClearResp, reqId)
    if (_port) {
        PendingKey rkey{CoherenceMessageType::ClearResp, reqId};
        auto rit = _readyResponses.find(rkey);
        if (rit != _readyResponses.end()) {
            DPRINTF(RubyEP, "[CLR-CACHE-HIT] node=%d reqId=%lu accepted=%d\n",
                    _nodeId, reqId,
                    rit->second.b.clearResp.accepted ? 1 : 0);
            bool a = rit->second.b.clearResp.accepted;
            _inflightClearReqs.erase(reqId);
            _clearRetryTick.erase(reqId);
            _readyResponses.erase(rit);
            return a ? 1 : 0;
        }
        if (_inflightClearReqs.count(reqId)) {
            auto retry = _clearRetryTick.find(reqId);
            if (retry != _clearRetryTick.end() && curTick() < retry->second)
                return -2;
            // A dropped ClearReq has no response to release the dedup guard.
            // Retransmit the same tuple after bounded virtual time so the home
            // can deduplicate it without allocating a new grant transaction.
            _inflightClearReqs.erase(reqId);
        }
        DPRINTF(RubyEP,
                "[CLR-CACHE-MISS] node=%d reqId=%lu sending new ClearReq\n",
                _nodeId, reqId);
    }

    CoherenceMessage req;
    req.h.type = CoherenceMessageType::ClearReq;
    req.h.srcNode = _nodeId;
    req.h.srcSocket = _socketId;
    req.h.dstNode = homeNode;
    req.h.dstSocket = homeSocket;
    req.h.homeNode = homeNode;
    req.h.homeSocket = homeSocket;
    req.h.ingressSocket = _socketId;
    req.h.requesterNode = srcNode;
    req.h.homeLinePa = linePa;
    req.h.epoch = epoch;
    req.h.reqId = reqId;
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();
    req.b.clearReq.reason = 0;

    _lastResponseValid = false;
    if (!transportSend(req)) {
        return -1;
    }

    DPRINTF(RubyEP, "[CLR-TX] n=%d reqId=%lu curT=%lu\n",
            _nodeId, reqId, curTick());

    // Port async path: schedule check, return pending
    if (_port) {
        // Clear is retried until its response is cached. Keep one network copy
        // in flight for this reqId; otherwise every local retry retransmits it.
        _inflightClearReqs.insert(reqId);
        _clearRetryTick[reqId] = curTick() + 1000000;
        scheduleResponseCheck();
        return -2;
    }

    if (!transportRecv(CoherenceMessageType::ClearResp, reqId)) {
        warn("UBAdapter node=%d: sendClearReq: no response PA=0x%lx\n",
             _nodeId, linePa);
        return -1;
    }

    const CoherenceMessage &resp = _lastResponse;
    if (resp.h.type != CoherenceMessageType::ClearResp) {
        warn("UBAdapter node=%d: sendClearReq: unexpected response type %s\n",
             _nodeId, coherenceMsgTypeName(resp.h.type));
        return -1;
    }

    return resp.b.clearResp.accepted ? 1 : 0;
}

// ---- Recall Response (fire-and-forget → home UBCC) ----

bool
UBAdapter::sendRecallResp(uint64_t linePa, int ownerNode,
                           bool ackReceived, bool dataReturned, uint64_t epoch,
                           uint64_t reqId,
                           const DataBlock *dataBlk,
                           int homeNode, int homeSocket,
                           bool dataForwarded)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sendRecallResp PA=0x%lx "
            "owner=%d dataReturned=%d epoch=%lu reqId=%lu homeNode=%d homeSocket=%d\n",
            _nodeId, _socketId, linePa, ownerNode, dataReturned, epoch, reqId,
            homeNode, homeSocket);

    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendRecallResp called with no transport bound\n",
              _nodeId, _socketId);
    }

    CoherenceMessage req;
    req.h.type = CoherenceMessageType::RecallResp;
    req.h.srcNode = _nodeId;
    req.h.srcSocket = _socketId;
    req.h.dstNode = homeNode;
    req.h.dstSocket = homeSocket;
    req.h.homeNode = homeNode;
    req.h.homeSocket = homeSocket;
    req.h.ingressSocket = _socketId;
    req.h.requesterNode = ownerNode;
    req.h.homeLinePa = linePa;
    req.h.epoch = epoch;
    req.h.reqId = reqId;
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();

    if (ackReceived)
        req.h.flags |= static_cast<uint32_t>(CFLAG_ACCEPTED);
    if (dataReturned)
        req.h.flags |= static_cast<uint32_t>(CFLAG_DATA_RETURNED);
    if (dataForwarded)
        req.h.flags |= static_cast<uint32_t>(CFLAG_DATA_FORWARDED);
    if (dataBlk && dataReturned) {
        req.h.flags |= static_cast<uint32_t>(CFLAG_HAS_DATA);
        memcpy(req.b.recallResp.data, dataBlk->getData(0, 64), 64);
    }

    // No response is expected, but loss under local port backpressure would
    // strand the Home transaction. Queue until the transport accepts it.
    return transportSendReliable(req);
}

// ---- Invalidation Ack (fire-and-forget → home UBCC) ----

bool
UBAdapter::sendInvalidateAck(uint64_t linePa, int ackNode,
                              uint64_t epoch, uint64_t reqId,
                              int homeNode, int homeSocket)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sendInvalidateAck PA=0x%lx "
            "ackNode=%d epoch=%lu reqId=%lu homeNode=%d homeSocket=%d\n",
            _nodeId, _socketId, linePa, ackNode, epoch, reqId,
            homeNode, homeSocket);

    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendInvalidateAck called with no transport bound\n",
              _nodeId, _socketId);
    }

    CoherenceMessage req;
    req.h.type = CoherenceMessageType::InvalidateAck;
    req.h.srcNode = _nodeId;
    req.h.srcSocket = _socketId;
    req.h.dstNode = homeNode;
    req.h.dstSocket = homeSocket;
    req.h.homeNode = homeNode;
    req.h.homeSocket = homeSocket;
    req.h.ingressSocket = _socketId;
    req.h.requesterNode = ackNode;
    req.h.homeLinePa = linePa;
    req.h.epoch = epoch;
    req.h.reqId = reqId;
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();

    // Fire-and-forget
    return transportSendReliable(req);
}

// ---- C4: Direct data forward from owner to requester (bypasses home) ----

bool
UBAdapter::sendDirectData(const CoherenceMessage &msg)
{
    if (!_port) return false;
    if (_directOutputs.empty() && transportSend(msg)) return true;
    for (const auto &queued : _directOutputs)
        if (queued.h.reqId == msg.h.reqId && queued.h.homeLinePa == msg.h.homeLinePa &&
            queued.h.dstNode == msg.h.dstNode && queued.h.dstSocket == msg.h.dstSocket)
            return true;
    if (!_directOutputs.push_back(msg)) return false;
    scheduleResponseCheck();
    return true;
}

// ---- Cross-node Recall Request (EPBackend → EPBackend via router) ----

bool
UBAdapter::sendRecallReqToOwner(int targetNode,
                                 const OuterRecallMsg &recallMsg,
                                 int homeSocket)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sendRecallReqToOwner target=%d PA=0x%lx homeSocket=%d\n",
            _nodeId, _socketId, targetNode, recallMsg.linePa, homeSocket);

    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendRecallReqToOwner called with no transport bound\n",
              _nodeId, _socketId);
    }

    CoherenceMessage req;
    req.h.type = CoherenceMessageType::RecallReq;
    req.h.srcNode = _nodeId;
    req.h.srcSocket = homeSocket;  // home's socket plane for sharer routing
    req.h.dstNode = targetNode;
    req.h.dstSocket = homeSocket;  // route through home's socket plane
    req.h.homeNode = recallMsg.homeNode;
    req.h.homeSocket = homeSocket;
    req.h.ingressSocket = homeSocket;
    req.h.requesterNode = recallMsg.requesterNode >= 0 ? recallMsg.requesterNode : _nodeId;  // C4: pass-through requester
    req.h.targetNode = targetNode;
    req.h.homeLinePa = recallMsg.linePa;
    req.h.localLinePa = recallMsg.ownerLocalPa;
    req.h.epoch = recallMsg.epoch;
    req.h.reqId = recallMsg.reqId;
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();

    if (recallMsg.isReadRequest)
        req.h.flags |= static_cast<uint32_t>(CFLAG_IS_READ_RECALL);
    if (recallMsg.dataNeeded)
        req.h.flags |= static_cast<uint32_t>(CFLAG_HAS_DATA);

    // Route intent through the actual Home. It shares the Home's four-credit
    // window rather than fabricating a second sequence domain for that peer.
    req.h.flags |= static_cast<uint32_t>(CFLAG_HOME_CONTROL_RELAY);
    req.h.srcSocket = _socketId;
    req.h.dstNode = recallMsg.homeNode;
    req.h.dstSocket = homeSocket;
    if (!_directOutputs.push_back(req)) {
        // Existing foreground root still owns the unsent intent; retry cannot
        // substitute a different identity. This API must be made fallible.
        return false;
    }
    scheduleResponseCheck();
    return true;
}

// ---- Cross-node Invalidate Request (EPBackend → EPBackend via router) ----

bool
UBAdapter::sendInvalidateReqToSharer(int targetNode,
                                      const OuterInvalidateMsg &invMsg,
                                      int homeSocket)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sendInvalidateReqToSharer target=%d PA=0x%lx homeSocket=%d\n",
            _nodeId, _socketId, targetNode, invMsg.linePa, homeSocket);

    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendInvalidateReqToSharer called with no transport bound\n",
              _nodeId, _socketId);
    }

    CoherenceMessage req;
    req.h.type = CoherenceMessageType::InvalidateReq;
    req.h.srcNode = _nodeId;
    req.h.srcSocket = homeSocket;  // home's socket plane
    req.h.dstNode = targetNode;
    req.h.dstSocket = homeSocket;  // route through home's socket plane
    req.h.homeNode = invMsg.homeNode;
    req.h.homeSocket = homeSocket;
    req.h.ingressSocket = homeSocket;
    req.h.requesterNode = _nodeId;
    req.h.targetNode = targetNode;
    req.h.homeLinePa = invMsg.linePa;
    req.h.localLinePa = invMsg.sharerLocalPa;
    req.h.epoch = invMsg.epoch;
    req.h.reqId = invMsg.reqId;
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();

    req.h.flags |= static_cast<uint32_t>(CFLAG_HOME_CONTROL_RELAY);
    req.h.srcSocket = _socketId;
    req.h.dstNode = invMsg.homeNode;
    req.h.dstSocket = homeSocket;
    if (!_directOutputs.push_back(req)) return false;
    scheduleResponseCheck();
    return true;
}

// ---- v4-dual-socket: QueryLineMetaReq (async with stable reqId) ----

int
UBAdapter::sendQueryLineMetaReq(uint64_t homePa, int homeNode, int homeSocket,
                                 uint64_t &outEpoch, int &outOwnerNode,
                                 bool &outFound,
                                 uint64_t *outReqId,
                                 uint64_t cachedReqId)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sendQueryLineMetaReq homePa=0x%lx "
            "homeNode=%d homeSocket=%d cachedReqId=%lu\n",
            _nodeId, _socketId, homePa, homeNode, homeSocket, cachedReqId);

    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendQueryLineMetaReq called with no transport bound\n",
              _nodeId, _socketId);
    }

    // ── Phase 2 async: retry path — check cached response by exact reqId ──
    if (cachedReqId > 0) {
        PendingKey key{CoherenceMessageType::QueryLineMetaResp, cachedReqId};
        auto it = _readyResponses.find(key);
        if (it != _readyResponses.end()) {
            outFound = it->second.b.queryLineMetaResp.found;
            outEpoch = it->second.b.queryLineMetaResp.epoch;
            outOwnerNode = it->second.b.queryLineMetaResp.ownerNode;
            _readyResponses.erase(it);   // consume
            inform("[QLM-CACHED-REQID] node=%d reqId=%lu pa=0x%lx found=%d epoch=%lu owner=%d",
                   _nodeId, cachedReqId, homePa, outFound, outEpoch,
                   outOwnerNode);
            return outFound ? 0 : -1;
        }
        // Response not yet arrived — the original request is still in-flight.
        // Do NOT send a duplicate; the caller will poll again.
        DPRINTF(RubyEP,
                "[QLM-WAIT-REQID] node=%d reqId=%lu pa=0x%lx — not yet cached\n",
                _nodeId, cachedReqId, homePa);
        return -2;
    }

    // ── Phase 2 async: fresh request — allocate stable reqId ──
    uint64_t reqId = allocLocalReqId();
    if (outReqId) *outReqId = reqId;

    CoherenceMessage req;
    req.h.type = CoherenceMessageType::QueryLineMetaReq;
    req.h.srcNode = _nodeId;
    req.h.srcSocket = _socketId;
    req.h.dstNode = homeNode;
    req.h.dstSocket = homeSocket;
    req.h.homeNode = homeNode;
    req.h.homeSocket = homeSocket;
    req.h.ingressSocket = _socketId;
    req.h.homeLinePa = homePa;
    req.h.reqId = reqId;           // Phase 2 async: unique stable reqId
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();

    _lastResponseValid = false;
    if (!transportSend(req)) {
        if (outReqId) *outReqId = 0;
        return -1;
    }

    // Port async path: schedule check, return pending
    if (_port) {
        scheduleResponseCheck();
        inform("[QLM-SENT] node=%d pa=0x%lx reqId=%lu",
               _nodeId, homePa, reqId);
        return -2;
    }

    // Sync fallback (no port) — not expected in production
    if (!transportRecv(CoherenceMessageType::QueryLineMetaResp, req.h.reqId)) {
        warn("UBAdapter node=%d socket=%d: sendQueryLineMetaReq: no response PA=0x%lx\n",
             _nodeId, _socketId, homePa);
        return -1;
    }

    const CoherenceMessage &resp = _lastResponse;
    if (resp.h.type != CoherenceMessageType::QueryLineMetaResp) {
        warn("UBAdapter node=%d socket=%d: sendQueryLineMetaReq: unexpected response type %s\n",
             _nodeId, _socketId, coherenceMsgTypeName(resp.h.type));
        return -1;
    }

    outFound = resp.b.queryLineMetaResp.found;
    outEpoch = resp.b.queryLineMetaResp.epoch;
    outOwnerNode = resp.b.queryLineMetaResp.ownerNode;
    return outFound ? 0 : -1;
}

// Phase 2 async: try to retrieve a cached QueryLineMetaResp by reqId.
bool
UBAdapter::tryGetQueryLineMetaResp(uint64_t reqId,
                                    uint64_t &outEpoch, int &outOwnerNode,
                                    bool &outFound)
{
    PendingKey key{CoherenceMessageType::QueryLineMetaResp, reqId};
    auto it = _readyResponses.find(key);
    if (it != _readyResponses.end()) {
        outFound = it->second.b.queryLineMetaResp.found;
        outEpoch = it->second.b.queryLineMetaResp.epoch;
        outOwnerNode = it->second.b.queryLineMetaResp.ownerNode;
        // Consume the response so it's not reused for the wrong PA
        _readyResponses.erase(it);
        inform("[QLM-FOUND] node=%d reqId=%lu pa=0x%lx found=%d epoch=%lu owner=%d",
               _nodeId, reqId, outFound ? 0UL : 0UL, outFound, outEpoch,
               outOwnerNode);
        return true;
    }
    return false;
}

// ---- v4-dual-socket: HomeWritebackNotify (fire-and-forget) ----

void
UBAdapter::sendHomeWritebackNotify(uint64_t homePa, uint64_t epoch,
                                    int homeNode, int homeSocket)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sendHomeWritebackNotify homePa=0x%lx "
            "epoch=%lu homeNode=%d homeSocket=%d\n",
            _nodeId, _socketId, homePa, epoch, homeNode, homeSocket);

    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendHomeWritebackNotify called with no transport bound\n",
              _nodeId, _socketId);
    }

    CoherenceMessage req;
    req.h.type = CoherenceMessageType::HomeWritebackNotify;
    req.h.srcNode = _nodeId;
    req.h.srcSocket = _socketId;
    req.h.dstNode = homeNode;
    req.h.dstSocket = homeSocket;
    req.h.homeNode = homeNode;
    req.h.homeSocket = homeSocket;
    req.h.ingressSocket = _socketId;
    req.h.homeLinePa = homePa;
    req.h.epoch = epoch;
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();

    // Fire-and-forget
    (void)transportSend(req);
}

// ---- Receive message from router ----

// Phase 2: typed error response helper for MetaRNF line requests.
// Always makes best-effort to send a response; never silently drops.
void
UBAdapter::sendMetaRNFLineErrorResponse(framework::Port *port,
                               CoherenceMessageType respType,
                               MetaRNFLineStatus st,
                               uint64_t reqId, uint64_t bucketOffset,
                               int nodeId, int nodeSocket,
                               int dstNode, int dstSocket,
                               uint32_t sourceId, uint32_t targetId)
{
    CoherenceMessage resp;
    resp.h.type = respType;
    resp.h.srcNode = static_cast<uint16_t>(nodeId);
    resp.h.srcSocket = static_cast<uint16_t>(nodeSocket);
    resp.h.dstNode = static_cast<uint16_t>(dstNode);
    resp.h.dstSocket = static_cast<uint16_t>(dstSocket);
    resp.h.reqId = reqId;
    if (respType == CoherenceMessageType::MetaRNFLineReadResp) {
        resp.b.metaRNFLineReadResp.status = st;
        resp.b.metaRNFLineReadResp.bucketOffset = bucketOffset;
    } else {
        resp.b.metaRNFLineWriteResp.status = st;
        resp.b.metaRNFLineWriteResp.bucketOffset = bucketOffset;
    }

    if (!sendMetadataReply(resp)) {
        // Port unavailable — log DEBUG-only, not stderr.
        DPRINTF(RubyEP,
                "[DEBUG-PHASE2] node=%d: cannot send %s for reqId=%lu bucketOffset=0x%lx (no port/buffer)\n",
                nodeId, coherenceMsgTypeName(respType), reqId, bucketOffset);
    }
}

void
UBAdapter::recvFromRouter(const CoherenceMessage &msg)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d: recvFromRouter type=%s src=%d dst=%d\n",
            _nodeId, coherenceMsgTypeName(msg.h.type),
            msg.h.srcNode, msg.h.dstNode);

    switch (msg.h.type) {
        case CoherenceMessageType::ReadResp:
            inform("[ADAPTER-GOT-RESP] node=%d type=ReadResp pa=0x%lx src=%d grant=%d epoch=%lu reqId=%lu",
                   _nodeId, msg.h.homeLinePa, msg.h.srcNode,
                   static_cast<int>(msg.b.readResp.grantType), msg.h.epoch,
                   msg.h.reqId);
            _lastResponse = msg;
            _lastResponseValid = true;
            break;
        case CoherenceMessageType::WritebackResp:
        case CoherenceMessageType::EvictResp:
        case CoherenceMessageType::UpgradeResp:
        case CoherenceMessageType::UpgradeDoneResp:
        case CoherenceMessageType::ClearResp: {
            DPRINTF(RubyEP,
                    "[CLEAR-RESP] node=%d via=recvFromRouter pa=0x%lx src=%d "
                    "accepted=%d epoch=%lu reqId=%lu\n",
                    _nodeId, msg.h.homeLinePa, msg.h.srcNode,
                    msg.b.clearResp.accepted ? 1 : 0, msg.h.epoch,
                    msg.h.reqId);
            _lastResponse = msg;
            _lastResponseValid = true;
            cacheResponse(msg);
            break;
        }
        case CoherenceMessageType::QueryLineMetaResp:
            // Store for caller AND in _readyResponses keyed by reqId (Phase 2 async)
            _lastResponse = msg;
            _lastResponseValid = true;
            {
                cacheResponse(msg);
            }
            break;
        case CoherenceMessageType::HAPermissionResp:
        case CoherenceMessageType::HAPresenceProbeResp: {
            _lastResponse = msg;
            _lastResponseValid = true;
            cacheResponse(msg);
            break;
        }
        case CoherenceMessageType::HAPresenceProbeReq:
            if (_backend)
                _backend->handleHAPresenceProbeRequest(msg, this);
            else
                warn("UBAdapter node=%d: HA probe received without backend", _nodeId);
            break;
        case CoherenceMessageType::HAPermissionReq:
        {
            const unsigned peer = msg.h.srcNode * _numSockets + msg.h.srcSocket;
            if (peer >= unsigned(_numNodes * _numSockets)) return;
            MetadataReply *reservation = nullptr;
            for (unsigned i = peer * 64; i < (peer + 1) * 64; ++i) {
                auto &out = _permissionReplies[i];
                if (out.reserved && out.message.h.reqId == msg.h.reqId) return;
                if (!out.reserved && !reservation) reservation = &out;
            }
            // A source holds at most64 permission response promises. Each
            // grants the destination one reply cell before callback dispatch.
            panic_if(!reservation, "permission sender exceeded response credit window");
            reservation->message.h = msg.h;
            reservation->reserved = true;
            if (_backend)
                _backend->handleHAPermissionRequest(msg, this);
            else
                warn("UBAdapter node=%d: HA permission request without backend",
                     _nodeId);
            break;
        }
        case CoherenceMessageType::HAPermissionAck:
            // Ack is terminal at the endpoint.  The HA controller owns any
            // permission transaction state; gem5 intentionally has none here.
            break;

        case CoherenceMessageType::UpgradeAckNotify: {
            // Async notification from UBCC: all invalidation acks received
            // for an upgrade. Forward to EPBackend::notifyUpgradeAckReady().
            if (_backend) {
                _backend->notifyUpgradeAckReady(msg.h.homeLinePa);
            } else {
                warn("UBAdapter node=%d: UpgradeAckNotify received but no EPBackend bound\n",
                     _nodeId);
            }
            break;
        }

        case CoherenceMessageType::RecallReq: {
            // Reconstruct OuterRecallMsg from CoherenceMessage and deliver to EPBackend
            OuterRecallMsg recallMsg;
            recallMsg.linePa = msg.h.homeLinePa;
            recallMsg.ownerLocalPa = msg.h.localLinePa;
            recallMsg.ownerNode = msg.h.targetNode;
            recallMsg.homeNode = msg.h.homeNode;
            recallMsg.epoch = msg.h.epoch;
            recallMsg.reqId = msg.h.reqId;
            recallMsg.isReadRequest =
                (msg.h.flags & static_cast<uint32_t>(CFLAG_IS_READ_RECALL)) != 0;
            recallMsg.dataNeeded =
                (msg.h.flags & static_cast<uint32_t>(CFLAG_HAS_DATA)) != 0;

            // C4: Extract direct-forward target from requesterNode header field
            recallMsg.requesterNode = msg.h.requesterNode;
            recallMsg.requesterSocket = msg.h.ingressSocket;
            recallMsg.sourceSocket = _socketId;

            if (_backend) {
                _backend->handleRecallRequest(recallMsg);
            } else {
                warn("UBAdapter node=%d: RecallReq received but no EPBackend bound\n",
                     _nodeId);
            }
            break;
        }

        case CoherenceMessageType::InvalidateReq: {
            // Reconstruct OuterInvalidateMsg from CoherenceMessage and deliver to EPBackend
            OuterInvalidateMsg invMsg;
            invMsg.linePa = msg.h.homeLinePa;
            invMsg.sharerLocalPa = msg.h.localLinePa;
            invMsg.sharerNode = msg.h.targetNode;
            invMsg.homeNode = msg.h.homeNode;
            invMsg.sourceSocket = _socketId;
            invMsg.epoch = msg.h.epoch;
            invMsg.reqId = msg.h.reqId;

            if (_backend) {
                _backend->handleInvalidationRequest(invMsg);
            } else {
                warn("UBAdapter node=%d: InvalidateReq received but no EPBackend bound\n",
                     _nodeId);
            }
            break;
        }

        case CoherenceMessageType::MetaRNFReadReq: {
            if (!reserveMetadataReply(msg)) break;
            // UBIO's backstore schema uses compact page IDs. Map the ID into
            // this socket's metadata DRAM range before issuing CHI accesses.
            uint64_t pageId = msg.h.homeLinePa;
            auto *metaRNF = MetaRNFController::getInstance(_nodeId, _socketId);
            if (!metaRNF) {
                CoherenceMessage error;
                error.h = msg.h;
                error.h.type = CoherenceMessageType::MetaRNFReadResp;
                std::swap(error.h.srcNode, error.h.dstNode);
                std::swap(error.h.srcSocket, error.h.dstSocket);
                error.b.metaRNF = UBMetaRNFBody{};
                sendMetadataReply(error);
                break;
            }
            constexpr uint64_t pageBytes = 256;
            uint64_t pagePa = metaRNF->metadataRangeStart() + pageId * pageBytes;
            uint64_t reqId = msg.h.reqId;
            if (pagePa < metaRNF->metadataRangeStart() ||
                pagePa + pageBytes > metaRNF->metadataRangeEnd()) {
                warn("UBAdapter node=%d: metadata page ID 0x%lx out of range",
                     _nodeId, pageId);
                CoherenceMessage error;
                error.h = msg.h;
                error.h.type = CoherenceMessageType::MetaRNFReadResp;
                std::swap(error.h.srcNode, error.h.dstNode);
                std::swap(error.h.srcSocket, error.h.dstSocket);
                error.b.metaRNF = UBMetaRNFBody{};
                sendMetadataReply(error);
                break;
            }
            auto tport = _port;
            const int srcNode = _nodeId;
            const int srcSocket = _socketId;
            const int dstNode = msg.h.srcNode;
            const int dstSocket = msg.h.srcSocket;
            const uint32_t sourceId =
                static_cast<uint32_t>(srcNode * _numSockets + srcSocket);
            const uint32_t targetId =
                static_cast<uint32_t>(dstNode * _numSockets + dstSocket);
            struct MRState { int done; uint8_t buf[256]; };
            auto *state = new MRState{0, {}};
            for (int i = 0; i < 4; i++) {
                uint64_t blockPa = pagePa + i * 64;
                metaRNF->issueRead(blockPa,
                    [this, tport, reqId, state, i, pageId, srcNode, srcSocket,
                     dstNode, dstSocket, sourceId, targetId]
                    (bool ok, const MetaRNFController::MetaLine &db) {
                    if (ok) memcpy(&state->buf[i*64], db.data(), 64);
                    if (++state->done == 4) {
                        CoherenceMessage resp;
                        resp.h.type = CoherenceMessageType::MetaRNFReadResp;
                        resp.h.srcNode = static_cast<uint16_t>(srcNode);
                        resp.h.srcSocket = static_cast<uint16_t>(srcSocket);
                        resp.h.dstNode = static_cast<uint16_t>(dstNode);
                        resp.h.dstSocket = static_cast<uint16_t>(dstSocket);
                        resp.h.reqId = reqId;
                        resp.h.homeLinePa = pageId;
                        resp.b.metaRNF.pagePa = pageId;
                        memcpy(resp.b.metaRNF.data, state->buf, 256);
                        if (!sendMetadataReply(resp))
                            warn("UBAdapter: failed to send MetaRNFReadResp reqId=%lu",
                                 reqId);
                        delete state;
                    }
                });
            }
            break;
        }
        case CoherenceMessageType::MetaRNFWriteReq: {
            if (!reserveMetadataReply(msg)) break;
            uint64_t pageId = msg.h.homeLinePa;
            auto *metaRNF = MetaRNFController::getInstance(_nodeId, _socketId);
            if (!metaRNF) {
                CoherenceMessage error;
                error.h = msg.h;
                error.h.type = CoherenceMessageType::MetaRNFWriteResp;
                std::swap(error.h.srcNode, error.h.dstNode);
                std::swap(error.h.srcSocket, error.h.dstSocket);
                error.h.flags = 0;
                sendMetadataReply(error);
                break;
            }
            constexpr uint64_t pageBytes = 256;
            uint64_t pagePa = metaRNF->metadataRangeStart() + pageId * pageBytes;
            if (pagePa < metaRNF->metadataRangeStart() ||
                pagePa + pageBytes > metaRNF->metadataRangeEnd()) {
                warn("UBAdapter node=%d: metadata page ID 0x%lx out of range",
                     _nodeId, pageId);
                CoherenceMessage error;
                error.h = msg.h;
                error.h.type = CoherenceMessageType::MetaRNFWriteResp;
                std::swap(error.h.srcNode, error.h.dstNode);
                std::swap(error.h.srcSocket, error.h.dstSocket);
                error.h.flags = 0;
                sendMetadataReply(error);
                break;
            }
            // Phase D2: track completion of all 4 sub-writes, then send
            // MetaRNFWriteResp back to UBIO with aggregate success/failure.
            inform("[D2-MRNFRECV] node=%d pageId=0x%lx pagePa=0x%lx",
                   _nodeId, pageId, pagePa);
            struct WriteState {
                int pending = 4;
                bool anyFailed = false;
                framework::Port *port;
                uint64_t reqId;
                int nodeId;
                int socketId;
                int dstNode;
                int dstSocket;
                uint32_t sourceId;
                uint32_t targetId;
                uint64_t pagePa;
            };
            auto *ws = new WriteState;
            ws->port = _port;
            ws->reqId = msg.h.reqId;
            ws->nodeId = _nodeId;
            ws->socketId = _socketId;
            ws->dstNode = msg.h.srcNode;
            ws->dstSocket = msg.h.srcSocket;
            ws->sourceId = static_cast<uint32_t>(
                _nodeId * _numSockets + _socketId);
            ws->targetId = static_cast<uint32_t>(
                msg.h.srcNode * _numSockets + msg.h.srcSocket);
            ws->pagePa = pagePa;
            for (int i = 0; i < 4; i++) {
                MetaRNFController::MetaLine ml;
                memcpy(ml.data(), &msg.b.metaRNF.data[i * 64], 64);
                metaRNF->issueWrite(pagePa + i * 64, ml,
                    [this, ws](bool ok) {
                        if (!ok) ws->anyFailed = true;
                        if (--ws->pending > 0) return;
                        // All 4 sub-writes done — send ack to UBIO
                        inform("[D2-WRITE-CB] node=%d pagePa=0x%lx anyFailed=%d port=%p",
                               ws->nodeId, ws->pagePa,
                               ws->anyFailed ? 1 : 0, (void*)ws->port);
                        CoherenceMessage resp;
                        resp.h.type = CoherenceMessageType::MetaRNFWriteResp;
                        resp.h.srcNode = ws->nodeId;
                        resp.h.srcSocket = ws->socketId;
                        resp.h.dstNode = ws->dstNode;
                        resp.h.dstSocket = ws->dstSocket;
                        resp.h.homeLinePa = ws->pagePa;
                        resp.h.reqId = ws->reqId;
                        resp.b.metaRNF.pagePa = ws->pagePa;
                        // Use flags bit 0 to signal success/failure
                        if (ws->anyFailed)
                            resp.h.flags = 0;
                        else
                            resp.h.flags = 1;  // D2: bit 0 = durable
                        if (!sendMetadataReply(resp))
                            warn("UBAdapter: failed to send MetaRNFWriteResp reqId=%lu",
                                 ws->reqId);
                        delete ws;
                    });
            }
            break;
        }

        // ---- Phase 2+3: 64B line operations with typed status ----
        // Req B: ubio sends logical bucketOffset; UBAdapter computes physical PA.
        case CoherenceMessageType::MetaRNFLineReadReq: {
            if (!reserveMetadataReply(msg)) break;
            uint64_t bucketOffset = msg.b.metaRNFLineReadReq.bucketOffset;
            auto *metaRNF = MetaRNFController::getInstance(_nodeId, _socketId);
            if (!metaRNF) {
                sendMetaRNFLineErrorResponse(_port,
                    CoherenceMessageType::MetaRNFLineReadResp,
                    MetaRNFLineStatus::IoError,
                    msg.h.reqId, bucketOffset, _nodeId, _socketId,
                    msg.h.srcNode, msg.h.srcSocket,
                    static_cast<uint32_t>(
                        _nodeId * _numSockets + _socketId),
                    static_cast<uint32_t>(
                        msg.h.srcNode * _numSockets + msg.h.srcSocket));
                break;
            }
            uint64_t physPa = metaRNF->metadataRangeStart() +
                              bucketOffset * 64ULL;
            if (physPa < metaRNF->metadataRangeStart() ||
                physPa + 64 > metaRNF->metadataRangeEnd()) {
                sendMetaRNFLineErrorResponse(_port,
                    CoherenceMessageType::MetaRNFLineReadResp,
                    MetaRNFLineStatus::RangeError,
                    msg.h.reqId, bucketOffset, _nodeId, _socketId,
                    msg.h.srcNode, msg.h.srcSocket,
                    static_cast<uint32_t>(
                        _nodeId * _numSockets + _socketId),
                    static_cast<uint32_t>(
                        msg.h.srcNode * _numSockets + msg.h.srcSocket));
                break;
            }
            uint64_t reqId = msg.h.reqId;
            int dstNode = msg.h.srcNode;
            int dstSocket = msg.h.srcSocket;
            const int srcSocket = _socketId;
            const uint32_t sourceId = static_cast<uint32_t>(
                _nodeId * _numSockets + _socketId);
            const uint32_t targetId = static_cast<uint32_t>(
                dstNode * _numSockets + dstSocket);
            auto tport = _port;
            metaRNF->issueReadLine(physPa,
                [this, tport, reqId, bucketOffset, nodeId = _nodeId, srcSocket,
                 dstNode, dstSocket, sourceId, targetId]
                (MetaRNFLineStatus st, const MetaRNFController::MetaLine &data) {
                    CoherenceMessage resp;
                    resp.h.type = CoherenceMessageType::MetaRNFLineReadResp;
                    resp.h.srcNode = static_cast<uint16_t>(nodeId);
                    resp.h.srcSocket = static_cast<uint16_t>(srcSocket);
                    resp.h.dstNode = static_cast<uint16_t>(dstNode);
                    resp.h.dstSocket = static_cast<uint16_t>(dstSocket);
                    resp.h.reqId = reqId;
                    resp.b.metaRNFLineReadResp.status = st;
                    resp.b.metaRNFLineReadResp.bucketOffset = bucketOffset;
                    if (st == MetaRNFLineStatus::Ok)
                        memcpy(resp.b.metaRNFLineReadResp.data, data.data(), 64);
                    if (!sendMetadataReply(resp))
                        warn("UBAdapter: failed to send MetaRNFLineReadResp reqId=%lu",
                             reqId);
                });
            break;
        }
        case CoherenceMessageType::MetaRNFLineWriteReq: {
            if (!reserveMetadataReply(msg)) break;
            uint64_t bucketOffset = msg.b.metaRNFLineWriteReq.bucketOffset;
            auto *metaRNF = MetaRNFController::getInstance(_nodeId, _socketId);
            if (!metaRNF) {
                sendMetaRNFLineErrorResponse(_port,
                    CoherenceMessageType::MetaRNFLineWriteResp,
                    MetaRNFLineStatus::IoError,
                    msg.h.reqId, bucketOffset, _nodeId, _socketId,
                    msg.h.srcNode, msg.h.srcSocket,
                    static_cast<uint32_t>(
                        _nodeId * _numSockets + _socketId),
                    static_cast<uint32_t>(
                        msg.h.srcNode * _numSockets + msg.h.srcSocket));
                break;
            }
            uint64_t physPa = metaRNF->metadataRangeStart() +
                              bucketOffset * 64ULL;
            if (physPa < metaRNF->metadataRangeStart() ||
                physPa + 64 > metaRNF->metadataRangeEnd()) {
                sendMetaRNFLineErrorResponse(_port,
                    CoherenceMessageType::MetaRNFLineWriteResp,
                    MetaRNFLineStatus::RangeError,
                    msg.h.reqId, bucketOffset, _nodeId, _socketId,
                    msg.h.srcNode, msg.h.srcSocket,
                    static_cast<uint32_t>(
                        _nodeId * _numSockets + _socketId),
                    static_cast<uint32_t>(
                        msg.h.srcNode * _numSockets + msg.h.srcSocket));
                break;
            }
            uint64_t reqId = msg.h.reqId;
            int dstNode = msg.h.srcNode;
            int dstSocket = msg.h.srcSocket;
            const int srcSocket = _socketId;
            const uint32_t sourceId = static_cast<uint32_t>(
                _nodeId * _numSockets + _socketId);
            const uint32_t targetId = static_cast<uint32_t>(
                dstNode * _numSockets + dstSocket);
            auto tport = _port;
            MetaRNFController::MetaLine ml;
            memcpy(ml.data(), msg.b.metaRNFLineWriteReq.data, 64);
            metaRNF->issueWriteLine(physPa, ml,
                [this, tport, reqId, bucketOffset, nodeId = _nodeId, srcSocket,
                 dstNode, dstSocket, sourceId, targetId]
                (MetaRNFLineStatus st) {
                    CoherenceMessage resp;
                    resp.h.type = CoherenceMessageType::MetaRNFLineWriteResp;
                    resp.h.srcNode = static_cast<uint16_t>(nodeId);
                    resp.h.srcSocket = static_cast<uint16_t>(srcSocket);
                    resp.h.dstNode = static_cast<uint16_t>(dstNode);
                    resp.h.dstSocket = static_cast<uint16_t>(dstSocket);
                    resp.h.reqId = reqId;
                    resp.b.metaRNFLineWriteResp.status = st;
                    resp.b.metaRNFLineWriteResp.bucketOffset = bucketOffset;
                    if (!sendMetadataReply(resp))
                        warn("UBAdapter: failed to send MetaRNFLineWriteResp reqId=%lu",
                             reqId);
                });
            break;
        }

        default:
            warn("UBAdapter node=%d: unhandled message type %s\n",
                 _nodeId, coherenceMsgTypeName(msg.h.type));
            break;
    }
}

// ---- Event-driven response processing (Step 1) ----

void
UBAdapter::registerClockAdapter(UBAdapter *adapter)
{
    if (!adapter || !adapter->_port)
        return;
    if (std::find(_clockAdapters.begin(), _clockAdapters.end(), adapter) ==
            _clockAdapters.end()) {
        _clockAdapters.push_back(adapter);
    }
    if (!_clockOwner)
        _clockOwner = adapter;
}

void
UBAdapter::unregisterClockAdapter(UBAdapter *adapter)
{
    _clockAdapters.erase(
        std::remove(_clockAdapters.begin(), _clockAdapters.end(), adapter),
        _clockAdapters.end());
    if (_clockOwner == adapter) {
        if (adapter->_responseCheckEvent.scheduled())
            adapter->deschedule(adapter->_responseCheckEvent);
        adapter->_eventArmed = false;
        _clockOwner = _clockAdapters.empty() ? nullptr : _clockAdapters.front();
        if (_clockOwner)
            armClockPump(curTick());
    }
}

void
UBAdapter::armClockPump(Tick when)
{
    if (!_clockOwner || !_clockOwner->_port || _clockPumpRunning)
        return;
    panic_if(when < curTick(),
             "UBAdapter clock pump cannot schedule in the past: now=%lu when=%lu",
             curTick(), when);
    if (_clockOwner->_responseCheckEvent.scheduled()) {
        when = std::min(when, _clockOwner->_responseCheckEvent.when());
        if (_clockOwner->_responseCheckEvent.when() != when)
            _clockOwner->reschedule(_clockOwner->_responseCheckEvent, when);
    } else {
        _clockOwner->schedule(_clockOwner->_responseCheckEvent, when);
    }
    _clockOwner->_eventArmed = true;
}

size_t
UBAdapter::pollVisibleMessages(Tick curT, size_t budget)
{
    if (!_port)
        return 0;
    size_t processed = 0;
    while (processed < budget) {
        framework::ReceiveStatus st;
        const framework::Message *m =
            framework::ReceiveMessage(_port, curT, &st);
        if (!m || st != framework::ReceiveStatus::Message)
            break;
        ++processed;
        if (framework::GetMessageType(m) == framework::MessageType::Terminate ||
            framework::GetMessageType(m) == framework::MessageType::ControlSync) {
            continue;
        }
        if (framework::GetMessageType(m) != framework::MessageType::Payload) {
            static int noncoh = 0;
            if (++noncoh <= 5) {
                DPRINTF(RubyEP,
                        "[WAKEUP-NONCOH] node=%d type=%u payload_sz=%zu\n",
                        _nodeId,
                        static_cast<unsigned>(framework::GetMessageType(m)),
                        framework::GetMessagePayloadSize(m));
            }
            continue;
        }
        if (const CoherenceMessage *bc = coherencePayload(m)) {
            if (bc->h.type == CoherenceMessageType::BarrierRelease) {
                inform("[UBADAPTER-BARRIER-RELEASE] node=%d mask=0x%x seq=%u",
                       _nodeId, bc->b.barrier.mask, bc->b.barrier.seq);
                if (!System::systemList.empty()) {
                    System::systemList[0]->syncWait.releaseBarrier(
                        bc->b.barrier.mask, bc->b.barrier.seq);
                }
                continue;
            }
            if (bc->h.type == CoherenceMessageType::BarrierReached) {
                continue;
            }
            if (TracePerfPolicy::get().shouldEmit("gem5")) {
                inform("[TRACE-PERF] %lu|%d|gem5|%lu|0x%lx|RECV|%s|src=%d",
                       framework::GetMessageTimestamp(m), _nodeId, bc->h.reqId,
                       bc->h.homeLinePa, coherenceMsgTypeName(bc->h.type),
                       bc->h.srcNode);
            }
        }
        static int cohcnt = 0;
        if (++cohcnt <= 5) {
            DPRINTF(RubyEP,
                    "[WAKEUP-COH] node=%d type=%u payload_sz=%zu req_id=%lu\n",
                    _nodeId,
                    static_cast<unsigned>(framework::GetMessageType(m)),
                    framework::GetMessagePayloadSize(m),
                    framework::GetMessageRequestId(m));
        }
        handleResponse(m);
    }
    drainDeferredControls();
    return processed;
}

void
UBAdapter::wakeup()
{
    if (this != _clockOwner || !_port)
        return;
    _clockPumpRunning = true;
    _eventArmed = false;
    ++_responseCheckCount;
    const uint64_t curT = curTick();
    uint64_t minSafe = curT;
    uint64_t waitRounds = 0;
    do {
        minSafe = std::numeric_limits<uint64_t>::max();
        bool syncFailed = false;
        for (UBAdapter *adapter : _clockAdapters) {
            if (!adapter || !adapter->_port)
                continue;
            adapter->drainReliableOutputs();
            syncFailed |= !framework::EmitSync(adapter->_port, curT);
            adapter->pollVisibleMessages(curT, 64);
            minSafe = std::min(
                minSafe, framework::SafeTimestamp(adapter->_port, curT));
        }
        if (minSafe > curT)
            break;
        ++waitRounds;
        if (waitRounds % 2000 == 0) {
            DPRINTF(RubyEP,
                    "[DEBUG-CLK-SYNC] node=%d curT=%lu safeT=%lu WAIT "
                    "round=%lu adapters=%lu\n",
                    _nodeId, curT, minSafe, waitRounds,
                    static_cast<unsigned long>(_clockAdapters.size()));
        }
        if (syncFailed)
            std::this_thread::sleep_for(std::chrono::microseconds(50));
        else
            std::this_thread::yield();
    } while (true);

    _clockPumpRunning = false;
    panic_if(minSafe <= curT,
             "UBAdapter clock pump attempted same-tick scheduling: now=%lu safe=%lu",
             curT, minSafe);
    armClockPump(minSafe);
}

void
UBAdapter::handleResponse(const framework::Message *m)
{
    if (!_port) return;

    const CoherenceMessage *coh = coherencePayload(m);
    DPRINTF(RubyEP,
            "[DEBUG-UBADAPTER-RECV] node=%d msg_type=%u payload_sz=%zu payload=%s coh_type=%d reqId=%lu\n",
            _nodeId, static_cast<unsigned>(framework::GetMessageType(m)),
            framework::GetMessagePayloadSize(m),
            coh ? "OK" : "NULL",
            coh ? static_cast<int>(coh->h.type) : -1,
            coh ? coh->h.reqId : 0UL);
    if (!coh) return;

    if (coh->h.type == CoherenceMessageType::ClearResp) {
        DPRINTF(RubyEP,
                "[CLEAR-RESP] node=%d via=handleResponse pa=0x%lx src=%d "
                "accepted=%d epoch=%lu reqId=%lu\n",
                _nodeId, coh->h.homeLinePa, coh->h.srcNode,
                coh->b.clearResp.accepted ? 1 : 0, coh->h.epoch,
                coh->h.reqId);
    }
    if (coh->h.type == CoherenceMessageType::UpgradeResp) {
        inform("[UPGRADE-FORENSIC] stage=GEM5_RESP_RECV node=%d socket=%d "
               "pa=0x%lx reqId=%lu epoch=%lu flags=0x%x msgTs=%lu curT=%lu",
               _nodeId, _socketId, coh->h.homeLinePa, coh->h.reqId,
               coh->h.epoch, coh->h.flags,
               framework::GetMessageTimestamp(m), curTick());
    }

    // Async control messages: enqueue FIFO, process later via drainDeferredControls
    if (coh->h.type == CoherenceMessageType::UpgradeAckNotify) {
        recvFromRouter(*coh);
        return;
    }
    if (coh->h.type == CoherenceMessageType::HAPermissionReq) {
        recvFromRouter(*coh);
        return;
    }
    if (coh->h.type == CoherenceMessageType::RecallReq ||
        coh->h.type == CoherenceMessageType::InvalidateReq ||
        coh->h.type == CoherenceMessageType::HAPresenceProbeReq) {
        const unsigned peer = coh->h.srcNode * _numSockets + coh->h.srcSocket;
        using Receipt = decltype(_controlReceipts)::Receipt;
        const auto result = _controlReceipts.receive(peer, coh->h);
        panic_if(result == Receipt::Invalid,
                 "Home control violated receive lease node=%d peer=%u reqId=%lu lease=%lu",
                 _nodeId, peer, coh->h.reqId, coh->h.seqNum);
        if (result == Receipt::Stale) return;
        if (result == Receipt::New) {
            // A higher lease can only be issued after Home received the prior
            // response. Cancel a queued replay of that retired response before
            // its slot is reused by the new native callback.
            _controlOutputs[peer][coh->h.seqNum & 3].queued = false;
        }
        if (result == Receipt::Duplicate) {
            const auto *receipt = _controlReceipts.get(peer, coh->h.seqNum & 3);
            if (!receipt->active) {
                auto &out = _controlOutputs[peer][coh->h.seqNum & 3];
                out.queued = true;
                drainReliableOutputs();
            }
            return;
        }
    }
    switch (coh->h.type) {
      case CoherenceMessageType::InvalidateReq:
      case CoherenceMessageType::RecallReq:
      case CoherenceMessageType::HAPresenceProbeReq:
        inform("[ASYNC-CTRL-ENQ] node=%d type=%s reqId=%lu pa=0x%lx src=%d dst=%d curT=%lu depth=%zu",
               _nodeId, coherenceMsgTypeName(coh->h.type), coh->h.reqId,
               coh->h.homeLinePa, coh->h.srcNode, coh->h.dstNode,
               curTick(), _deferredControls.size() + 1);
        panic_if(!_deferredControls.push_back(cc::glob::ControlFrame(*coh)),
                 "control lease accounting exceeded reserved receive storage");
        return;
      default:
        break;
    }

    // UBIO initiates MetaRNF requests through this Port. They are requests to
    // gem5, not responses to a gem5 transaction, so they must not enter the
    // response cache below.
    switch (coh->h.type) {
      case CoherenceMessageType::MetaRNFReadReq:
      case CoherenceMessageType::MetaRNFWriteReq:
      case CoherenceMessageType::MetaRNFLineReadReq:
      case CoherenceMessageType::MetaRNFLineWriteReq:
      case CoherenceMessageType::HAPermissionAck:
        recvFromRouter(*coh);
        return;
      default:
        break;
    }

    // RecallResp is consumed by the home UBCC first; if ubio mirrors it back
    // to local gem5, use it only as an event-driven wakeup so the requester's
    // EP-SNF retry queue replays immediately after RECALL.DONE instead of
    // idling for the 20k-cycle fallback backoff.
    if (coh->h.type == CoherenceMessageType::RecallResp) {
        if (_onResponseWired) {
            DPRINTF(RubyEP,
                    "[RSP-WIRED] node=%d socket=%d firing recall-done wakeup reqId=%lu\n",
                    _nodeId, _socketId, coh->h.reqId);
            _onResponseWired();
        }
        return;
    }

    // Responses are consumed by the retry-based callers using exact identity.
    // Store in ready-response cache for retry-based sendReadReq
    cacheResponse(*coh);

    // The read reservation includes the response slot. Keep it until the
    // exact retry consumes the payload, not merely until transport arrival.
    if (coh->h.type == CoherenceMessageType::ClearResp)
    {
        _inflightClearReqs.erase(coh->h.reqId);
        _clearRetryTick.erase(coh->h.reqId);
    }

    // Immediate response notification: fire the wired callback so the
    // EPSNFController wakes up NOW and processes this response via retry,
    // instead of waiting for the next EP_RETRY_CYCLES interval.
    if (_onResponseWired && (coh->h.type == CoherenceMessageType::ReadResp ||
                              coh->h.type == CoherenceMessageType::ClearResp ||
                              coh->h.type == CoherenceMessageType::UpgradeResp ||
                              coh->h.type == CoherenceMessageType::WritebackResp ||
                               coh->h.type == CoherenceMessageType::EvictResp ||
                               coh->h.type == CoherenceMessageType::QueryLineMetaResp ||
                               coh->h.type == CoherenceMessageType::HAPermissionResp ||
                               coh->h.type == CoherenceMessageType::HAPresenceProbeResp)) {
        DPRINTF(RubyEP,
                "[RSP-WIRED] node=%d socket=%d firing immediate wakeup for type=%d reqId=%lu\n",
                _nodeId, _socketId, static_cast<int>(coh->h.type),
                coh->h.reqId);
        _onResponseWired();
    }

    // Event-driven completion of a held SnpCleanInvalid-upgrade: an
    // OuterUpgradeResp is now cached in _readyResponses, so a checkOnly retry
    // will succeed. Proactively drive the held upgrade instead of relying on
    // the snoop being re-issued to pull it (busy-wait livelock; TC16/25/53).
    // No-other-sharers upgrades send no UpgradeAckNotify, so this is the only
    // event that can complete them.
    if (coh->h.type == CoherenceMessageType::UpgradeResp && _backend) {
        _backend->onUpgradeRespArrived(coh->h.reqId);
    }
}

void
UBAdapter::scheduleResponseCheck()
{
    if (_port)
        armClockPump(curTick() + 10);
}

uint64_t
UBAdapter::allocLocalReqId()
{
    uint64_t id = _nextLocalReqId++;
    if (id == 0) id = _nextLocalReqId++;
    return id;
}

void
UBAdapter::clearReadyResponsesForLine(
    uint64_t linePa, CoherenceMessageType responseType)
{
    for (auto it = _readyResponses.begin(); it != _readyResponses.end(); ) {
        if (it->first.respType == responseType &&
            (it->second.h.homeLinePa == linePa ||
             it->second.h.localLinePa == linePa)) {
            inform("[RSP-CACHE-CLEAR] node=%d socket=%d type=%s reqId=%lu "
                   "pa=0x%lx",
                   _nodeId, _socketId, coherenceMsgTypeName(responseType),
                   it->second.h.reqId, linePa);
            it = _readyResponses.erase(it);
        } else {
            ++it;
        }
    }
}

void
UBAdapter::drainDeferredControls()
{
    if (_drainingDeferredControls) return;
    _drainingDeferredControls = true;
    for (auto selected = _deferredControls.begin(); selected != _deferredControls.end(); ) {
        // Independent lines may drain around a blocked control. A failed
        // backend admission retains the exact original payload, not a fake ACK.
        CoherenceMessage msg = selected->expand();
        const auto pa = msg.h.localLinePa ? msg.h.localLinePa : msg.h.homeLinePa;
        if (_backend && msg.h.type == CoherenceMessageType::RecallReq) {
            if (!_backend->canAcceptRecall(pa, msg.h.reqId, _socketId)) {
                ++selected;
                continue;
            }
            OuterRecallMsg recall;
            recall.linePa = msg.h.homeLinePa;
            recall.ownerLocalPa = msg.h.localLinePa;
            recall.ownerNode = msg.h.targetNode;
            recall.homeNode = msg.h.homeNode;
            recall.epoch = msg.h.epoch;
            recall.reqId = msg.h.reqId;
            recall.isReadRequest = msg.h.flags & static_cast<uint32_t>(CFLAG_IS_READ_RECALL);
            recall.dataNeeded = msg.h.flags & static_cast<uint32_t>(CFLAG_HAS_DATA);
            recall.requesterNode = msg.h.requesterNode;
            recall.requesterSocket = msg.h.ingressSocket;
            recall.sourceSocket = _socketId;
            if (!_backend->handleRecallRequest(recall)) {
                ++selected;
                continue;
            }
            selected = _deferredControls.erase(selected);
            continue;
        }
        if (msg.h.type == CoherenceMessageType::InvalidateReq && _backend) {
            if (!_backend->canAcceptInvalidation(pa, msg.h.reqId, _socketId)) {
                ++selected;
                continue;
            }
            OuterInvalidateMsg inv;
            inv.linePa = msg.h.homeLinePa;
            inv.sharerLocalPa = msg.h.localLinePa;
            inv.sharerNode = msg.h.targetNode;
            inv.homeNode = msg.h.homeNode;
            inv.sourceSocket = _socketId;
            inv.epoch = msg.h.epoch;
            inv.reqId = msg.h.reqId;
            if (!_backend->handleInvalidationRequest(inv)) {
                ++selected;
                continue;
            }
            selected = _deferredControls.erase(selected);
            continue;
        }
        selected = _deferredControls.erase(selected);
        inform("[ASYNC-CTRL-DRAIN] node=%d type=%s reqId=%lu pa=0x%lx curT=%lu remaining=%zu",
               _nodeId, coherenceMsgTypeName(msg.h.type), msg.h.reqId,
               msg.h.homeLinePa, curTick(), _deferredControls.size());
        recvFromRouter(msg);
    }
    _drainingDeferredControls = false;
}

} // namespace ruby
} // namespace gem5
