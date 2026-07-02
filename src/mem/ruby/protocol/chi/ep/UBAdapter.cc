#include "mem/ruby/protocol/chi/ep/UBAdapter.hh"

#include <cstdio>
#include <limits>
#include <thread>

#include "base/logging.hh"
#include "debug/RubyEP.hh"
#include "framework/MemMessage.hh"
#include "framework/Port.hh"
#include "mem/ruby/protocol/chi/ep/EPBackend.hh"
#include "sim/core.hh"
#include "sim/cur_tick.hh"
#include "sim/sync_wait.hh"
#include "sim/system.hh"

namespace gem5
{
namespace ruby
{

UBAdapter::UBAdapter(const Params &p)
    : SimObject(p),
      _nodeId(p.node_id),
      _socketId(p.socket_id),
      _numNodes(p.num_nodes),
      _numSockets(p.num_sockets),
      _localNode(p.local_node),
      _addrMap(p.num_nodes, p.num_sockets, 128ULL * 1024 * 1024),
      _responseCheckEvent([this]{ wakeup(); }, name() + ".responseCheck")
{
    fatal_if(sizeof(CoherenceMessage) > framework::kMaxPayloadSize,
             "UBAdapter: CoherenceMessage size (%zu) exceeds MemMessage payload (%u)",
             sizeof(CoherenceMessage), framework::kMaxPayloadSize);
    DPRINTF(RubyEP, "UBAdapter node=%d socket=%d created\n", _nodeId, _socketId);
}

UBAdapter::~UBAdapter()
{
}

void
UBAdapter::init()
{
    SimObject::init();

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
            int numSockets = _numSockets;
            int gid = _nodeId * numSockets + _socketId;
            framework::PortParams pp = framework::PortEnvLoader::gem5UbioPort(gid);
            _port = new framework::Port();
            if (!_port->init(pp)) {
                std::fprintf(stderr, "[UBAdapter] node=%d socket=%d Port init failed\n",
                             _nodeId, _socketId);
                delete _port; _port = nullptr;
            } else {
                std::printf("[Port gem5_ubio] n=%d s=%d gid=%d rx=%s tx->%s\n",
                            _nodeId, _socketId, gid,
                            pp.localRxEndpoint.c_str(), pp.peerRxEndpoint.c_str());
                std::printf("STEP5 Port enabled node=%d socket=%d gid=%d\n",
                            _nodeId, _socketId, gid);
                // Flush: this C++ stdio buffer is separate from Python's stdout;
                // the launcher greps the log for STEP5 to detect Port binding, so
                // it must reach the file immediately (not wait for buffer fill).
                std::fflush(stdout);
                // Multi-process split: when this gem5 node's simulation ends
                // (process exit), notify ubio with a best-effort TERMINATE so
                // the distributed clock treats this node as "done" (+inf) rather
                // than a frozen peer. Otherwise a node that finishes early (e.g.
                // an idle node) would cap min(safeTs) forever and freeze the
                // still-running nodes. See Port::safeTs PEER_LOST handling.
                framework::Port *portToClose = _port;
                int nodeForLog = _nodeId;
                registerExitCallback([portToClose, nodeForLog]() {
                    std::fprintf(stderr,
                        "[UBADAPTER-EXIT] node=%d sending TERMINATE to ubio\n",
                        nodeForLog);
                    portToClose->terminate();
                });

                // Multi-process split: register cross-node barrier callback
                // with the System's SyncWaitManager. When the workload calls
                // sync_wait with a mask spanning other nodes, the SyncWaitManager
                // calls our callback to send BARRIER_REACHED to ubio (which
                // forwards to the barrier_manager / other ubios). We receive
                // BARRIER_RELEASE in wakeup() and call releaseBarrier().
                // Barriers are per-NODE and flow through the socket-0 plane only.
                // Register the SyncWaitManager callback on socket 0's UBAdapter so
                // BARRIER_REACHED egresses via ubio(node,0); socket-1 UBAdapters do
                // not participate (would double-count / split the node's arrival).
                int localNode = _localNode;
                if (_socketId == 0 && localNode >= 0 && !System::systemList.empty()) {
                    System *sys = System::systemList[0];
                    sys->syncWait.setLocalNodeId(localNode);
                    sys->syncWait.setBarrierSendFn(
                        [this](uint32_t mask, uint32_t nodeId) {
                            sendBarrierReached(mask, nodeId);
                        });
                    std::fprintf(stderr,
                        "[UBADAPTER-BARRIER] node=%d socket=0 registered IPC barrier "
                        "callback (localNode=%d)\n", _nodeId, localNode);
                }
            }
        }
    }

}

void
UBAdapter::sendBarrierReached(uint32_t mask, uint32_t nodeId)
{
    if (!_port) return;
    framework::MemMessage *buf = _port->allocateSendBuffer(curTick());
    if (!buf) {
        std::fprintf(stderr,
            "[UBADAPTER-BARRIER] node=%d sendBarrierReached FAILED (no tx buf) "
            "mask=0x%x\n", _nodeId, mask);
        return;
    }
    // Barrier is carried as a PAYLOAD CoherenceMessage (BarrierReached); the
    // transport layer no longer has a dedicated BARRIER_REACHED type.
    buf->hdr.type = static_cast<uint32_t>(framework::MemMessageType::PAYLOAD);
    buf->hdr.req_id = mask;
    buf->hdr.sourceId = nodeId;
    buf->hdr.targetId = 0;
    CoherenceMessage bmsg;
    bmsg.h.type = CoherenceMessageType::BarrierReached;
    bmsg.h.srcNode = static_cast<uint16_t>(nodeId);
    bmsg.b.barrier.mask = mask;
    if (!buf->setPayload(bmsg)) {
        delete buf;
        std::fprintf(stderr,
            "[UBADAPTER-BARRIER] node=%d sendBarrierReached setPayload failed "
            "mask=0x%x\n", _nodeId, mask);
        return;
    }
    bool ok = _port->send(buf);
    std::fprintf(stderr,
        "[UBADAPTER-BARRIER-SEND] node=%d mask=0x%x ok=%d\n",
        _nodeId, mask, ok);
}

void
UBAdapter::startup()
{
    SimObject::startup();

    if (_port && !_eventArmed) {
        schedule(_responseCheckEvent, curTick());
        _eventArmed = true;
        std::fprintf(stderr,
                     "[UBADAPTER-STARTUP] node=%d socket=%d schedule sync wakeup @%lu\n",
                     _nodeId, _socketId, curTick());
    }
}

bool
UBAdapter::transportSend(const CoherenceMessage &msg)
{
    if (_port) {
        framework::MemMessage *buf = _port->allocateSendBuffer(curTick());
        if (!buf) {
            warn("UBAdapter node=%d socket=%d: transportSend no tx buffer (reqId=%lu type=%s)",
                 _nodeId, _socketId, msg.h.reqId, coherenceMsgTypeName(msg.h.type));
            return false;
        }

        buf->hdr.type = static_cast<uint32_t>(framework::MemMessageType::PAYLOAD);
        buf->hdr.req_id = msg.h.reqId;
        if (!buf->setPayload(msg)) {
            warn("UBAdapter node=%d socket=%d: transportSend payload encode failed (reqId=%lu)",
                 _nodeId, _socketId, msg.h.reqId);
            delete buf;
            return false;
        }

        if (!_port->send(buf)) {
            warn("UBAdapter node=%d socket=%d: transportSend port send failed (reqId=%lu)",
                 _nodeId, _socketId, msg.h.reqId);
            return false;
        }

        static int _tscount = 0;
        if (msg.h.type == CoherenceMessageType::ReadReq && ++_tscount <= 3)
            std::fprintf(stderr, "[GEM5-SEND] node=%d type=ReadReq reqId=%lu gem5_tick=%lu buf_ts=%lu\n",
                         _nodeId, msg.h.reqId, curTick(), buf->hdr.timestamp);
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
        framework::MemMessage *m = _port->recv(visible);
        if (!m) {
            continue;
        }

        const auto msgType = static_cast<framework::MemMessageType>(m->hdr.type);
        if (msgType == framework::MemMessageType::CONTROL_SYNC) {
            continue;
        }
        if (msgType != framework::MemMessageType::PAYLOAD) {
            warn("UBAdapter node=%d socket=%d: transportRecv unexpected MemMessage type=%u",
                 _nodeId, _socketId, m->hdr.type);
            continue;
        }

        const CoherenceMessage *coh = m->getPayload<CoherenceMessage>();
        if (!coh) {
            warn("UBAdapter node=%d socket=%d: transportRecv bad payload size=%u",
                 _nodeId, _socketId, m->payloadLen());
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
            const CoherenceMessage &resp = rit->second;
            if (outGrantVisibleTick) *outGrantVisibleTick = resp.b.readResp.grantVisibleTick;
            if (outSentinelVisibleTick) *outSentinelVisibleTick = resp.b.readResp.sentinelVisibleTick;
            if (outRecallNeeded) *outRecallNeeded = resp.b.readResp.recallNeeded;
            if (outRecallOwnerNode) *outRecallOwnerNode = resp.b.readResp.recallOwnerNode;
            if (outDataSource) *outDataSource = static_cast<GrantDataSource>(resp.b.readResp.dataSource);
            if (outAuthEpoch) *outAuthEpoch = resp.b.readResp.authEpoch;
            if (outPendingInvCount) *outPendingInvCount = resp.b.readResp.pendingInvCount;
            if (outPendingInvMask) *outPendingInvMask = resp.b.readResp.pendingInvMask;
            if (outCommittedEpoch) *outCommittedEpoch = resp.b.readResp.committedEpoch;
            if (outGrantData && outGrantDataValid) {
                // Grant data is valid for Modified grants (dirty fill) AND
                // whenever the home sourced the data from a recall buffer (the
                // previous owner's dirty line). A remote ReadShared whose home
                // just recalled the owner must still receive that recalled dirty
                // data — gating only on GlobalGrantModified dropped it, so the
                // requester read zeros. (See dataSource==RecallBuffer.)
                *outGrantDataValid =
                     (resp.b.readResp.grantType ==
                         static_cast<int>(OuterGrantType::GlobalGrantModified)) ||
                    (resp.b.readResp.dataSource ==
                         static_cast<int>(GrantDataSource::RecallBuffer));
                if (*outGrantDataValid) memcpy(outGrantData->getDataMod(0), resp.b.readResp.grantData, 64);
            }
            _inflightReadReqs.erase(reqId);
            _readyResponses.erase(rit);
            return static_cast<int>(resp.b.readResp.grantType);
        }
        if (_inflightReadReqs.count(reqId)) {
            return -2;
        }
    }

    _lastResponseValid = false;
    if (!transportSend(req)) {
        return -1;
    }

    // Port async path: schedule check, return pending
    if (_port) {
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
                             uint64_t epochVal, bool keepAsClean,
                             int homeNode, int homeSocket)
{
    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendWritebackReq called with no transport bound\n",
              _nodeId, _socketId);
    }

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
    req.h.reqId = 0;
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();
    if (keepAsClean)
        req.h.flags |= static_cast<uint32_t>(CFLAG_KEEP_AS_CLEAN);

    _lastResponseValid = false;
    if (!transportSend(req)) {
        return -1;
    }

    // Port async path: schedule check, return pending
    if (_port) {
        scheduleResponseCheck();
        return -2;
    }

    if (!transportRecv(CoherenceMessageType::WritebackResp, req.h.reqId)) {
        warn("UBAdapter node=%d: sendWritebackReq: no response PA=0x%lx\n",
             _nodeId, homePa);
        return -1;
    }

    const CoherenceMessage &resp = _lastResponse;
    if (resp.h.type != CoherenceMessageType::WritebackResp) {
        warn("UBAdapter node=%d: sendWritebackReq: unexpected response type %s\n",
             _nodeId, coherenceMsgTypeName(resp.h.type));
        return -1;
    }

    return resp.b.writebackResp.success ? 1 : 0;
}

// ---- Evict Request ----

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

    // Port async: check cached response first
    if (_port && _lastResponseValid &&
        _lastResponse.h.type == CoherenceMessageType::EvictResp &&
        _lastResponse.h.homeLinePa == homePa) {
        return _lastResponse.b.evictResp.success ? 1 : 0;
    }

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
                            bool checkOnly)
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
        auto rit = _readyResponses.find(rkey);
        if (rit != _readyResponses.end()) {
            const CoherenceMessage &resp = rit->second;
            if (outUpgradeTargetMask)
                *outUpgradeTargetMask = resp.b.upgradeResp.upgradeTargetMask;
            if (outCommittedEpoch)
                *outCommittedEpoch = resp.b.upgradeResp.committedEpoch;
            bool accepted =
                (resp.h.flags & static_cast<uint32_t>(CFLAG_ACCEPTED)) != 0;
            bool permanent =
                (resp.h.flags & static_cast<uint32_t>(CFLAG_BUSY)) != 0;
            _readyResponses.erase(rit);
            // 1=accepted, -3=permanent reject (not sharer → abandon),
            // 0=temporary reject (retry).
            return accepted ? 1 : (permanent ? -3 : 0);
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
    if (outUpgradeTargetMask)
        *outUpgradeTargetMask = resp.b.upgradeResp.upgradeTargetMask;
    if (outCommittedEpoch)
        *outCommittedEpoch = resp.b.upgradeResp.committedEpoch;

    DPRINTF(RubyEP,
            "UBAdapter node=%d: sendUpgradeReq result accepted=%d permanent=%d targetMask=0x%lx\n",
            _nodeId, accepted, permanent, resp.b.upgradeResp.upgradeTargetMask);

    // 1=accepted, -3=permanent reject (not sharer → abandon), 0=temporary (retry).
    return accepted ? 1 : (permanent ? -3 : 0);
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
    if (_port && _lastResponseValid &&
        _lastResponse.h.type == CoherenceMessageType::UpgradeDoneResp &&
        _lastResponse.h.reqId == reqId) {
        return _lastResponse.b.upgradeDoneResp.accepted ? 1 : 0;
    }

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
            std::fprintf(stderr, "[CLR-CACHE-HIT] node=%d reqId=%lu accepted=%d\n",
                         _nodeId, reqId, rit->second.b.clearResp.accepted ? 1 : 0);
            bool a = rit->second.b.clearResp.accepted;
            _readyResponses.erase(rit);
            return a ? 1 : 0;
        }
        std::fprintf(stderr, "[CLR-CACHE-MISS] node=%d reqId=%lu sending new ClearReq\n",
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

    std::fprintf(stderr, "[CLR-TX] n=%d reqId=%lu curT=%lu\n",
                 _nodeId, reqId, curTick());

    // Port async path: schedule check, return pending
    if (_port) {
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
                           bool dataReturned, uint64_t epoch,
                           uint64_t reqId,
                           const DataBlock *dataBlk,
                           int homeNode, int homeSocket)
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

    if (dataReturned)
        req.h.flags |= static_cast<uint32_t>(CFLAG_DATA_RETURNED);
    if (dataBlk && dataReturned) {
        req.h.flags |= static_cast<uint32_t>(CFLAG_HAS_DATA);
        memcpy(req.b.recallResp.data, dataBlk->getData(0, 64), 64);
    }

    // Fire-and-forget: no response expected
    return transportSend(req);
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
    return transportSend(req);
}

// ---- Cross-node Recall Request (EPBackend → EPBackend via router) ----

void
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
    req.h.requesterNode = _nodeId;
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

    // Fire-and-forget: no response expected from the remote adapter
    (void)transportSend(req);
}

// ---- Cross-node Invalidate Request (EPBackend → EPBackend via router) ----

void
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

    // Fire-and-forget
    (void)transportSend(req);
}

// ---- v4-dual-socket: QueryLineMetaReq (synchronous query) ----

int
UBAdapter::sendQueryLineMetaReq(uint64_t homePa, int homeNode, int homeSocket,
                                 uint64_t &outEpoch, int &outOwnerNode,
                                 bool &outFound)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sendQueryLineMetaReq homePa=0x%lx "
            "homeNode=%d homeSocket=%d\n",
            _nodeId, _socketId, homePa, homeNode, homeSocket);

    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendQueryLineMetaReq called with no transport bound\n",
              _nodeId, _socketId);
    }

    // Port async: check cached response first
    if (_port && _lastResponseValid &&
        _lastResponse.h.type == CoherenceMessageType::QueryLineMetaResp &&
        _lastResponse.h.homeLinePa == homePa) {
        outFound = _lastResponse.b.queryLineMetaResp.found;
        outEpoch = _lastResponse.b.queryLineMetaResp.epoch;
        outOwnerNode = _lastResponse.b.queryLineMetaResp.ownerNode;
        return outFound ? 0 : -1;
    }

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

void
UBAdapter::recvFromRouter(const CoherenceMessage &msg)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d: recvFromRouter type=%s src=%d dst=%d\n",
            _nodeId, coherenceMsgTypeName(msg.h.type),
            msg.h.srcNode, msg.h.dstNode);

    switch (msg.h.type) {
        case CoherenceMessageType::ReadResp:
            std::fprintf(stderr,
                         "[ADAPTER-GOT-RESP] node=%d type=ReadResp pa=0x%lx src=%d grant=%d epoch=%lu reqId=%lu\n",
                         _nodeId, msg.h.homeLinePa, msg.h.srcNode,
                         static_cast<int>(msg.b.readResp.grantType),
                         msg.h.epoch, msg.h.reqId);
            printf("[ADAPTER-GOT-RESP] node=%d type=ReadResp pa=0x%lx src=%d "
                   "grant=%d epoch=%lu reqId=%lu\n",
                   _nodeId, msg.h.homeLinePa, msg.h.srcNode,
                   static_cast<int>(msg.b.readResp.grantType),
                   msg.h.epoch, msg.h.reqId);
            _lastResponse = msg;
            _lastResponseValid = true;
            break;
        case CoherenceMessageType::WritebackResp:
        case CoherenceMessageType::EvictResp:
        case CoherenceMessageType::UpgradeResp:
        case CoherenceMessageType::UpgradeDoneResp:
        case CoherenceMessageType::ClearResp:
            std::fprintf(stderr,
                         "[CLEAR-RESP] node=%d via=recvFromRouter pa=0x%lx src=%d accepted=%d epoch=%lu reqId=%lu\n",
                         _nodeId, msg.h.homeLinePa, msg.h.srcNode,
                         msg.b.clearResp.accepted ? 1 : 0,
                         msg.h.epoch, msg.h.reqId);
            [[fallthrough]];
        case CoherenceMessageType::QueryLineMetaResp:
            // Synchronous response — store for caller
            _lastResponse = msg;
            _lastResponseValid = true;
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

        default:
            warn("UBAdapter node=%d: unhandled message type %s\n",
                 _nodeId, coherenceMsgTypeName(msg.h.type));
            break;
    }
}

// ---- Event-driven response processing (Step 1) ----

void
UBAdapter::wakeup()
{
    if (!_port) return;

    // 1. Emit sync (heartbeat) to let peer advance its boundary
    _port->emitSync(curTick());

    // 2. Drain all ready messages. CONTROL_SYNC now arrives as an ordinary
    //    kMessage and is skipped by hdr.type below (2.1.2 alignment).
    framework::ReceiveStatus st;
    framework::MemMessage *m = _port->recv(curTick(), &st);
    while (m && st == framework::ReceiveStatus::kMessage) {
        if (m->hdr.type == static_cast<uint32_t>(framework::MemMessageType::TERMINATE)) {
            m = _port->recv(curTick(), &st);
            continue;
        }
        if (m->hdr.type == static_cast<uint32_t>(framework::MemMessageType::CONTROL_SYNC)) {
            m = _port->recv(curTick(), &st);
            continue;
        }
        if (m->hdr.type != static_cast<uint32_t>(framework::MemMessageType::PAYLOAD)) {
            static int noncoh = 0;
            if (++noncoh <= 5)
                std::fprintf(stderr, "[WAKEUP-NONCOH] node=%d type=%u sz=%u\n",
                             _nodeId, m->hdr.type, m->hdr.size);
            m = _port->recv(curTick(), &st);
            continue;
        }
        // Barrier control now travels as a PAYLOAD CoherenceMessage. Peek its
        // coherence type: BarrierRelease releases the local sync_wait mask;
        // BarrierReached is handled by ubio/barrier_manager, not gem5, so skip.
        if (const CoherenceMessage *bc = m->getPayload<CoherenceMessage>()) {
            if (bc->h.type == CoherenceMessageType::BarrierRelease) {
                uint32_t mask = bc->b.barrier.mask;
                std::fprintf(stderr,
                    "[UBADAPTER-BARRIER-RELEASE] node=%d mask=0x%x\n", _nodeId, mask);
                if (!System::systemList.empty())
                    System::systemList[0]->syncWait.releaseBarrier(mask);
                m = _port->recv(curTick(), &st);
                continue;
            }
            if (bc->h.type == CoherenceMessageType::BarrierReached) {
                m = _port->recv(curTick(), &st);
                continue;
            }
        }
        // PortAsync: dispatch to handleResponse (pendingByReqId map)
        static int cohcnt = 0;
        if (++cohcnt <= 5)
            std::fprintf(stderr, "[WAKEUP-COH] node=%d type=%u sz=%u req_id=%lu\n",
                         _nodeId, m->hdr.type, m->hdr.size, m->hdr.req_id);
        if (_port) {
            handleResponse(m);
        } else {
            const CoherenceMessage *coh = m->getPayload<CoherenceMessage>();
            if (coh) recvFromRouter(*coh);
        }
        m = _port->recv(curTick(), &st);
    }

    // 3. Drain deferred async control messages before checking responses
    drainDeferredControls();

    // 4. Check for matched responses (for retry-based callers)
    checkResponseCallbacks();

    // 5. Schedule next wakeup using safeTs (conservative PDES bound).
    //    safeT = min(peer's latest timestamp, ownLastSync + syncInterval).
    //
    //    - safeT > curTick  : the peer is ahead; advance our clock to safeT.
    //    - safeT <= curTick : the peer has NOT advanced past us yet. We must
    //      NOT advance simulated time (that races gem5 ahead of the natives and
    //      breaks the protocol). Earlier we re-armed the event at the SAME tick
    //      and let it re-fire — but that hot-spins the gem5 event queue at
    //      millions of wakeups/sec, and each wakeup hammers ZMQ recv on the
    //      peer's socket. That saturated the IPC path and made the peer's own
    //      loop ~100x slower (the idle node's ubio peer crawled while the active
    //      nodes' peers flew), throttling the whole simulation to ~1 leapfrog
    //      step per 10 ms (the ZMQ send timeout).
    //
    //      Instead we busy-wait in WALL-CLOCK time, yielding the CPU between
    //      polls, exactly like the reference waitForUbsimAdvance() in
    //      docs/all.cpp. We never advance simulated time, so there is no drift;
    //      we stop hammering, so the peer advances quickly; and we still drain
    //      responses so the protocol keeps flowing. The syncInterval lookahead
    //      window guarantees the peer can always advance >= linkLatency, so this
    //      wait terminates promptly.
    ++_responseCheckCount;
    const uint64_t curT = curTick();
    uint64_t safeT = _port->safeTs(curT);
    bool stalled = !(safeT > curT);

    if (stalled) {
        uint64_t waitIters = 0;
        // Safety net only: in normal operation the peer lifts safeT within a
        // few microseconds. If something is genuinely wedged, fall back to a
        // same-tick re-arm so the event queue can run other nodes' events.
        const uint64_t kWaitCap = 2000000ULL;
        while (safeT <= curT && waitIters < kWaitCap) {
            std::this_thread::yield();
            // Drain whatever the peer has sent so receiveTimestamp() can rise
            // and any in-flight coherence responses keep flowing.
            framework::ReceiveStatus wst;
            framework::MemMessage *wm = _port->recv(curT, &wst);
            // CONTROL_SYNC arrives as kMessage and is ignored (no branch below).
            while (wm && wst == framework::ReceiveStatus::kMessage) {
                if (wm->hdr.type ==
                    static_cast<uint32_t>(framework::MemMessageType::TERMINATE)) {
                    wm = _port->recv(curT, &wst);
                    continue;
                }
                if (wm->hdr.type ==
                    static_cast<uint32_t>(framework::MemMessageType::PAYLOAD)) {
                    // Peek for barrier control (now a PAYLOAD CoherenceMessage).
                    const CoherenceMessage *bc = wm->getPayload<CoherenceMessage>();
                    if (bc && bc->h.type == CoherenceMessageType::BarrierRelease) {
                        if (!System::systemList.empty())
                            System::systemList[0]->syncWait.releaseBarrier(
                                bc->b.barrier.mask);
                    } else {
                        handleResponse(wm);
                    }
                }
                wm = _port->recv(curT, &wst);
            }
            safeT = _port->safeTs(curT);
            ++waitIters;
        }
        drainDeferredControls();
        checkResponseCallbacks();
        stalled = !(safeT > curT);
    }

    uint64_t nextT = stalled ? curT : safeT;
    // If stalled but there is a pending message (timestamp > curT), advance
    // to the pending timestamp so the message can be delivered next wakeup.
    // This is critical for BARRIER_RELEASE from ubio: without it the release
    // stays in Port recv's _pending queue forever and the barrier never fires.
    if (stalled) {
        uint64_t pendingT = _port->receiveTimestamp();
        if (pendingT > curT && pendingT < nextT &&
            pendingT != ~static_cast<uint64_t>(0))
            nextT = pendingT;
    }
    if (_responseCheckEvent.scheduled())
        reschedule(_responseCheckEvent, nextT);
    else
        schedule(_responseCheckEvent, nextT);
    _eventArmed = true;

    if (_responseCheckCount % 2000 == 0) {
        uint64_t rxt = _port->receiveTimestamp();
        std::fprintf(stderr,
                     "[CLK-SYNC] node=%d curT=%lu rxt=%lu safeT=%lu %s cnt=%lu\n",
                     _nodeId, curT, rxt, safeT,
                     stalled ? "WAIT" : "advance",
                     (unsigned long)_responseCheckCount);
    }
}

void
UBAdapter::checkResponseCallbacks()
{
    if (!_port || _readyResponses.empty()) return;

    bool delivered = false;
    for (auto rit = _readyResponses.begin(); rit != _readyResponses.end(); ) {
        const PendingKey &rkey = rit->first;
        auto pit = _pendingByReqId.find(rkey);
        if (pit != _pendingByReqId.end() && pit->second.onResp) {
            pit->second.onResp(rit->second);
            _pendingByReqId.erase(pit);
            rit = _readyResponses.erase(rit);
            delivered = true;
        } else {
            ++rit;
        }
    }
    if (delivered && _onResponseWired) {
        std::fprintf(stderr, "[RSP-WIRED] node=%d firing wakeup\n", _nodeId);
    }
}

void
UBAdapter::handleResponse(framework::MemMessage *m)
{
    if (!_port) return;

    const CoherenceMessage *coh = m->getPayload<CoherenceMessage>();
    std::fprintf(stderr, "[HR-ENTRY] node=%d msg_type=%u msg_sz=%u payload=%s coh_type=%d reqId=%lu\n",
                 _nodeId, m->hdr.type, m->hdr.size,
                 coh ? "OK" : "NULL",
                 coh ? static_cast<int>(coh->h.type) : -1,
                 coh ? coh->h.reqId : 0UL);
    if (!coh) return;

    if (coh->h.type == CoherenceMessageType::ClearResp) {
        std::fprintf(stderr,
                     "[CLEAR-RESP] node=%d via=handleResponse pa=0x%lx src=%d accepted=%d epoch=%lu reqId=%lu\n",
                     _nodeId, coh->h.homeLinePa, coh->h.srcNode,
                     coh->b.clearResp.accepted ? 1 : 0,
                     coh->h.epoch, coh->h.reqId);
    }

    // Async control messages: enqueue FIFO, process later via drainDeferredControls
    switch (coh->h.type) {
      case CoherenceMessageType::InvalidateReq:
      case CoherenceMessageType::RecallReq:
      case CoherenceMessageType::UpgradeAckNotify:
        _deferredControls.push_back(*coh);
        return;
      default:
        break;
    }

    // Dispatch via _pendingByReqId — now keyed by (respType, reqId)
    PendingKey key{coh->h.type, coh->h.reqId};
    auto it = _pendingByReqId.find(key);

    // Store in ready-response cache for retry-based sendReadReq
    _readyResponses[key] = *coh;
    if (coh->h.type == CoherenceMessageType::ReadResp) {
        static int rc = 0;
        if (++rc <= 3)
            warn("UBAdapter node=%d: stored ReadResp reqId=%lu grant=%d\n",
                 _nodeId, coh->h.reqId,
                 static_cast<int>(coh->b.readResp.grantType));
    }

    // If there's a direct callback, also invoke it
    if (it != _pendingByReqId.end() && it->second.onResp) {
        _inflightReadReqs.erase(coh->h.reqId);
        it->second.onResp(*coh);
        _pendingByReqId.erase(it);
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
    if (!_eventArmed && _port) {
        schedule(_responseCheckEvent, curTick() + 10);
        _eventArmed = true;
    }
}

uint64_t
UBAdapter::allocLocalReqId()
{
    uint64_t id = _nextLocalReqId++;
    if (id == 0) id = _nextLocalReqId++;
    return id;
}

void
UBAdapter::clearReadyResponsesForLine(uint64_t linePa)
{
    for (auto it = _readyResponses.begin(); it != _readyResponses.end(); ) {
        if (it->second.h.homeLinePa == linePa ||
            it->second.h.localLinePa == linePa) {
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
    while (!_deferredControls.empty()) {
        CoherenceMessage msg = _deferredControls.front();
        _deferredControls.pop_front();
        recvFromRouter(msg);
    }
    _drainingDeferredControls = false;
}

} // namespace ruby
} // namespace gem5
