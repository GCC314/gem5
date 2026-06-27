#include "mem/ruby/protocol/chi/ep/UBAdapter.hh"
#include "mem/ruby/protocol/chi/ep/UBCCController.hh"

#include <cstdio>
#include <limits>

#include "base/logging.hh"
#include "debug/RubyEP.hh"
#include "framework/MemMessage.hh"
#include "framework/Port.hh"
#include "mem/ruby/protocol/chi/ep/EPBackend.hh"
#include "sim/cur_tick.hh"

namespace gem5
{
namespace ruby
{

UBAdapter::UBAdapter(const Params &p)
    : SimObject(p),
      _nodeId(p.node_id),
      _socketId(p.socket_id),

      _addrMap(3, 1, 128ULL * 1024 * 1024),
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

    // Create Port directly (guarantees all nodes get one regardless of init order)
    const char* portEnv = getenv("UBIO_PORT_ENABLE");
    if (portEnv) {
        int enableNode = atoi(portEnv);
        if (enableNode < 0 || _nodeId == enableNode) {
            auto* ctx = new zmq::context_t(1);
            std::string ep = "ipc:///tmp/ubio_n" + std::to_string(_nodeId);
            _port = new framework::Port(
                "gem5_ubio", _nodeId, 0, ep, true, *ctx, 1000);
            std::printf("[STEP5] Port enabled node=%d ep=%s\n", _nodeId, ep.c_str());
        }
    }

}

bool
UBAdapter::transportSend(const CoherenceMessage &msg)
{
    if (_port) {
        framework::MemMessage *buf = _port->sendAllocateBuffer(curTick());
        if (!buf) {
            warn("UBAdapter node=%d socket=%d: transportSend no tx buffer (reqId=%lu type=%s)",
                 _nodeId, _socketId, msg.h.reqId, coherenceMsgTypeName(msg.h.type));
            return false;
        }

        buf->hdr.type = static_cast<uint32_t>(framework::MemMessageType::COH_MSG);
        buf->hdr.req_id = msg.h.reqId;
        if (!buf->setPayload(msg)) {
            warn("UBAdapter node=%d socket=%d: transportSend payload encode failed (reqId=%lu)",
                 _nodeId, _socketId, msg.h.reqId);
            return false;
        }

        if (!_port->send(buf)) {
            warn("UBAdapter node=%d socket=%d: transportSend port send failed (reqId=%lu)",
                 _nodeId, _socketId, msg.h.reqId);
            return false;
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
        if (msgType != framework::MemMessageType::COH_MSG) {
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

    // Port async path: if response already cached, return it directly
    if (_port && _lastResponseValid && _lastResponse.h.type == CoherenceMessageType::ReadResp
        && _lastResponse.h.reqId == reqId) {
        // Fill output pointers from cached response
        if (outGrantVisibleTick) *outGrantVisibleTick = _lastResponse.b.readResp.grantVisibleTick;
        if (outSentinelVisibleTick) *outSentinelVisibleTick = _lastResponse.b.readResp.sentinelVisibleTick;
        if (outRecallNeeded) *outRecallNeeded = _lastResponse.b.readResp.recallNeeded;
        if (outRecallOwnerNode) *outRecallOwnerNode = _lastResponse.b.readResp.recallOwnerNode;
        if (outDataSource) *outDataSource = static_cast<GrantDataSource>(_lastResponse.b.readResp.dataSource);
        if (outAuthEpoch) *outAuthEpoch = _lastResponse.b.readResp.authEpoch;
        if (outPendingInvCount) *outPendingInvCount = _lastResponse.b.readResp.pendingInvCount;
        if (outPendingInvMask) *outPendingInvMask = _lastResponse.b.readResp.pendingInvMask;
        if (outCommittedEpoch) *outCommittedEpoch = _lastResponse.b.readResp.committedEpoch;
        if (outGrantData && outGrantDataValid) {
            *outGrantDataValid = (_lastResponse.b.readResp.grantType
                                  == static_cast<int>(UBCC_OuterGrantType::GlobalGrantModified));
            if (*outGrantDataValid)
                memcpy(outGrantData->getDataMod(0),
                       _lastResponse.b.readResp.grantData, 64);
        }
        return static_cast<int>(_lastResponse.b.readResp.grantType);
    }

    _lastResponseValid = false;
    if (!transportSend(req)) {
        return -1;
    }

    if (_port) {
        scheduleResponseCheck();
        return -2;
    }

    if (!transportRecv(CoherenceMessageType::ReadResp, reqId)) {
        printf("[ADAPTER-NO-RESP] node=%d pa=0x%lx epoch=%lu reqId=%lu home=%d\n",
               _nodeId, homePa, epoch, reqId, homeNode);
        warn("UBAdapter node=%d: sendReadReq: no response received "
             "PA=0x%lx\n", _nodeId, homePa);
        return -1;
    }

    const CoherenceMessage &resp = _lastResponse;
    if (resp.h.type != CoherenceMessageType::ReadResp) {
        warn("UBAdapter node=%d: sendReadReq: unexpected response type %s "
             "PA=0x%lx\n", _nodeId, coherenceMsgTypeName(resp.h.type), homePa);
        return -1;
    }

    int grant = static_cast<int>(resp.b.readResp.grantType);
    printf("[ADAPTER-GRANT-OK] node=%d pa=0x%lx grant=%d\n",
           _nodeId, homePa, grant);

    if (outGrantVisibleTick)
        *outGrantVisibleTick = resp.b.readResp.grantVisibleTick;
    if (outSentinelVisibleTick)
        *outSentinelVisibleTick = resp.b.readResp.sentinelVisibleTick;
    if (outRecallNeeded)
        *outRecallNeeded = resp.b.readResp.recallNeeded;
    if (outRecallOwnerNode)
        *outRecallOwnerNode = resp.b.readResp.recallOwnerNode;
    if (outDataSource)
        *outDataSource = static_cast<GrantDataSource>(resp.b.readResp.dataSource);
    if (outAuthEpoch)
        *outAuthEpoch = resp.b.readResp.authEpoch;
    if (outPendingInvCount)
        *outPendingInvCount = resp.b.readResp.pendingInvCount;
    if (outPendingInvMask)
        *outPendingInvMask = resp.b.readResp.pendingInvMask;
    if (outCommittedEpoch)
        *outCommittedEpoch = resp.b.readResp.committedEpoch;
    if (outGrantDataValid) {
        *outGrantDataValid =
            (resp.h.flags & static_cast<uint32_t>(CFLAG_HAS_DATA)) != 0;
    }
    if (outGrantData &&
        (resp.h.flags & static_cast<uint32_t>(CFLAG_HAS_DATA)) != 0) {
        outGrantData->setData(resp.b.readResp.grantData, 0, 64);
    }

    DPRINTF(RubyEP,
            "UBAdapter node=%d: sendReadReq result grant=%d "
            "recallNeeded=%d recallOwner=%d dataSource=%d authEpoch=%lu\n",
            _nodeId, grant,
            resp.b.readResp.recallNeeded, resp.b.readResp.recallOwnerNode,
            resp.b.readResp.dataSource, resp.b.readResp.authEpoch);

    return grant;
}

// ---- Phase 2+: Writeback Request ----

bool
UBAdapter::sendWritebackReq(uint64_t homePa, int requesterNode,
                             uint64_t epochVal, bool keepAsClean,
                             int homeNode, int homeSocket)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sendWritebackReq homePa=0x%lx "
            "reqNode=%d epoch=%lu keepAsClean=%d homeNode=%d homeSocket=%d\n",
            _nodeId, _socketId, homePa, requesterNode, epochVal, keepAsClean,
            homeNode, homeSocket);

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
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();
    if (keepAsClean)
        req.h.flags |= static_cast<uint32_t>(CFLAG_KEEP_AS_CLEAN);

    _lastResponseValid = false;
    if (!transportSend(req)) {
        return false;
    }

    if (!transportRecv(CoherenceMessageType::WritebackResp, req.h.reqId)) {
        warn("UBAdapter node=%d: sendWritebackReq: no response PA=0x%lx\n",
             _nodeId, homePa);
        return false;
    }

    const CoherenceMessage &resp = _lastResponse;
    if (resp.h.type != CoherenceMessageType::WritebackResp) {
        warn("UBAdapter node=%d: sendWritebackReq: unexpected response type %s\n",
             _nodeId, coherenceMsgTypeName(resp.h.type));
        return false;
    }

    return resp.b.writebackResp.success;
}

// ---- Evict Request ----

bool
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
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();

    _lastResponseValid = false;
    if (!transportSend(req)) {
        return false;
    }

    if (!transportRecv(CoherenceMessageType::EvictResp, req.h.reqId)) {
        warn("UBAdapter node=%d: sendEvictReq: no response PA=0x%lx\n",
             _nodeId, homePa);
        return false;
    }

    const CoherenceMessage &resp = _lastResponse;
    if (resp.h.type != CoherenceMessageType::EvictResp) {
        warn("UBAdapter node=%d: sendEvictReq: unexpected response type %s\n",
             _nodeId, coherenceMsgTypeName(resp.h.type));
        return false;
    }

    return resp.b.evictResp.success;
}

// ---- Upgrade Request ----

bool
UBAdapter::sendUpgradeReq(uint64_t homePa, int requesterNode,
                            uint64_t epoch, uint64_t reqId,
                            int desiredPerm, int cause,
                            uint64_t *outUpgradeTargetMask,
                            uint64_t *outCommittedEpoch,
                            int homeNode, int homeSocket)
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
        return false;
    }

    if (!transportRecv(CoherenceMessageType::UpgradeResp, reqId)) {
        warn("UBAdapter node=%d: sendUpgradeReq: no response PA=0x%lx\n",
             _nodeId, homePa);
        return false;
    }

    const CoherenceMessage &resp = _lastResponse;
    if (resp.h.type != CoherenceMessageType::UpgradeResp) {
        warn("UBAdapter node=%d: sendUpgradeReq: unexpected response type %s\n",
             _nodeId, coherenceMsgTypeName(resp.h.type));
        return false;
    }

    bool accepted = (resp.h.flags & static_cast<uint32_t>(CFLAG_ACCEPTED)) != 0;
    if (outUpgradeTargetMask)
        *outUpgradeTargetMask = resp.b.upgradeResp.upgradeTargetMask;
    if (outCommittedEpoch)
        *outCommittedEpoch = resp.b.upgradeResp.committedEpoch;

    DPRINTF(RubyEP,
            "UBAdapter node=%d: sendUpgradeReq result accepted=%d targetMask=0x%lx\n",
            _nodeId, accepted, resp.b.upgradeResp.upgradeTargetMask);

    return accepted;
}

// ---- Upgrade Done Request ----

bool
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
        return false;
    }

    if (!transportRecv(CoherenceMessageType::UpgradeDoneResp, reqId)) {
        warn("UBAdapter node=%d: sendUpgradeDoneReq: no response PA=0x%lx\n",
             _nodeId, homePa);
        return false;
    }

    const CoherenceMessage &resp = _lastResponse;
    if (resp.h.type != CoherenceMessageType::UpgradeDoneResp) {
        warn("UBAdapter node=%d: sendUpgradeDoneReq: unexpected response type %s\n",
             _nodeId, coherenceMsgTypeName(resp.h.type));
        return false;
    }

    return resp.b.upgradeDoneResp.accepted;
}

// ---- Clear Request ----

bool
UBAdapter::sendClearReq(uint64_t linePa, int srcNode,
                         uint64_t epoch, uint64_t reqId,
                         int homeNode, int homeSocket)
{
    DPRINTF(RubyEP,
            "UBAdapter node=%d socket=%d: sendClearReq PA=0x%lx "
            "srcNode=%d epoch=%lu reqId=%lu homeNode=%d homeSocket=%d\n",
            _nodeId, _socketId, linePa, srcNode, epoch, reqId,
            homeNode, homeSocket);

    if (!_port) {
        fatal("UBAdapter node=%d socket=%d: sendClearReq called with no transport bound\n",
              _nodeId, _socketId);
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

    req.b.clearReq.reason = 0; // GrantHandshake

    _lastResponseValid = false;
    if (!transportSend(req)) {
        return false;
    }

    if (!transportRecv(CoherenceMessageType::ClearResp, reqId)) {
        warn("UBAdapter node=%d: sendClearReq: no response PA=0x%lx\n",
             _nodeId, linePa);
        return false;
    }

    const CoherenceMessage &resp = _lastResponse;
    if (resp.h.type != CoherenceMessageType::ClearResp) {
        warn("UBAdapter node=%d: sendClearReq: unexpected response type %s\n",
             _nodeId, coherenceMsgTypeName(resp.h.type));
        return false;
    }

    return resp.b.clearResp.accepted;
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
    printf("[RECALL-TRACE-B] UBAdapter n=%d sendRecallReqToOwner PA=0x%lx target=%d epoch=%lu\n",
           _nodeId, recallMsg.linePa, targetNode, recallMsg.epoch);
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
    req.h.seqNum = _nextSeq++;
    req.h.enqueueTick = curTick();
    req.h.readyTick = curTick();

    _lastResponseValid = false;
    if (!transportSend(req)) {
        return -1;
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

    // 2. Drain all ready messages (using three-state recv)
    framework::ReceiveStatus st;
    framework::MemMessage *m = _port->recv(curTick(), &st);
    while (m && (st == framework::ReceiveStatus::kMessage || st == framework::ReceiveStatus::kSync)) {
        if (m->hdr.type == static_cast<uint32_t>(framework::MemMessageType::CONTROL_SYNC)) {
            m = _port->recv(curTick(), &st);
            continue;
        }
        if (m->hdr.type != static_cast<uint32_t>(framework::MemMessageType::COH_MSG)) {
            m = _port->recv(curTick(), &st);
            continue;
        }
        // PortAsync: dispatch to handleResponse (pendingByReqId map)
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

    // 5. Schedule next wakeup using safeTs for conservative advancement
    if (++_responseCheckCount < 100000) {
        uint64_t safeT = _port->safeTs(curTick());
        if (safeT <= curTick()) safeT = curTick() + 1000;
        schedule(_responseCheckEvent, safeT);
        _eventArmed = true;
    } else {
        static int wstop = 0;
        if (++wstop <= 1)
            warn("UBAdapter node=%d: wakeup STOPPED after 100000 checks\n", _nodeId);
    }
}

void
UBAdapter::checkResponseCallbacks()
{
    if (!_port) return;
    // Match _lastResponse against _pendingByReqId entries
    if (!_lastResponseValid) return;
    auto it = _pendingByReqId.find(_lastResponse.h.reqId);
    if (it == _pendingByReqId.end()) return;
    if (it->second.onResp) {
        it->second.onResp(_lastResponse);
    }
    _pendingByReqId.erase(it);
    _lastResponseValid = false;
}

void
UBAdapter::handleResponse(framework::MemMessage *m)
{
    if (!_port) return;

    const CoherenceMessage *coh = m->getPayload<CoherenceMessage>();
    if (!coh) return;

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

    // Dispatch via _pendingByReqId map
    auto it = _pendingByReqId.find(m->hdr.req_id);
    if (it != _pendingByReqId.end() && it->second.onResp) {
        it->second.onResp(*coh);
        _pendingByReqId.erase(it);
        return;
    }

    // Fallback: store as lastResponse for retry-based callers
    _lastResponse = *coh;
    _lastResponseValid = true;
}

void
UBAdapter::scheduleResponseCheck()
{
    if (!_eventArmed && _port) {
        schedule(_responseCheckEvent, curTick() + 10);
        _eventArmed = true;
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
