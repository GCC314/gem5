#include "mem/ruby/protocol/chi/ep/UBRouter.hh"

#include <cstring>
#include <cstdio>

#include "base/logging.hh"
#include "debug/RubyEP.hh"
#include "debug/UBLatency.hh"
#include "mem/ruby/protocol/chi/ep/UBAdapter.hh"
#include "mem/ruby/protocol/chi/ep/UBCCController.hh"
#include "sim/cur_tick.hh"

namespace gem5
{
namespace ruby
{

// ---- Static registry ----
std::map<UBRouter::RouterKey, UBRouter*> UBRouter::_routers;

UBRouter* UBRouter::getRouter(int nodeId, int socketId)
{
    auto it = _routers.find({nodeId, socketId});
    return (it != _routers.end()) ? it->second : nullptr;
}

void UBRouter::registerRouter(int nodeId, int socketId, UBRouter *router)
{
    _routers[{nodeId, socketId}] = router;
}

// ---- Constructor / Destructor ----

UBRouter::UBRouter(const Params &p)
    : SimObject(p),
      _nodeId(p.node_id),
      _socketId(p.socket_id),
      _defaultLatency(p.ub_msg_latency),
      _drainEvent([this]{ drainReadyQueues(); }, name() + ".drainEvent")
{
    registerRouter(_nodeId, _socketId, this);
    DPRINTF(RubyEP, "UBRouter node=%d socket=%d created, defaultLatency=%lu\n",
            _nodeId, _socketId, _defaultLatency);
}

UBRouter::~UBRouter()
{
    _routers.erase({_nodeId, _socketId});
    for (auto &kv : _pairQueues) {
        delete kv.second;
    }
    _pairQueues.clear();
}

void
UBRouter::init()
{
    SimObject::init();
}

// ---- Queue management ----

// Pack (srcNode,srcSocket,dstNode,dstSocket) into a QueueKey.
// key.first  = (srcNode<<16) | srcSocket
// key.second = (dstNode<<16) | dstSocket
static inline UBRouter::QueueKey
makeQueueKey(int srcNode, int srcSocket, int dstNode, int dstSocket)
{
    return std::make_pair(
        (srcNode << 16) | (srcSocket & 0xffff),
        (dstNode << 16) | (dstSocket & 0xffff));
}

UBMsgQueue*
UBRouter::getOrCreateQueue(int srcNode, int srcSocket,
                             int dstNode, int dstSocket)
{
    auto key = makeQueueKey(srcNode, srcSocket, dstNode, dstSocket);
    auto it = _pairQueues.find(key);
    if (it == _pairQueues.end()) {
        UBMsgQueue *q = new UBMsgQueue();
        q->setLatency(0);
        _pairQueues[key] = q;
        return q;
    }
    return it->second;
}

// ---- Main send path ----

void
UBRouter::sendMessage(const UBMsg &msg, Tick forcedLatency)
{
    DPRINTF(RubyEP,
            "UBRouter node=%d socket=%d: sendMessage %s src=(%d,%d) dst=(%d,%d)\n",
            _nodeId, _socketId, ubMsgTypeName(msg.h.type),
            msg.h.srcNode, msg.h.srcSocket, msg.h.dstNode, msg.h.dstSocket);

    // forcedLatency >=0 means caller specifies latency; -1 means use queue default
    UBMsgQueue *q = getOrCreateQueue(
        msg.h.srcNode, msg.h.srcSocket, msg.h.dstNode, msg.h.dstSocket);
    Tick lat;
    if (forcedLatency >= 0) {
        lat = forcedLatency;
    } else {
        // Cross-node latency applies when srcNode != dstNode
        lat = (msg.h.srcNode != msg.h.dstNode) ? _defaultLatency : 0;
    }
    q->enqueue(msg, curTick(), lat);
    DPRINTF(UBLatency,
            "[UBLAT] tick=%lu src=%d,%d dst=%d,%d type=%s pa=0x%lx epoch=%lu reqId=%lu action=ENQUEUE\n",
            curTick(), msg.h.srcNode, msg.h.srcSocket, msg.h.dstNode, msg.h.dstSocket,
            ubMsgTypeName(msg.h.type), msg.h.homeLinePa, msg.h.epoch, msg.h.reqId);

    drainReadyQueues();
}

// ---- Drain logic ----

void
UBRouter::drainReadyQueues()
{
    Tick now = curTick();
    bool progress = true;

    constexpr int maxDrainPerWakeup = 128;
    int drained = 0;

    while (progress && drained < maxDrainPerWakeup) {
        progress = false;

        for (auto &kv : _pairQueues) {
            UBMsgQueue *q = kv.second;
            while (q->hasReady(now) && drained < maxDrainPerWakeup) {
                UBMsg msg = q->popReady(now);
                drained++;
                progress = true;

                DPRINTF(RubyEP,
                        "UBRouter node=%d: draining %s src=%d dst=%d\n",
                        _nodeId, ubMsgTypeName(msg.h.type),
                        msg.h.srcNode, msg.h.dstNode);
                DPRINTF(UBLatency,
                        "[UBLAT] tick=%lu src=%d,%d dst=%d,%d type=%s pa=0x%lx epoch=%lu reqId=%lu action=DEQUEUE\n",
                        now, msg.h.srcNode, msg.h.srcSocket, msg.h.dstNode, msg.h.dstSocket,
                        ubMsgTypeName(msg.h.type), msg.h.homeLinePa, msg.h.epoch, msg.h.reqId);

                if (msg.h.dstNode == _nodeId && msg.h.dstSocket == _socketId) {
                    // Local delivery — route to UBCC or Adapter
                    DPRINTF(UBLatency,
                            "[UBLAT] tick=%lu src=%d,%d dst=%d,%d type=%s pa=0x%lx epoch=%lu reqId=%lu action=DELIVER\n",
                            now, msg.h.srcNode, msg.h.srcSocket, msg.h.dstNode, msg.h.dstSocket,
                            ubMsgTypeName(msg.h.type), msg.h.homeLinePa, msg.h.epoch, msg.h.reqId);
                    switch (msg.h.type) {
                        case UBMsgType::ReadReq:
                        case UBMsgType::WritebackReq:
                        case UBMsgType::EvictReq:
                        case UBMsgType::UpgradeReq:
                        case UBMsgType::UpgradeDoneReq:
                        case UBMsgType::ClearReq:
                        case UBMsgType::RecallResp:
                        case UBMsgType::InvalidateAck:
                        case UBMsgType::QueryLineMetaReq:
                        case UBMsgType::HomeWritebackNotify:
                            // Destination is local UBCC
                            {
                                UBMsg response;
                                deliverToUbcc(msg, response);
                                // Send response back through reverse queue
                                // (only for request types that expect a response)
                                if (msg.h.type == UBMsgType::RecallResp ||
                                    msg.h.type == UBMsgType::InvalidateAck) {
                                    // Fire-and-forget: no response needed
                                } else if (response.h.type != UBMsgType::ReadReq) {
                                    // Response enqueue: reverse direction, same sockets
                                    UBMsgQueue *revQ = getOrCreateQueue(
                                        _nodeId, _socketId,
                                        msg.h.srcNode, msg.h.srcSocket);
                                    revQ->enqueue(response, now, 0);
                                    DPRINTF(UBLatency,
                                            "[UBLAT] tick=%lu src=%d,%d dst=%d,%d type=%s pa=0x%lx epoch=%lu reqId=%lu action=ENQUEUE\n",
                                            now, _nodeId, _socketId, msg.h.srcNode, msg.h.srcSocket,
                                            ubMsgTypeName(response.h.type), response.h.homeLinePa,
                                            response.h.epoch, response.h.reqId);
                                }
                            }
                            break;

                        case UBMsgType::RecallReq:
                        case UBMsgType::InvalidateReq:
                        case UBMsgType::ReadResp:
                        case UBMsgType::WritebackResp:
                        case UBMsgType::EvictResp:
                        case UBMsgType::UpgradeResp:
                        case UBMsgType::UpgradeDoneResp:
                        case UBMsgType::ClearResp:
                        case UBMsgType::UpgradeAckNotify:
                        case UBMsgType::QueryLineMetaResp:
                            // Destination is local UBAdapter
                         printf("[ROUTER-DELIVER-RESP] node=%d socket=%d pa=0x%lx type=%s src=(%d,%d) dst=(%d,%d)\n",
                                _nodeId, _socketId, msg.h.homeLinePa, ubMsgTypeName(msg.h.type),
                                msg.h.srcNode, msg.h.srcSocket, msg.h.dstNode, msg.h.dstSocket);
                            deliverToAdapter(msg);
                            break;

                        default:
                            warn("UBRouter node=%d: unhandled message type "
                                 "for local delivery: %s\n",
                                 _nodeId, ubMsgTypeName(msg.h.type));
                            break;
                    }
                } else {
                    // Remote delivery — find destination router by (node,socket)
                    DPRINTF(RubyEP,
                            "UBRouter node=%d socket=%d: remote delivery to (node=%d,socket=%d)\n",
                            _nodeId, _socketId, msg.h.dstNode, msg.h.dstSocket);
                    UBRouter *dstRouter = getRouter(msg.h.dstNode, msg.h.dstSocket);
                    if (dstRouter) {
                        dstRouter->sendMessage(msg, 0);
                    } else {
                        warn("UBRouter node=%d socket=%d: no router for dst (node=%d,socket=%d)\n",
                             _nodeId, _socketId, msg.h.dstNode, msg.h.dstSocket);
                    }
                }
            }
        }
    }

    // v4-latency: reschedule drain if any queue has pending (not-yet-ready) messages
    bool hasPending = false;
    for (auto &kv : _pairQueues) {
        if (kv.second->size() > 0) { hasPending = true; break; }
    }
    if (drained >= maxDrainPerWakeup || hasPending) {
        DPRINTF(RubyEP,
                "UBRouter node=%d: max drain reached (%d) or pending, "
                "scheduling next drain\n",
                _nodeId, maxDrainPerWakeup);
        schedule(_drainEvent, curTick() + 1);
    }
}

// ---- Delivery to local UBCC ----

void
UBRouter::deliverToUbcc(const UBMsg &msg, UBMsg &response)
{
    if (!_localUbcc) {
        warn("UBRouter node=%d: deliverToUbcc called but no local UBCC\n",
             _nodeId);
        return;
    }

    DPRINTF(RubyEP,
            "UBRouter node=%d socket=%d: deliverToUbcc type=%s\n",
            _nodeId, _socketId, ubMsgTypeName(msg.h.type));

    switch (msg.h.type) {
        case UBMsgType::ReadReq: {
            UBCC_OuterReqType ubccReq =
                ((msg.h.flags & static_cast<uint32_t>(UB_FLAG_WRITE_INTENT)) || msg.b.readReq.neededPerm == 1)
                    ? UBCC_OuterReqType::GlobalReadUnique
                    : UBCC_OuterReqType::GlobalReadShared;

            Tick grantVisibleTick = 0;
            Tick sentinelVisibleTick = 0;
            bool recallNeeded = false;
            int recallOwnerNode = -1;
            GrantDataSource dataSource = GrantDataSource::HomeMemory;
            uint64_t authEpoch = 0;

            UBCC_OuterGrantType ubccGrant =
                _localUbcc->processOuterRequest(
                    msg.h.homeLinePa, ubccReq,
                    (msg.h.flags & static_cast<uint32_t>(UB_FLAG_WRITE_INTENT)) != 0,
                    msg.h.requesterNode,
                    msg.h.epoch, msg.h.reqId,
                    &grantVisibleTick, &sentinelVisibleTick,
                    &recallNeeded, &recallOwnerNode,
                    &dataSource, &authEpoch);

            int pendingInvCount =
                _localUbcc->getPendingInvalidationCount(msg.h.homeLinePa);
            uint64_t pendingInvMask =
                _localUbcc->getPendingInvalidationMask(msg.h.homeLinePa);
            uint64_t committedEpoch =
                _localUbcc->getEpochForLine(msg.h.homeLinePa);
            DataBlock grantData(64);
            bool hasGrantData = false;
            if (dataSource == GrantDataSource::RecallBuffer) {
                hasGrantData =
                    _localUbcc->copyOutstandingGrantData(msg.h.homeLinePa,
                                                         grantData);
            }

            response.h.type = UBMsgType::ReadResp;
            response.h.srcNode = _nodeId;
            response.h.srcSocket = _socketId;
            response.h.dstNode = msg.h.srcNode;
            response.h.dstSocket = msg.h.srcSocket;
            response.h.homeNode = _nodeId;
            response.h.homeSocket = _socketId;
            response.h.ingressSocket = msg.h.ingressSocket;
            response.h.requesterNode = msg.h.requesterNode;
            response.h.homeLinePa = msg.h.homeLinePa;
            response.h.epoch = msg.h.epoch;
            response.h.reqId = msg.h.reqId;
            response.h.flags = 0;
            if (hasGrantData) {
                response.h.flags |= static_cast<uint32_t>(UB_FLAG_HAS_DATA);
            }

            printf("[ROUTER-UBCC-RESP] home=%d socket=%d pa=0x%lx grant=%d src=(%d,%d)\n",
                   _nodeId, _socketId, msg.h.homeLinePa, static_cast<int>(ubccGrant),
                   msg.h.srcNode, msg.h.srcSocket);
            response.b.readResp.grantType =
                static_cast<int8_t>(ubccGrant);
            response.b.readResp.dataSource =
                static_cast<int8_t>(dataSource);
            response.b.readResp.pendingInvCount = pendingInvCount;
            response.b.readResp.grantVisibleTick = grantVisibleTick;
            response.b.readResp.sentinelVisibleTick = sentinelVisibleTick;
            response.b.readResp.recallNeeded = recallNeeded;
            response.b.readResp.recallOwnerNode = recallOwnerNode;
            response.b.readResp.authEpoch = authEpoch;
            response.b.readResp.committedEpoch = committedEpoch;
            response.b.readResp.pendingInvMask = pendingInvMask;
            if (hasGrantData) {
                memcpy(response.b.readResp.grantData,
                       grantData.getData(0, 64), 64);
            }
            break;
        }

        case UBMsgType::WritebackReq: {
            bool keepAsClean =
                (msg.h.flags & static_cast<uint32_t>(UB_FLAG_KEEP_AS_CLEAN)) != 0;
            bool success = _localUbcc->processWriteback(
                msg.h.homeLinePa, msg.h.requesterNode,
                msg.h.epoch, keepAsClean);

            response.h.type = UBMsgType::WritebackResp;
            response.h.srcNode = _nodeId;
            response.h.srcSocket = _socketId;
            response.h.dstNode = msg.h.srcNode;
            response.h.dstSocket = msg.h.srcSocket;
            response.h.homeLinePa = msg.h.homeLinePa;
            response.h.epoch = msg.h.epoch;
            response.h.reqId = msg.h.reqId;
            response.b.writebackResp.success = success;
            break;
        }

        case UBMsgType::EvictReq: {
            bool success = _localUbcc->processEvict(
                msg.h.homeLinePa, msg.h.requesterNode,
                msg.h.epoch);

            response.h.type = UBMsgType::EvictResp;
            response.h.srcNode = _nodeId;
            response.h.srcSocket = _socketId;
            response.h.dstNode = msg.h.srcNode;
            response.h.dstSocket = msg.h.srcSocket;
            response.h.homeLinePa = msg.h.homeLinePa;
            response.h.epoch = msg.h.epoch;
            response.h.reqId = msg.h.reqId;
            response.b.evictResp.success = success;
            break;
        }

        case UBMsgType::UpgradeReq: {
            UBCC_UpgradeCause ubccCause =
                (msg.b.upgradeReq.cause == 0)
                    ? UBCC_UpgradeCause::LocalCleanUnique
                    : UBCC_UpgradeCause::LocalStoreUpgrade;

            bool accepted = _localUbcc->processOuterUpgradeReq(
                msg.h.homeLinePa, msg.h.requesterNode,
                msg.h.epoch, msg.h.reqId,
                msg.b.upgradeReq.desiredPerm, ubccCause);

            uint64_t targetMask = _localUbcc->getUpgradePendingTargetMask(
                msg.h.homeLinePa);

            response.h.type = UBMsgType::UpgradeResp;
            response.h.srcNode = _nodeId;
            response.h.srcSocket = _socketId;
            response.h.dstNode = msg.h.srcNode;
            response.h.dstSocket = msg.h.srcSocket;
            response.h.homeLinePa = msg.h.homeLinePa;
            response.h.epoch = msg.h.epoch;
            response.h.reqId = msg.h.reqId;
            response.h.flags = accepted
                ? static_cast<uint32_t>(UB_FLAG_ACCEPTED) : 0;
            response.b.upgradeResp.upgradeTargetMask = targetMask;
            response.b.upgradeResp.committedEpoch =
                _localUbcc->getEpochForLine(msg.h.homeLinePa);
            break;
        }

        case UBMsgType::UpgradeDoneReq: {
            bool accepted = _localUbcc->processOuterUpgradeDone(
                msg.h.homeLinePa, msg.h.requesterNode,
                msg.h.epoch, msg.h.reqId);

            response.h.type = UBMsgType::UpgradeDoneResp;
            response.h.srcNode = _nodeId;
            response.h.srcSocket = _socketId;
            response.h.dstNode = msg.h.srcNode;
            response.h.dstSocket = msg.h.srcSocket;
            response.h.homeLinePa = msg.h.homeLinePa;
            response.h.epoch = msg.h.epoch;
            response.h.reqId = msg.h.reqId;
            response.b.upgradeDoneResp.accepted = accepted;
            break;
        }

        case UBMsgType::ClearReq: {
            bool accepted = _localUbcc->processClear(
                msg.h.homeLinePa, msg.h.requesterNode,
                msg.h.epoch, msg.h.reqId);

            response.h.type = UBMsgType::ClearResp;
            response.h.srcNode = _nodeId;
            response.h.srcSocket = _socketId;
            response.h.dstNode = msg.h.srcNode;
            response.h.dstSocket = msg.h.srcSocket;
            response.h.homeLinePa = msg.h.homeLinePa;
            response.h.epoch = msg.h.epoch;
            response.h.reqId = msg.h.reqId;
            response.b.clearResp.accepted = accepted;
            break;
        }

        case UBMsgType::RecallResp: {
            bool dataReturned =
                (msg.h.flags & static_cast<uint32_t>(UB_FLAG_DATA_RETURNED)) != 0;
            bool hasData =
                (msg.h.flags & static_cast<uint32_t>(UB_FLAG_HAS_DATA)) != 0;

            DataBlock dataBlk(64);
            const DataBlock *dataPtr = nullptr;
            if (hasData) {
                dataBlk.setData(msg.b.recallResp.data, 0, 64);
                dataPtr = &dataBlk;
            }

            _localUbcc->processRecallResponse(
                msg.h.homeLinePa, msg.h.requesterNode,
                dataReturned, msg.h.epoch, msg.h.reqId,
                dataPtr);
            // Fire-and-forget: no response message
            break;
        }

        case UBMsgType::InvalidateAck: {
            _localUbcc->processInvalidationAck(
                msg.h.homeLinePa, msg.h.requesterNode,
                msg.h.epoch, msg.h.reqId);
            // Fire-and-forget: no response message
            break;
        }

        case UBMsgType::QueryLineMetaReq: {
            // v4-dual-socket: EPBackend queries UBCC for {epoch, ownerNode}
            uint64_t qEpoch = 0;
            int qOwnerNode = -1;
            MESIState qState = MESIState::G_I;
            bool qFound = false;
            _localUbcc->queryLineMeta(msg.h.homeLinePa, qEpoch, qOwnerNode,
                                       qState, qFound);

            response.h.type = UBMsgType::QueryLineMetaResp;
            response.h.srcNode = _nodeId;
            response.h.srcSocket = _socketId;
            response.h.dstNode = msg.h.srcNode;
            response.h.dstSocket = msg.h.srcSocket;
            response.h.homeLinePa = msg.h.homeLinePa;
            response.h.epoch = msg.h.epoch;
            response.h.reqId = msg.h.reqId;
            response.b.queryLineMetaResp.found = qFound;
            response.b.queryLineMetaResp.epoch = qEpoch;
            response.b.queryLineMetaResp.ownerNode = qOwnerNode;
            break;
        }

        case UBMsgType::HomeWritebackNotify: {
            // v4-dual-socket: HN-F completed DDR4 writeback, notify UBCC
            _localUbcc->processHomeWritebackNotify(
                msg.h.homeLinePa, msg.h.epoch);
            // Fire-and-forget: no response
            break;
        }

        default:
            warn("UBRouter node=%d socket=%d: deliverToUbcc unhandled type %s\n",
                 _nodeId, _socketId, ubMsgTypeName(msg.h.type));
            break;
    }
}

// ---- Delivery to local adapter ----

void
UBRouter::deliverToAdapter(const UBMsg &msg)
{
    if (!_localAdapter) {
        warn("UBRouter node=%d socket=%d: deliverToAdapter called but no local adapter\n",
             _nodeId, _socketId);
        return;
    }

    DPRINTF(RubyEP,
            "UBRouter node=%d socket=%d: deliverToAdapter type=%s\n",
            _nodeId, _socketId, ubMsgTypeName(msg.h.type));

    _localAdapter->recvFromRouter(msg);
}

} // namespace ruby
} // namespace gem5
