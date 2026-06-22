#include "mem/ruby/protocol/chi/ep/UBIOModule.hh"

#include <cstring>
#include <cstdio>

#include "base/logging.hh"
#include "debug/RubyEP.hh"
#include "debug/UBLatency.hh"
#include "mem/ruby/protocol/chi/ep/UBAdapter.hh"
#include "mem/ruby/protocol/chi/ep/UBCCController.hh"
#include "sim/cur_tick.hh"

#include <cstdio>

namespace gem5
{
namespace ruby
{

// ---- Static registry ----
std::map<UBIOModule::RouterKey, UBIOModule *> UBIOModule::_routers;

UBIOModule * UBIOModule::getRouter(int nodeId, int socketId)
{
    auto it = _routers.find({nodeId, socketId});
    return (it != _routers.end()) ? it->second : nullptr;
}

void UBIOModule::registerRouter(int nodeId, int socketId, UBIOModule *router)
{
    _routers[{nodeId, socketId}] = router;
}

// ---- Constructor / Destructor ----

UBIOModule::UBIOModule(const Params &p)
    : SimObject(p),
      _nodeId(p.node_id),
      _socketId(p.socket_id),
      _defaultLatency(p.ub_msg_latency),
      _drainEvent([this]{ drainReadyQueues(); }, name() + ".drainEvent")
{
    registerRouter(_nodeId, _socketId, this);
    DPRINTF(RubyEP, "UBIOModule node=%d socket=%d created, defaultLatency=%lu\n",
            _nodeId, _socketId, _defaultLatency);

    // Parse fault rules from SimObject params (debug-only)
    parseFaultRules(p.fault_rules);
}

UBIOModule::~UBIOModule()
{
    _routers.erase({_nodeId, _socketId});
    for (auto &kv : _pairQueues) {
        delete kv.second;
    }
    _pairQueues.clear();
}

void
UBIOModule::init()
{
    SimObject::init();
}

// ---- Queue management ----

// Pack (srcNode,srcSocket,dstNode,dstSocket) into a QueueKey.
// key.first  = (srcNode<<16) | srcSocket
// key.second = (dstNode<<16) | dstSocket
static inline UBIOModule::QueueKey
makeQueueKey(int srcNode, int srcSocket, int dstNode, int dstSocket)
{
    return std::make_pair(
        (srcNode << 16) | (srcSocket & 0xffff),
        (dstNode << 16) | (dstSocket & 0xffff));
}

CoherenceMessageQueue*
UBIOModule::getOrCreateQueue(int srcNode, int srcSocket,
                             int dstNode, int dstSocket)
{
    auto key = makeQueueKey(srcNode, srcSocket, dstNode, dstSocket);
    auto it = _pairQueues.find(key);
    if (it == _pairQueues.end()) {
        CoherenceMessageQueue *q = new CoherenceMessageQueue();
        q->setLatency(0);
        _pairQueues[key] = q;
        return q;
    }
    return it->second;
}

// ---- Main send path ----

void
UBIOModule::sendMessage(const CoherenceMessage &msg, Tick forcedLatency)
{
    DPRINTF(RubyEP,
            "UBIOModule node=%d socket=%d: sendMessage %s src=(%d,%d) dst=(%d,%d)\n",
            _nodeId, _socketId, coherenceMsgTypeName(msg.h.type),
            msg.h.srcNode, msg.h.srcSocket, msg.h.dstNode, msg.h.dstSocket);

#ifndef NDEBUG
    // ── Debug Fault Injection (compile-time guarded) ──
    int faultCopies = applyFaultRules(msg);
    if (faultCopies == 0) {
        // Dropped by fault rule — do not enqueue
        return;
    }
#endif

    // forcedLatency >=0 means caller specifies latency; -1 means use queue default
    CoherenceMessageQueue *q = getOrCreateQueue(
        msg.h.srcNode, msg.h.srcSocket, msg.h.dstNode, msg.h.dstSocket);
    Tick lat;
    if (forcedLatency >= 0) {
        lat = forcedLatency;
    } else {
        // Cross-node latency applies when srcNode != dstNode
        lat = (msg.h.srcNode != msg.h.dstNode) ? _defaultLatency : 0;
    }

#ifndef NDEBUG
    if (faultCopies >= 1) {
        q->enqueue(msg, curTick(), lat);
    }
    if (faultCopies >= 2) {
        // Duplicate: enqueue a second copy
        q->enqueue(msg, curTick(), lat);
    }
#else
    q->enqueue(msg, curTick(), lat);
#endif

    DPRINTF(UBLatency,
            "[UBLAT] tick=%lu src=%d,%d dst=%d,%d type=%s pa=0x%lx epoch=%lu reqId=%lu action=ENQUEUE\n",
            curTick(), msg.h.srcNode, msg.h.srcSocket, msg.h.dstNode, msg.h.dstSocket,
            coherenceMsgTypeName(msg.h.type), msg.h.homeLinePa, msg.h.epoch, msg.h.reqId);

    drainReadyQueues();
}

// ---- Drain logic ----

void
UBIOModule::drainReadyQueues()
{
    Tick now = curTick();
    bool progress = true;

    constexpr int maxDrainPerWakeup = 128;
    int drained = 0;

    while (progress && drained < maxDrainPerWakeup) {
        progress = false;

        for (auto &kv : _pairQueues) {
            CoherenceMessageQueue *q = kv.second;
            while (q->hasReady(now) && drained < maxDrainPerWakeup) {
                CoherenceMessage msg = q->popReady(now);
                drained++;
                progress = true;

                DPRINTF(RubyEP,
                        "UBIOModule node=%d: draining %s src=%d dst=%d\n",
                        _nodeId, coherenceMsgTypeName(msg.h.type),
                        msg.h.srcNode, msg.h.dstNode);
                DPRINTF(UBLatency,
                        "[UBLAT] tick=%lu src=%d,%d dst=%d,%d type=%s pa=0x%lx epoch=%lu reqId=%lu action=DEQUEUE\n",
                        now, msg.h.srcNode, msg.h.srcSocket, msg.h.dstNode, msg.h.dstSocket,
                        coherenceMsgTypeName(msg.h.type), msg.h.homeLinePa, msg.h.epoch, msg.h.reqId);

                if (msg.h.dstNode == _nodeId && msg.h.dstSocket == _socketId) {
                    // Local delivery — route to UBCC or Adapter
                    DPRINTF(UBLatency,
                            "[UBLAT] tick=%lu src=%d,%d dst=%d,%d type=%s pa=0x%lx epoch=%lu reqId=%lu action=DELIVER\n",
                            now, msg.h.srcNode, msg.h.srcSocket, msg.h.dstNode, msg.h.dstSocket,
                            coherenceMsgTypeName(msg.h.type), msg.h.homeLinePa, msg.h.epoch, msg.h.reqId);
                    switch (msg.h.type) {
                        case CoherenceMessageType::ReadReq:
                        case CoherenceMessageType::WritebackReq:
                        case CoherenceMessageType::EvictReq:
                        case CoherenceMessageType::UpgradeReq:
                        case CoherenceMessageType::UpgradeDoneReq:
                        case CoherenceMessageType::ClearReq:
                        case CoherenceMessageType::RecallResp:
                        case CoherenceMessageType::InvalidateAck:
                        case CoherenceMessageType::QueryLineMetaReq:
                        case CoherenceMessageType::HomeWritebackNotify:
                            // Destination is local UBCC
                            {
                                CoherenceMessage response;
                                deliverToUbcc(msg, response);
                                // Send response back through reverse queue
                                // (only for request types that expect a response)
                                if (msg.h.type == CoherenceMessageType::RecallResp ||
                                    msg.h.type == CoherenceMessageType::InvalidateAck) {
                                    // Fire-and-forget: no response needed
                                } else if (response.h.type != CoherenceMessageType::ReadReq) {
                                    // Response enqueue: reverse direction, same sockets
                                    CoherenceMessageQueue *revQ = getOrCreateQueue(
                                        _nodeId, _socketId,
                                        msg.h.srcNode, msg.h.srcSocket);
                                    revQ->enqueue(response, now, 0);
                                    DPRINTF(UBLatency,
                                            "[UBLAT] tick=%lu src=%d,%d dst=%d,%d type=%s pa=0x%lx epoch=%lu reqId=%lu action=ENQUEUE\n",
                                            now, _nodeId, _socketId, msg.h.srcNode, msg.h.srcSocket,
                                            coherenceMsgTypeName(response.h.type), response.h.homeLinePa,
                                            response.h.epoch, response.h.reqId);
                                }
                            }
                            break;

                        case CoherenceMessageType::RecallReq:
                        case CoherenceMessageType::InvalidateReq:
                        case CoherenceMessageType::ReadResp:
                        case CoherenceMessageType::WritebackResp:
                        case CoherenceMessageType::EvictResp:
                        case CoherenceMessageType::UpgradeResp:
                        case CoherenceMessageType::UpgradeDoneResp:
                        case CoherenceMessageType::ClearResp:
                        case CoherenceMessageType::UpgradeAckNotify:
                        case CoherenceMessageType::QueryLineMetaResp:
                            // Destination is local UBAdapter
                         printf("[ROUTER-DELIVER-RESP] node=%d socket=%d pa=0x%lx type=%s src=(%d,%d) dst=(%d,%d)\n",
                                _nodeId, _socketId, msg.h.homeLinePa, coherenceMsgTypeName(msg.h.type),
                                msg.h.srcNode, msg.h.srcSocket, msg.h.dstNode, msg.h.dstSocket);
                            deliverToAdapter(msg);
                            break;

                        default:
                            warn("UBIOModule node=%d: unhandled message type "
                                 "for local delivery: %s\n",
                                 _nodeId, coherenceMsgTypeName(msg.h.type));
                            break;
                    }
                } else {
                    // Remote delivery — find destination router by (node,socket)
                    DPRINTF(RubyEP,
                            "UBIOModule node=%d socket=%d: remote delivery to (node=%d,socket=%d)\n",
                            _nodeId, _socketId, msg.h.dstNode, msg.h.dstSocket);
                    UBIOModule *dstRouter = getRouter(msg.h.dstNode, msg.h.dstSocket);
                    if (dstRouter) {
                        dstRouter->sendMessage(msg, 0);
                    } else {
                        warn("UBIOModule node=%d socket=%d: no router for dst (node=%d,socket=%d)\n",
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
                "UBIOModule node=%d: max drain reached (%d) or pending, "
                "scheduling next drain\n",
                _nodeId, maxDrainPerWakeup);
        schedule(_drainEvent, curTick() + 1);
    }
}

// ---- Delivery to local UBCC ----

void
UBIOModule::deliverToUbcc(const CoherenceMessage &msg, CoherenceMessage &response)
{
    if (!_localUbcc) {
        warn("UBIOModule node=%d: deliverToUbcc called but no local UBCC\n",
             _nodeId);
        return;
    }

    DPRINTF(RubyEP,
            "UBIOModule node=%d socket=%d: deliverToUbcc type=%s\n",
            _nodeId, _socketId, coherenceMsgTypeName(msg.h.type));

    switch (msg.h.type) {
        case CoherenceMessageType::ReadReq: {
            UBCC_OuterReqType ubccReq =
                ((msg.h.flags & static_cast<uint32_t>(CFLAG_WRITE_INTENT)) || msg.b.readReq.neededPerm == 1)
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
                    (msg.h.flags & static_cast<uint32_t>(CFLAG_WRITE_INTENT)) != 0,
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

            response.h.type = CoherenceMessageType::ReadResp;
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
                response.h.flags |= static_cast<uint32_t>(CFLAG_HAS_DATA);
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

        case CoherenceMessageType::WritebackReq: {
            bool keepAsClean =
                (msg.h.flags & static_cast<uint32_t>(CFLAG_KEEP_AS_CLEAN)) != 0;
            bool success = _localUbcc->processWriteback(
                msg.h.homeLinePa, msg.h.requesterNode,
                msg.h.epoch, keepAsClean);

            response.h.type = CoherenceMessageType::WritebackResp;
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

        case CoherenceMessageType::EvictReq: {
            bool success = _localUbcc->processEvict(
                msg.h.homeLinePa, msg.h.requesterNode,
                msg.h.epoch);

            response.h.type = CoherenceMessageType::EvictResp;
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

        case CoherenceMessageType::UpgradeReq: {
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

            response.h.type = CoherenceMessageType::UpgradeResp;
            response.h.srcNode = _nodeId;
            response.h.srcSocket = _socketId;
            response.h.dstNode = msg.h.srcNode;
            response.h.dstSocket = msg.h.srcSocket;
            response.h.homeLinePa = msg.h.homeLinePa;
            response.h.epoch = msg.h.epoch;
            response.h.reqId = msg.h.reqId;
            response.h.flags = accepted
                ? static_cast<uint32_t>(CFLAG_ACCEPTED) : 0;
            response.b.upgradeResp.upgradeTargetMask = targetMask;
            response.b.upgradeResp.committedEpoch =
                _localUbcc->getEpochForLine(msg.h.homeLinePa);
            break;
        }

        case CoherenceMessageType::UpgradeDoneReq: {
            bool accepted = _localUbcc->processOuterUpgradeDone(
                msg.h.homeLinePa, msg.h.requesterNode,
                msg.h.epoch, msg.h.reqId);

            response.h.type = CoherenceMessageType::UpgradeDoneResp;
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

        case CoherenceMessageType::ClearReq: {
            bool accepted = _localUbcc->processClear(
                msg.h.homeLinePa, msg.h.requesterNode,
                msg.h.epoch, msg.h.reqId);

            response.h.type = CoherenceMessageType::ClearResp;
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

        case CoherenceMessageType::RecallResp: {
            bool dataReturned =
                (msg.h.flags & static_cast<uint32_t>(CFLAG_DATA_RETURNED)) != 0;
            bool hasData =
                (msg.h.flags & static_cast<uint32_t>(CFLAG_HAS_DATA)) != 0;

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

        case CoherenceMessageType::InvalidateAck: {
            _localUbcc->processInvalidationAck(
                msg.h.homeLinePa, msg.h.requesterNode,
                msg.h.epoch, msg.h.reqId);
            // Fire-and-forget: no response message
            break;
        }

        case CoherenceMessageType::QueryLineMetaReq: {
            // v4-dual-socket: EPBackend queries UBCC for {epoch, ownerNode}
            uint64_t qEpoch = 0;
            int qOwnerNode = -1;
            MESIState qState = MESIState::G_I;
            bool qFound = false;
            _localUbcc->queryLineMeta(msg.h.homeLinePa, qEpoch, qOwnerNode,
                                       qState, qFound);

            response.h.type = CoherenceMessageType::QueryLineMetaResp;
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

        case CoherenceMessageType::HomeWritebackNotify: {
            // v4-dual-socket: HN-F completed DDR4 writeback, notify UBCC
            _localUbcc->processHomeWritebackNotify(
                msg.h.homeLinePa, msg.h.epoch);
            // Fire-and-forget: no response
            break;
        }

        default:
            warn("UBIOModule node=%d socket=%d: deliverToUbcc unhandled type %s\n",
                 _nodeId, _socketId, coherenceMsgTypeName(msg.h.type));
            break;
    }
}

// ---- Delivery to local adapter ----

void
UBIOModule::deliverToAdapter(const CoherenceMessage &msg)
{
    if (!_localAdapter) {
        warn("UBIOModule node=%d socket=%d: deliverToAdapter called but no local adapter\n",
             _nodeId, _socketId);
        return;
    }

    DPRINTF(RubyEP,
            "UBIOModule node=%d socket=%d: deliverToAdapter type=%s\n",
            _nodeId, _socketId, coherenceMsgTypeName(msg.h.type));

    _localAdapter->recvFromRouter(msg);
}

// ── Debug Fault Injection ──

// Helper: parse a CoherenceMessageType name string
static CoherenceMessageType parseMsgTypeName(const std::string &s)
{
    if (s == "*" || s == "any")  return CoherenceMessageType::ReadReq; // wildcard
    if (s == "ReadReq")          return CoherenceMessageType::ReadReq;
    if (s == "ReadResp")         return CoherenceMessageType::ReadResp;
    if (s == "WritebackReq")     return CoherenceMessageType::WritebackReq;
    if (s == "WritebackResp")    return CoherenceMessageType::WritebackResp;
    if (s == "EvictReq")         return CoherenceMessageType::EvictReq;
    if (s == "EvictResp")        return CoherenceMessageType::EvictResp;
    if (s == "RecallReq")        return CoherenceMessageType::RecallReq;
    if (s == "RecallResp")       return CoherenceMessageType::RecallResp;
    if (s == "InvalidateReq")    return CoherenceMessageType::InvalidateReq;
    if (s == "InvalidateAck")    return CoherenceMessageType::InvalidateAck;
    if (s == "UpgradeReq")       return CoherenceMessageType::UpgradeReq;
    if (s == "UpgradeResp")      return CoherenceMessageType::UpgradeResp;
    if (s == "UpgradeDoneReq")   return CoherenceMessageType::UpgradeDoneReq;
    if (s == "UpgradeDoneResp")  return CoherenceMessageType::UpgradeDoneResp;
    if (s == "ClearReq")         return CoherenceMessageType::ClearReq;
    if (s == "ClearResp")        return CoherenceMessageType::ClearResp;
    if (s == "UpgradeAckNotify") return CoherenceMessageType::UpgradeAckNotify;
    if (s == "QueryLineMetaReq") return CoherenceMessageType::QueryLineMetaReq;
    if (s == "QueryLineMetaResp") return CoherenceMessageType::QueryLineMetaResp;
    if (s == "HomeWritebackNotify") return CoherenceMessageType::HomeWritebackNotify;
    return CoherenceMessageType::ReadReq; // default wildcard
}

void
UBIOModule::parseFaultRules(const std::vector<std::string> &rules)
{
    for (const auto &rule_str : rules) {
        // Format: "name:type:src:dst:pa:action[:delayTicks[:matchCount]]"
        DebugFaultRule rule;
        // Simple colon-delimited parsing
        std::vector<std::string> parts;
        size_t pos = 0, next = 0;
        while ((next = rule_str.find(':', pos)) != std::string::npos) {
            parts.push_back(rule_str.substr(pos, next - pos));
            pos = next + 1;
        }
        parts.push_back(rule_str.substr(pos));

        if (parts.size() < 6) {
            warn("UBIOModule *: malformed fault rule '%s' — skipping\n",
                 rule_str.c_str());
            continue;
        }

        rule.name       = parts[0];
        rule.matchType  = parseMsgTypeName(parts[1]);
        rule.matchSrcNode = std::stoi(parts[2]);
        rule.matchDstNode = std::stoi(parts[3]);
        rule.matchLinePa  = std::stoull(parts[4], nullptr, 0);

        const std::string &action_str = parts[5];
        if (action_str == "drop" || action_str == "Drop") {
            rule.action = DebugFaultAction::Drop;
        } else if (action_str == "delay" || action_str == "Delay") {
            rule.action = DebugFaultAction::Delay;
            rule.delayTicks = (parts.size() > 6) ? std::stoull(parts[6]) : 1000;
        } else if (action_str == "dup" || action_str == "Duplicate") {
            rule.action = DebugFaultAction::Duplicate;
        } else {
            warn("UBIOModule *: unknown fault action '%s' — skipping\n",
                 action_str.c_str());
            continue;
        }

        if (rule.action == DebugFaultAction::Delay && parts.size() > 6) {
            rule.delayTicks = std::stoull(parts[6]);
        }
        if (parts.size() > 7) {
            rule.matchCount = std::stoi(parts[7]);
        }

        addFaultRule(rule);
    }
}

void
UBIOModule::addFaultRule(const DebugFaultRule &rule)
{
    _faultRules.push_back(rule);
    DPRINTF(RubyEP,
            "UBIOModule node=%d socket=%d: added fault rule '%s' "
            "type=%s action=%d\n",
            _nodeId, _socketId, rule.name.c_str(),
            coherenceMsgTypeName(rule.matchType), static_cast<int>(rule.action));
}

void
UBIOModule::clearFaultRules()
{
    _faultRules.clear();
}

int
UBIOModule::applyFaultRules(const CoherenceMessage &msg)
{
    // Returns: 0 = drop, 1 = normal, 2 = duplicate
    int copies = 1;
    for (auto &rule : _faultRules) {
        // Check match count limit
        if (rule.matchCount > 0 && rule.firedCount >= rule.matchCount) {
            continue;
        }
        // Check message type match (wildcard: matchType == ReadReq means "any")
        if (rule.matchType != CoherenceMessageType::ReadReq &&
            rule.matchType != msg.h.type) {
            continue;
        }
        // Check source node match
        if (rule.matchSrcNode >= 0 && rule.matchSrcNode != msg.h.srcNode) {
            continue;
        }
        // Check dest node match
        if (rule.matchDstNode >= 0 && rule.matchDstNode != msg.h.dstNode) {
            continue;
        }
        // Check PA match
        if (rule.matchLinePa != 0 && rule.matchLinePa != msg.h.homeLinePa) {
            continue;
        }
        // Rule matches!
        rule.firedCount++;
        switch (rule.action) {
            case DebugFaultAction::Drop:
                printf("[UBFAULT] node=%d rule='%s' action=Drop "
                       "type=%s src=%d dst=%d pa=0x%lx\n",
                       _nodeId, rule.name.c_str(),
                       coherenceMsgTypeName(msg.h.type),
                       msg.h.srcNode, msg.h.dstNode, msg.h.homeLinePa);
                copies = 0;
                break;
            case DebugFaultAction::Delay:
                printf("[UBFAULT] node=%d rule='%s' action=Delay ticks=%lu "
                       "type=%s src=%d dst=%d pa=0x%lx\n",
                       _nodeId, rule.name.c_str(), rule.delayTicks,
                       coherenceMsgTypeName(msg.h.type),
                       msg.h.srcNode, msg.h.dstNode, msg.h.homeLinePa);
                // Delay is handled by scheduling a deferred enqueue
                // For now, pass through normally (delay not implemented)
                // TODO: implement deferred enqueue via event
                copies = 1;
                break;
            case DebugFaultAction::Duplicate:
                printf("[UBFAULT] node=%d rule='%s' action=Duplicate "
                       "type=%s src=%d dst=%d pa=0x%lx\n",
                       _nodeId, rule.name.c_str(),
                       coherenceMsgTypeName(msg.h.type),
                       msg.h.srcNode, msg.h.dstNode, msg.h.homeLinePa);
                copies = 2;
                break;
        }
    }
    return copies;
}

void
UBIOModule::delayedEnqueue(CoherenceMessage msg, CoherenceMessageQueue *q, Tick lat)
{
    q->enqueue(msg, curTick(), lat);
    drainReadyQueues();
}

} // namespace ruby
} // namespace gem5
