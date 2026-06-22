#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBMSG_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBMSG_HH__

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

#include "base/types.hh"

namespace gem5
{
namespace ruby
{

// ---- Message Type Enumeration ----
enum class CoherenceMessageType : uint16_t {
    ReadReq,
    ReadResp,
    RecallReq,
    RecallResp,
    InvalidateReq,
    InvalidateAck,
    WritebackReq,
    WritebackResp,
    EvictReq,
    EvictResp,
    UpgradeReq,
    UpgradeResp,
    UpgradeDoneReq,
    UpgradeDoneResp,
    ClearReq,
    ClearResp,
    UpgradeAckNotify,
    QueryLineMetaReq,        // v4-dual-socket: EPBackend queries UBCC for epoch/owner
    QueryLineMetaResp,       // v4-dual-socket: UBCC response
    HomeWritebackNotify,     // v4-dual-socket: HN-F completes DDR4 writeback
};

// ---- Message Flags ----
enum CoherenceMessageFlags : uint32_t {
    CFLAG_WRITE_INTENT   = 1u << 0,
    CFLAG_KEEP_AS_CLEAN  = 1u << 1,
    CFLAG_ACCEPTED       = 1u << 2,
    CFLAG_DATA_RETURNED  = 1u << 3,
    CFLAG_HAS_DATA       = 1u << 4,
    CFLAG_IS_READ_RECALL = 1u << 5,
    CFLAG_BUSY            = 1u << 6,
};

// ---- Message Header (fixed envelope) ----
struct CoherenceMessageHeader {
    CoherenceMessageType type;
    uint16_t srcNode;
    uint16_t srcSocket;       // v4-dual-socket: source socket
    uint16_t dstNode;
    uint16_t dstSocket;       // v4-dual-socket: destination socket (homeSocket for requests)
    uint16_t homeNode;
    uint16_t homeSocket;      // v4-dual-socket: home directory socket (from PA)
    uint16_t ingressSocket;   // v4-dual-socket: request entry socket (NUMA hint)
    uint16_t requesterNode;
    uint16_t targetNode;
    uint32_t flags;
    uint64_t homeLinePa;
    uint64_t localLinePa;
    uint64_t epoch;
    uint64_t reqId;
    uint64_t seqNum;
    Tick enqueueTick;
    Tick readyTick;

    CoherenceMessageHeader()
        : type(CoherenceMessageType::ReadReq),
          srcNode(0), srcSocket(0), dstNode(0), dstSocket(0),
          homeNode(0), homeSocket(0), ingressSocket(0),
          requesterNode(0), targetNode(0),
          flags(0),
          homeLinePa(0), localLinePa(0),
          epoch(0), reqId(0), seqNum(0),
          enqueueTick(0), readyTick(0) {}
};

// ---- Message Bodies (tagged union) ----
struct UBReadReqBody {
    uint8_t neededPerm;   // 0=Shared, 1=Unique

    UBReadReqBody() : neededPerm(0) {}
};

struct UBReadRespBody {
    int8_t grantType;           // -1 = BUSY, 0 = Shared, 1 = Exclusive, 2 = Modified
    int8_t dataSource;          // 0=HomeMemory, 1=RecallBuffer, 2=NoData
    int16_t pendingInvCount;    // -1 if no INVALIDATE outstanding
    Tick grantVisibleTick;
    Tick sentinelVisibleTick;
    bool recallNeeded;
    int recallOwnerNode;        // -1 if none
    uint64_t authEpoch;
    uint64_t committedEpoch;    // current committed home epoch
    uint64_t pendingInvMask;    // sharers still awaiting invalidation
    uint8_t grantData[64];      // optional recall-buffer payload for grant

    UBReadRespBody()
        : grantType(-1), dataSource(0), pendingInvCount(-1),
          grantVisibleTick(0), sentinelVisibleTick(0),
          recallNeeded(false), recallOwnerNode(-1), authEpoch(0),
          committedEpoch(0), pendingInvMask(0)
    {
        memset(grantData, 0, sizeof(grantData));
    }
};

struct UBRecallReqBody { /* no extra fields beyond header */ };

struct UBRecallRespBody {
    uint8_t data[64];  // F2: actual 64-byte cache line data
    UBRecallRespBody() { memset(data, 0, 64); }
};

struct UBInvalidateReqBody { /* no extra fields beyond header */ };

struct UBInvalidateAckBody { /* no extra fields beyond header */ };

struct UBWritebackReqBody { /* no extra fields beyond header */ };

struct UBWritebackRespBody {
    bool success;
    UBWritebackRespBody() : success(false) {}
};

struct UBEvictReqBody { /* no extra fields beyond header */ };

struct UBEvictRespBody {
    bool success;
    UBEvictRespBody() : success(false) {}
};

struct UBUpgradeReqBody {
    uint8_t desiredPerm;
    uint8_t cause;   // 0=LocalCleanUnique, 1=LocalStoreUpgrade

    UBUpgradeReqBody() : desiredPerm(0), cause(0) {}
};

struct UBUpgradeRespBody {
    uint64_t upgradeTargetMask;  // frozen sharers snapshot for invalidation fanout
    uint64_t committedEpoch;     // current committed home epoch for ack validation
    UBUpgradeRespBody() : upgradeTargetMask(0), committedEpoch(0) {}
};

struct UBUpgradeDoneReqBody { /* no extra fields beyond header */ };

struct UBUpgradeDoneRespBody {
    bool accepted;
    UBUpgradeDoneRespBody() : accepted(false) {}
};

struct UBClearReqBody {
    uint8_t reason;  // 0=GrantHandshake

    UBClearReqBody() : reason(0) {}
};

struct UBClearRespBody {
    bool accepted;
    UBClearRespBody() : accepted(false) {}
};

// v4-dual-socket new message bodies
struct UBQueryLineMetaReqBody {
    uint64_t homePa;
    UBQueryLineMetaReqBody() : homePa(0) {}
};

struct UBQueryLineMetaRespBody {
    bool found;
    uint64_t epoch;
    int ownerNode;
    UBQueryLineMetaRespBody() : found(false), epoch(0), ownerNode(-1) {}
};

struct UBHomeWritebackNotifyBody {
    uint64_t homePa;
    UBHomeWritebackNotifyBody() : homePa(0) {}
};

struct UBUpgradeAckNotifyBody {
    /* no extra fields — header-only notification */  // v4-P0 fix: FV-9 gap
};

union CoherenceMessageBody {
    UBReadReqBody readReq;
    UBReadRespBody readResp;
    UBRecallReqBody recallReq;
    UBRecallRespBody recallResp;
    UBInvalidateReqBody invalidateReq;
    UBInvalidateAckBody invalidateAck;
    UBWritebackReqBody writebackReq;
    UBWritebackRespBody writebackResp;
    UBEvictReqBody evictReq;
    UBEvictRespBody evictResp;
    UBUpgradeReqBody upgradeReq;
    UBUpgradeRespBody upgradeResp;
    UBUpgradeDoneReqBody upgradeDoneReq;
    UBUpgradeDoneRespBody upgradeDoneResp;
    UBClearReqBody clearReq;
    UBClearRespBody clearResp;
    UBQueryLineMetaReqBody queryLineMetaReq;
    UBQueryLineMetaRespBody queryLineMetaResp;
    UBHomeWritebackNotifyBody homeWritebackNotify;
    UBUpgradeAckNotifyBody upgradeAckNotify;  // v4-P0 fix: FV-9 gap

    CoherenceMessageBody() {} // value-initialized by CoherenceMessage default ctor
};

// ---- Full Message ----
struct CoherenceMessage {
    CoherenceMessageHeader h;
    CoherenceMessageBody b;

    CoherenceMessage() = default;
};

// ---- Debug helpers ----
inline const char*
coherenceMsgTypeName(CoherenceMessageType t)
{
    switch (t) {
        case CoherenceMessageType::ReadReq:          return "ReadReq";
        case CoherenceMessageType::ReadResp:         return "ReadResp";
        case CoherenceMessageType::RecallReq:        return "RecallReq";
        case CoherenceMessageType::RecallResp:       return "RecallResp";
        case CoherenceMessageType::InvalidateReq:    return "InvalidateReq";
        case CoherenceMessageType::InvalidateAck:    return "InvalidateAck";
        case CoherenceMessageType::WritebackReq:     return "WritebackReq";
        case CoherenceMessageType::WritebackResp:    return "WritebackResp";
        case CoherenceMessageType::EvictReq:         return "EvictReq";
        case CoherenceMessageType::EvictResp:        return "EvictResp";
        case CoherenceMessageType::UpgradeReq:       return "UpgradeReq";
        case CoherenceMessageType::UpgradeResp:      return "UpgradeResp";
        case CoherenceMessageType::UpgradeDoneReq:   return "UpgradeDoneReq";
        case CoherenceMessageType::UpgradeDoneResp:  return "UpgradeDoneResp";
        case CoherenceMessageType::ClearReq:         return "ClearReq";
        case CoherenceMessageType::ClearResp:        return "ClearResp";
        case CoherenceMessageType::UpgradeAckNotify: return "UpgradeAckNotify";
        case CoherenceMessageType::QueryLineMetaReq:  return "QueryLineMetaReq";
        case CoherenceMessageType::QueryLineMetaResp: return "QueryLineMetaResp";
        case CoherenceMessageType::HomeWritebackNotify: return "HomeWritebackNotify";
        default:                           return "Unknown";
    }
}

inline std::string
ubMsgToString(const CoherenceMessage &msg)
{
    char buf[512];
    snprintf(buf, sizeof(buf),
             "CoherenceMessage{src=(%u,%u) dst=(%u,%u) home=(%u,%u) ingress=%u "
             "reqNode=%u tgt=%u "
             "flags=0x%x homePA=0x%lx localPA=0x%lx "
             "epoch=%lu reqId=%lu seq=%lu}",
             coherenceMsgTypeName(msg.h.type),
             msg.h.srcNode, msg.h.srcSocket,
             msg.h.dstNode, msg.h.dstSocket,
             msg.h.homeNode, msg.h.homeSocket,
             msg.h.ingressSocket,
             msg.h.requesterNode, msg.h.targetNode,
             msg.h.flags, msg.h.homeLinePa, msg.h.localLinePa,
             msg.h.epoch, msg.h.reqId, msg.h.seqNum);
    return std::string(buf);
}

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBMSG_HH__
