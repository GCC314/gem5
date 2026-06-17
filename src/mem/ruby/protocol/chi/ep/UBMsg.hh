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
enum class UBMsgType : uint16_t {
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
};

// ---- Message Flags ----
enum UBMsgFlags : uint32_t {
    UB_FLAG_WRITE_INTENT   = 1u << 0,
    UB_FLAG_KEEP_AS_CLEAN  = 1u << 1,
    UB_FLAG_ACCEPTED       = 1u << 2,
    UB_FLAG_DATA_RETURNED  = 1u << 3,
    UB_FLAG_HAS_DATA       = 1u << 4,
    UB_FLAG_IS_READ_RECALL = 1u << 5,
    UB_FLAG_BUSY            = 1u << 6,
};

// ---- Message Header (fixed envelope) ----
struct UBMsgHeader {
    UBMsgType type;
    uint16_t srcNode;
    uint16_t dstNode;
    uint16_t homeNode;
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

    UBMsgHeader()
        : type(UBMsgType::ReadReq),
          srcNode(0), dstNode(0), homeNode(0),
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

union UBMsgBody {
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

    UBMsgBody() {} // value-initialized by UBMsg default ctor
};

// ---- Full Message ----
struct UBMsg {
    UBMsgHeader h;
    UBMsgBody b;

    UBMsg() = default;
};

// ---- Debug helpers ----
inline const char*
ubMsgTypeName(UBMsgType t)
{
    switch (t) {
        case UBMsgType::ReadReq:          return "ReadReq";
        case UBMsgType::ReadResp:         return "ReadResp";
        case UBMsgType::RecallReq:        return "RecallReq";
        case UBMsgType::RecallResp:       return "RecallResp";
        case UBMsgType::InvalidateReq:    return "InvalidateReq";
        case UBMsgType::InvalidateAck:    return "InvalidateAck";
        case UBMsgType::WritebackReq:     return "WritebackReq";
        case UBMsgType::WritebackResp:    return "WritebackResp";
        case UBMsgType::EvictReq:         return "EvictReq";
        case UBMsgType::EvictResp:        return "EvictResp";
        case UBMsgType::UpgradeReq:       return "UpgradeReq";
        case UBMsgType::UpgradeResp:      return "UpgradeResp";
        case UBMsgType::UpgradeDoneReq:   return "UpgradeDoneReq";
        case UBMsgType::UpgradeDoneResp:  return "UpgradeDoneResp";
        case UBMsgType::ClearReq:         return "ClearReq";
        case UBMsgType::ClearResp:        return "ClearResp";
        case UBMsgType::UpgradeAckNotify: return "UpgradeAckNotify";
        default:                          return "Unknown";
    }
}

inline std::string
ubMsgToString(const UBMsg &msg)
{
    char buf[256];
    snprintf(buf, sizeof(buf),
             "UBMsg{%s src=%u dst=%u home=%u reqNode=%u tgt=%u "
             "flags=0x%x homePA=0x%lx localPA=0x%lx "
             "epoch=%lu reqId=%lu seq=%lu}",
             ubMsgTypeName(msg.h.type),
             msg.h.srcNode, msg.h.dstNode, msg.h.homeNode,
             msg.h.requesterNode, msg.h.targetNode,
             msg.h.flags, msg.h.homeLinePa, msg.h.localLinePa,
             msg.h.epoch, msg.h.reqId, msg.h.seqNum);
    return std::string(buf);
}

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBMSG_HH__
