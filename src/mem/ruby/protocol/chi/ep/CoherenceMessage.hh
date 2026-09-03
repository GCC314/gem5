#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBMSG_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBMSG_HH__

#include <cstdint>
#include <cstdio>
#include <cstring>
#include <string>

namespace cc
{
namespace glob
{

using Tick = uint64_t;
using Addr = uint64_t;

enum class GrantDataSource : uint8_t {
    HomeMemory = 0,
    RecallBuffer = 1,
    NoData = 2,
};

// ---- Typed line operation status (Phase 2: MetaRNF 64B line transport) ----
enum class MetaRNFLineStatus : uint8_t {
    Ok            = 0,
    RetryableBusy = 1,
    IoError       = 2,
    Corrupt       = 3,
    RangeError    = 4,  // PA outside metadata DRAM range
    InvalidArgument = 5,
};

// ---- HA permission / presence protocol wire enums ----
enum class HAOperation : uint8_t {
    Read  = 0,
    Write = 1,
};

enum class HAProbeAction : uint8_t {
    Query    = 0,
    Validate = 1,
};

enum class HAStatus : uint8_t {
    Ok              = 0,
    RetryableBusy   = 1,
    Denied          = 2,
    NotPresent      = 3,
    InvalidArgument = 4,
};

static constexpr const char* haOperationName(HAOperation op)
{
    switch (op) {
        case HAOperation::Read:  return "Read";
        case HAOperation::Write: return "Write";
    }
    return "Unknown";
}

static constexpr const char* haProbeActionName(HAProbeAction action)
{
    switch (action) {
        case HAProbeAction::Query:    return "Query";
        case HAProbeAction::Validate: return "Validate";
    }
    return "Unknown";
}

static constexpr const char* haStatusName(HAStatus status)
{
    switch (status) {
        case HAStatus::Ok:              return "Ok";
        case HAStatus::RetryableBusy:   return "RetryableBusy";
        case HAStatus::Denied:          return "Denied";
        case HAStatus::NotPresent:      return "NotPresent";
        case HAStatus::InvalidArgument: return "InvalidArgument";
    }
    return "Unknown";
}

static constexpr const char* metaRNFLineStatusName(MetaRNFLineStatus s)
{
    switch (s) {
        case MetaRNFLineStatus::Ok:             return "Ok";
        case MetaRNFLineStatus::RetryableBusy:  return "RetryableBusy";
        case MetaRNFLineStatus::IoError:        return "IoError";
        case MetaRNFLineStatus::Corrupt:        return "Corrupt";
        case MetaRNFLineStatus::RangeError:     return "RangeError";
        case MetaRNFLineStatus::InvalidArgument:return "InvalidArgument";
    }
    return "Unknown";
}

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
    BarrierReached,          // cross-node barrier: a node has arrived (mask in body)
    BarrierRelease,          // cross-node barrier: release the nodes in the mask
    MetaRNFReadReq,          // Phase 3: ubio → gem5, read a 256B metadata page via MetaRNF
    MetaRNFReadResp,         // Phase 3: gem5 → ubio, response with 256B page data
    MetaRNFWriteReq,         // Phase 3: ubio → gem5, write a 256B metadata page via MetaRNF
    MetaRNFWriteResp,        // Phase D2: gem5 → ubio, write ack (success/failure)

    // Phase 2: 64B line operations with typed status
    MetaRNFLineReadReq,      // ubio → gem5, read a single 64B metadata line
    MetaRNFLineReadResp,     // gem5 → ubio, response with 64B data + typed status
    MetaRNFLineWriteReq,     // ubio → gem5, write a single 64B metadata line
    MetaRNFLineWriteResp,    // gem5 → ubio, write ack with typed status
    // Version 1 requires reqId!=0 and seqNum=1. Mixed old/new UBIO processes
    // are intentionally unsupported; upgrade every plane as one deployment.
    PeerExit = 30,           // Notify unless CFLAG_PEER_EXIT_ACK is set
    // HA protocol additions are append-only: existing wire values stay stable.
    HAPermissionReq = 31,    // permission read/write request; write carries 64B
    HAPermissionResp = 32,   // permission response; successful read carries 64B
    HAPermissionAck = 33,    // requester acknowledges accepted permission result
    HAPresenceProbeReq = 34, // query/validate line presence at an HA participant
    HAPresenceProbeResp = 35,// typed presence result
    NetworkExit = 36,        // UBIO/networksim application-level shutdown
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
    CFLAG_DATA_FORWARDED  = 1u << 7,  // C4: data was direct-forwarded from owner to requester
    CFLAG_PEER_EXIT_ACK   = 1u << 8,  // PeerExit ACK; clear means Notify
    CFLAG_NETWORK_EXIT_ACK = 1u << 9, // NetworkExit ACK; clear means Request
    CFLAG_DEFERRED         = 1u << 10, // Request accepted into deferred replay
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
    uint64_t authEpoch;         // GRANT_HANDSHAKE base epoch used by Clear
    uint64_t grantEpoch;        // committed epoch the granted cache line owns
    uint64_t committedEpoch;    // current committed home epoch
    uint64_t pendingInvMask;    // sharers still awaiting invalidation
    uint8_t grantData[64];      // optional recall-buffer payload for grant

    UBReadRespBody()
        : grantType(-1), dataSource(0), pendingInvCount(-1),
          grantVisibleTick(0), sentinelVisibleTick(0),
          recallNeeded(false), recallOwnerNode(-1), authEpoch(0), grantEpoch(0),
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

enum class UBWritebackKind : uint8_t {
    OwnerWriteback = 0,
    StoreCommit = 1,
    InternalPublication = 2,
};

enum class UBWriteDisposition : uint8_t {
    MemoryOnly = 0,
    DropOwner = 1,
    KeepClean = 2,
};

struct UBWritebackReqBody {
    UBWritebackKind kind;
    UBWriteDisposition disposition;
    uint8_t hasData;
    uint8_t reserved[5];
    uint64_t byteMask;
    uint8_t data[64];
    UBWritebackReqBody()
        : kind(UBWritebackKind::OwnerWriteback),
          disposition(UBWriteDisposition::DropOwner), hasData(0), reserved{},
          byteMask(0), data{} {}
};

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

// Cross-node barrier control (BarrierReached / BarrierRelease). The arriving /
// released node id travels in the header's srcNode field; `mask` is the set of
// participating nodes. Carried as a PAYLOAD CoherenceMessage so the transport
// layer (MemMessageType) only needs PAYLOAD/TERMINATE/CONTROL_SYNC.
struct UBBarrierBody {
    uint32_t mask;
    uint32_t seq;     // barrier generation — distinguishes successive barriers
                      // sharing the same mask (TC90 fix)
    UBBarrierBody() : mask(0), seq(0) {}
};

// Phase 3: MetaRNF metadata page access (256B pages)
struct UBMetaRNFBody {
    uint64_t pagePa;
    uint8_t  data[256];
    UBMetaRNFBody() : pagePa(0) { memset(data, 0, 256); }
};

// Phase 2: MetaRNF 64B line read request
struct UBMetaRNFLineReadReqBody {
    uint64_t bucketOffset;  // Req B: logical flat-bucket index; UBAdapter maps to physical
    UBMetaRNFLineReadReqBody() : bucketOffset(0) {}
};

// Phase 2: MetaRNF 64B line read response
struct UBMetaRNFLineReadRespBody {
    MetaRNFLineStatus status;   // typed: Ok, RetryableBusy, IoError, Corrupt, RangeError, InvalidArgument
    uint64_t bucketOffset;      // Req B: echoed logical offset from request
    uint8_t  data[64];          // valid only when status == Ok
    // padding implicit at end
    UBMetaRNFLineReadRespBody() : status(MetaRNFLineStatus::IoError),
        bucketOffset(0) { memset(data, 0, 64); }
};

// Phase 2: MetaRNF 64B line write request
struct UBMetaRNFLineWriteReqBody {
    uint64_t bucketOffset;  // Req B: logical flat-bucket index
    uint8_t  data[64];
    UBMetaRNFLineWriteReqBody() : bucketOffset(0) { memset(data, 0, 64); }
};

// Phase 2: MetaRNF 64B line write response
struct UBMetaRNFLineWriteRespBody {
    MetaRNFLineStatus status;   // typed: Ok, RetryableBusy, IoError, Corrupt, RangeError, InvalidArgument
    uint64_t bucketOffset;      // Req B: echoed logical offset from request
    UBMetaRNFLineWriteRespBody() : status(MetaRNFLineStatus::IoError), bucketOffset(0) {}
};

// HA permission requests have the fixed wire order operation, reserved[7],
// permissionEpoch, byteMask, data[64]. Read uses mask zero; a full write uses
// all ones; a partial write uses a non-zero proper subset. Explicit reserved
// bytes make offsets independent of compiler-inserted interior padding.
struct UBHAPermissionReqBody {
    HAOperation operation;
    uint8_t reserved[7];
    uint64_t permissionEpoch;
    uint64_t byteMask;
    uint8_t data[64];
    UBHAPermissionReqBody()
        : operation(HAOperation::Read), reserved{}, permissionEpoch(0),
          byteMask(0), data{} {}
};

struct UBHAPermissionRespBody {
    HAOperation operation;
    HAStatus status;
    uint8_t hasData;
    uint8_t reserved[5];
    uint64_t permissionEpoch;
    uint8_t data[64];
    UBHAPermissionRespBody()
        : operation(HAOperation::Read), status(HAStatus::InvalidArgument),
          hasData(0), reserved{}, permissionEpoch(0), data{} {}
};

struct UBHAPermissionAckBody {
    HAOperation operation;
    HAStatus status;
    uint8_t reserved[6];
    uint64_t permissionEpoch;
    UBHAPermissionAckBody()
        : operation(HAOperation::Read), status(HAStatus::InvalidArgument),
          reserved{}, permissionEpoch(0) {}
};

struct UBHAPresenceProbeReqBody {
    HAProbeAction action;
    uint8_t reserved[7];
    uint64_t expectedEpoch;
    UBHAPresenceProbeReqBody()
        : action(HAProbeAction::Query), reserved{}, expectedEpoch(0) {}
};

struct UBHAPresenceProbeRespBody {
    HAProbeAction action;
    HAStatus status;
    uint8_t present;
    uint8_t reserved[5];
    uint64_t observedEpoch;
    UBHAPresenceProbeRespBody()
        : action(HAProbeAction::Query), status(HAStatus::InvalidArgument),
          present(0), reserved{}, observedEpoch(0) {}
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
    UBBarrierBody barrier;                    // BarrierReached / BarrierRelease
    UBMetaRNFBody  metaRNF;                    // MetaRNFReadReq/Resp, MetaRNFWriteReq
    UBMetaRNFLineReadReqBody  metaRNFLineReadReq;    // Phase 2: MetaRNFLineReadReq
    UBMetaRNFLineReadRespBody metaRNFLineReadResp;   // Phase 2: MetaRNFLineReadResp
    UBMetaRNFLineWriteReqBody metaRNFLineWriteReq;   // Phase 2: MetaRNFLineWriteReq
    UBMetaRNFLineWriteRespBody metaRNFLineWriteResp; // Phase 2: MetaRNFLineWriteResp
    UBHAPermissionReqBody haPermissionReq;
    UBHAPermissionRespBody haPermissionResp;
    UBHAPermissionAckBody haPermissionAck;
    UBHAPresenceProbeReqBody haPresenceProbeReq;
    UBHAPresenceProbeRespBody haPresenceProbeResp;

    // The largest body covers the entire union and initializes every byte.
    // Senders then assign the active tagged member explicitly.
    CoherenceMessageBody() : metaRNF() {}
};

// ---- Full Message ----
struct CoherenceMessage {
    CoherenceMessageHeader h;
    CoherenceMessageBody b;

    CoherenceMessage() : h(), b() {}
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
        case CoherenceMessageType::BarrierReached:   return "BarrierReached";
        case CoherenceMessageType::BarrierRelease:   return "BarrierRelease";
        case CoherenceMessageType::MetaRNFReadReq:   return "MetaRNFReadReq";
        case CoherenceMessageType::MetaRNFReadResp:  return "MetaRNFReadResp";
        case CoherenceMessageType::MetaRNFWriteReq:  return "MetaRNFWriteReq";
        case CoherenceMessageType::MetaRNFWriteResp: return "MetaRNFWriteResp";
        case CoherenceMessageType::MetaRNFLineReadReq:  return "MetaRNFLineReadReq";
        case CoherenceMessageType::MetaRNFLineReadResp: return "MetaRNFLineReadResp";
        case CoherenceMessageType::MetaRNFLineWriteReq: return "MetaRNFLineWriteReq";
        case CoherenceMessageType::MetaRNFLineWriteResp:return "MetaRNFLineWriteResp";
        case CoherenceMessageType::PeerExit:            return "PeerExit";
        case CoherenceMessageType::HAPermissionReq:     return "HAPermissionReq";
        case CoherenceMessageType::HAPermissionResp:    return "HAPermissionResp";
        case CoherenceMessageType::HAPermissionAck:     return "HAPermissionAck";
        case CoherenceMessageType::HAPresenceProbeReq:  return "HAPresenceProbeReq";
        case CoherenceMessageType::HAPresenceProbeResp: return "HAPresenceProbeResp";
        case CoherenceMessageType::NetworkExit:          return "NetworkExit";
        default:                           return "Unknown";
    }
}

inline std::string
ubMsgToString(const CoherenceMessage &msg)
{
    char buf[512];
    snprintf(buf, sizeof(buf),
             "CoherenceMessage{type=%s src=(%u,%u) dst=(%u,%u) home=(%u,%u) ingress=%u "
             "reqNode=%u tgt=%u "
             "flags=0x%x homePA=0x%llx localPA=0x%llx "
             "epoch=%llu reqId=%llu seq=%llu}",
             coherenceMsgTypeName(msg.h.type),
             msg.h.srcNode, msg.h.srcSocket,
             msg.h.dstNode, msg.h.dstSocket,
             msg.h.homeNode, msg.h.homeSocket,
             msg.h.ingressSocket,
             msg.h.requesterNode, msg.h.targetNode,
             msg.h.flags,
             static_cast<unsigned long long>(msg.h.homeLinePa),
             static_cast<unsigned long long>(msg.h.localLinePa),
             static_cast<unsigned long long>(msg.h.epoch),
             static_cast<unsigned long long>(msg.h.reqId),
             static_cast<unsigned long long>(msg.h.seqNum));
    return std::string(buf);
}

} // namespace glob
} // namespace cc

// Keep the gem5-facing include compatible with the historical forwarding
// header while allowing this file to be mirrored byte-for-byte.
namespace gem5 { namespace ruby {
using namespace cc::glob;
} }

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBMSG_HH__
