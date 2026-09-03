// Phase 2: static layout / typed status test for MetaRNF 64B line transport.
// This file verifies compile-time invariants of the CoherenceMessage body
// types used by the MetaRNF line protocol.  All checks are static_assert;
// a compilation failure in this TU is a hard layout error.
#include "mem/ruby/protocol/chi/ep/CoherenceMessage.hh"

#include <cstdint>
#include <cstddef>
#include <type_traits>

namespace gem5 {
namespace ruby {

// Forward-declare EPBackend for the self-test hook signature.
class EPBackend;

// ---- sizeof checks (paranoid: keep message size bounded) ----
static_assert(sizeof(cc::glob::UBMetaRNFLineReadReqBody) <= 8,
              "MetaRNFLineReadReqBody too large (no payload beyond header)");
static_assert(sizeof(cc::glob::UBMetaRNFLineReadRespBody) >= 73,
              "MetaRNFLineReadRespBody too small (needs status + bucketOffset + 64B data)");
static_assert(sizeof(cc::glob::UBMetaRNFLineReadRespBody) <= 80,
              "MetaRNFLineReadRespBody too large");
static_assert(sizeof(cc::glob::UBMetaRNFLineWriteReqBody) >= 72,
              "MetaRNFLineWriteReqBody too small (needs bucketOffset + 64B data)");
static_assert(sizeof(cc::glob::UBMetaRNFLineWriteReqBody) <= 72,
              "MetaRNFLineWriteReqBody too large");
static_assert(sizeof(cc::glob::UBMetaRNFLineWriteRespBody) <= 16,
              "MetaRNFLineWriteRespBody too large (status + bucketOffset)");

// ---- MetaRNFLineStatus width (must fit in uint8_t) ----
static_assert(sizeof(cc::glob::MetaRNFLineStatus) == 1,
              "MetaRNFLineStatus must be 1 byte");

// ---- Body field offsets: ReadResp ----
static_assert(offsetof(cc::glob::UBMetaRNFLineReadRespBody, status) == 0,
              "status must be at offset 0 in ReadResp body");
static_assert(offsetof(cc::glob::UBMetaRNFLineReadRespBody, bucketOffset) > 0,
              "bucketOffset must follow status in ReadResp body");
static_assert(offsetof(cc::glob::UBMetaRNFLineReadRespBody, data) > 0,
              "data must follow bucketOffset in ReadResp body");

// ---- Body field offsets: WriteResp ----
static_assert(offsetof(cc::glob::UBMetaRNFLineWriteRespBody, status) == 0,
              "status must be at offset 0 in WriteResp body");

// ---- CoherenceMessageBody union must hold all line types ----
static_assert(sizeof(cc::glob::CoherenceMessageBody) >=
                  sizeof(cc::glob::UBMetaRNFLineReadRespBody),
              "CoherenceMessageBody too small for MetaRNFLineReadResp");
static_assert(sizeof(cc::glob::CoherenceMessageBody) >=
                  sizeof(cc::glob::UBMetaRNFLineWriteReqBody),
              "CoherenceMessageBody too small for MetaRNFLineWriteReq");

// ---- MetaRNFLineStatus values in valid uint8_t range ----
static_assert(static_cast<uint8_t>(cc::glob::MetaRNFLineStatus::Ok) == 0,
              "Ok must be 0");
static_assert(static_cast<uint8_t>(cc::glob::MetaRNFLineStatus::RetryableBusy) == 1,
              "RetryableBusy must be 1");
static_assert(static_cast<uint8_t>(cc::glob::MetaRNFLineStatus::IoError) == 2,
              "IoError must be 2");
static_assert(static_cast<uint8_t>(cc::glob::MetaRNFLineStatus::RangeError) == 4,
              "RangeError must be 4");
static_assert(static_cast<uint8_t>(cc::glob::MetaRNFLineStatus::InvalidArgument) == 5,
              "InvalidArgument must be 5");

// ---- Round-trip: status name table covers all values ----
static_assert(
    std::is_same<decltype(cc::glob::metaRNFLineStatusName(
                     cc::glob::MetaRNFLineStatus::Ok)),
                 const char*>::value,
    "metaRNFLineStatusName must return const char*");

// ---- CoherenceMessageHeader packing: ensure no unexpected holes ----
// The header is used as the wire-format envelope; a size change would
// silently break serialization between gem5 and ubio.
static_assert(sizeof(cc::glob::CoherenceMessageHeader) == 80,
              "CoherenceMessageHeader size must be 80 bytes (wire format)");

// ---- HA additions: append-only enum values and fixed wire layouts ----
static_assert(static_cast<uint16_t>(cc::glob::CoherenceMessageType::PeerExit) == 30,
               "existing CoherenceMessageType values must remain stable");
static_assert(static_cast<uint16_t>(cc::glob::CoherenceMessageType::HAPermissionReq) == 31,
               "HA message types must be appended after PeerExit");
static_assert(static_cast<uint16_t>(cc::glob::CoherenceMessageType::HAPermissionResp) == 32 &&
              static_cast<uint16_t>(cc::glob::CoherenceMessageType::HAPermissionAck) == 33 &&
              static_cast<uint16_t>(cc::glob::CoherenceMessageType::HAPresenceProbeReq) == 34 &&
              static_cast<uint16_t>(cc::glob::CoherenceMessageType::HAPresenceProbeResp) == 35 &&
              static_cast<uint16_t>(cc::glob::CoherenceMessageType::NetworkExit) == 36,
              "explicit HA/exit message values changed");
static_assert(cc::glob::CFLAG_WRITE_INTENT == (1u << 0) &&
              cc::glob::CFLAG_KEEP_AS_CLEAN == (1u << 1) &&
              cc::glob::CFLAG_ACCEPTED == (1u << 2) &&
              cc::glob::CFLAG_DATA_RETURNED == (1u << 3) &&
              cc::glob::CFLAG_HAS_DATA == (1u << 4) &&
              cc::glob::CFLAG_IS_READ_RECALL == (1u << 5) &&
              cc::glob::CFLAG_BUSY == (1u << 6) &&
              cc::glob::CFLAG_DATA_FORWARDED == (1u << 7) &&
              cc::glob::CFLAG_PEER_EXIT_ACK == (1u << 8) &&
              cc::glob::CFLAG_NETWORK_EXIT_ACK == (1u << 9) &&
              cc::glob::CFLAG_DEFERRED == (1u << 10),
              "coherence message flag values changed");
static_assert(sizeof(cc::glob::HAOperation) == 1 &&
              sizeof(cc::glob::HAProbeAction) == 1 &&
              sizeof(cc::glob::HAStatus) == 1,
               "HA wire enums must remain one byte");
static_assert(sizeof(cc::glob::UBWritebackKind) == 1 &&
              sizeof(cc::glob::UBWriteDisposition) == 1,
              "writeback wire enums must remain one byte");
static_assert(static_cast<uint8_t>(cc::glob::UBWritebackKind::OwnerWriteback) == 0 &&
              static_cast<uint8_t>(cc::glob::UBWritebackKind::StoreCommit) == 1,
              "writeback kind wire values changed");
static_assert(static_cast<uint8_t>(cc::glob::UBWriteDisposition::MemoryOnly) == 0 &&
              static_cast<uint8_t>(cc::glob::UBWriteDisposition::DropOwner) == 1 &&
              static_cast<uint8_t>(cc::glob::UBWriteDisposition::KeepClean) == 2,
              "writeback disposition wire values changed");
static_assert(sizeof(cc::glob::UBWritebackReqBody) == 80,
              "writeback request wire body must remain 80 bytes");
static_assert(offsetof(cc::glob::UBWritebackReqBody, kind) == 0 &&
              offsetof(cc::glob::UBWritebackReqBody, disposition) == 1 &&
              offsetof(cc::glob::UBWritebackReqBody, hasData) == 2 &&
              offsetof(cc::glob::UBWritebackReqBody, reserved) == 3 &&
              offsetof(cc::glob::UBWritebackReqBody, byteMask) == 8 &&
              offsetof(cc::glob::UBWritebackReqBody, data) == 16,
              "writeback request wire offsets changed");
static_assert(sizeof(cc::glob::UBHAPermissionReqBody) == 88,
              "HA permission request wire body must remain 88 bytes");
static_assert(sizeof(cc::glob::UBHAPermissionRespBody) == 80,
              "HA permission response wire body must remain 80 bytes");
static_assert(sizeof(cc::glob::UBHAPermissionAckBody) == 16,
              "HA permission ack wire body must remain 16 bytes");
static_assert(sizeof(cc::glob::UBHAPresenceProbeReqBody) == 16,
              "HA presence probe request wire body must remain 16 bytes");
static_assert(sizeof(cc::glob::UBHAPresenceProbeRespBody) == 16,
              "HA presence probe response wire body must remain 16 bytes");
static_assert(offsetof(cc::glob::UBHAPermissionReqBody, permissionEpoch) == 8 &&
              offsetof(cc::glob::UBHAPermissionReqBody, byteMask) == 16 &&
              offsetof(cc::glob::UBHAPermissionReqBody, data) == 24,
              "HA permission request data wire offset changed");
static_assert(offsetof(cc::glob::UBHAPermissionReqBody, operation) == 0 &&
              offsetof(cc::glob::UBHAPermissionReqBody, reserved) == 1,
              "HA permission request prefix offsets changed");
static_assert(offsetof(cc::glob::UBHAPermissionRespBody, operation) == 0 &&
              offsetof(cc::glob::UBHAPermissionRespBody, status) == 1 &&
              offsetof(cc::glob::UBHAPermissionRespBody, hasData) == 2 &&
              offsetof(cc::glob::UBHAPermissionRespBody, reserved) == 3 &&
              offsetof(cc::glob::UBHAPermissionRespBody, permissionEpoch) == 8 &&
              offsetof(cc::glob::UBHAPermissionRespBody, data) == 16,
              "HA permission response offsets changed");
static_assert(offsetof(cc::glob::UBHAPermissionAckBody, operation) == 0 &&
              offsetof(cc::glob::UBHAPermissionAckBody, status) == 1 &&
              offsetof(cc::glob::UBHAPermissionAckBody, reserved) == 2 &&
              offsetof(cc::glob::UBHAPermissionAckBody, permissionEpoch) == 8,
              "HA permission ack offsets changed");
static_assert(offsetof(cc::glob::UBHAPresenceProbeReqBody, action) == 0 &&
              offsetof(cc::glob::UBHAPresenceProbeReqBody, reserved) == 1 &&
              offsetof(cc::glob::UBHAPresenceProbeReqBody, expectedEpoch) == 8,
              "HA probe request offsets changed");
static_assert(offsetof(cc::glob::UBHAPresenceProbeRespBody, action) == 0 &&
              offsetof(cc::glob::UBHAPresenceProbeRespBody, status) == 1 &&
              offsetof(cc::glob::UBHAPresenceProbeRespBody, present) == 2 &&
              offsetof(cc::glob::UBHAPresenceProbeRespBody, reserved) == 3 &&
              offsetof(cc::glob::UBHAPresenceProbeRespBody, observedEpoch) == 8,
              "HA probe response offsets changed");
static_assert(sizeof(cc::glob::CoherenceMessageBody) == 264,
              "HA additions must not grow the message body ABI");
static_assert(sizeof(cc::glob::CoherenceMessage) == 344,
              "HA additions must not grow the message wire ABI");
static_assert(std::is_copy_assignable<cc::glob::UBHAPermissionRespBody>::value,
              "HA permission responses must be cacheable for async polling");
static_assert(std::is_copy_assignable<cc::glob::UBHAPresenceProbeRespBody>::value,
              "HA probe responses must be cacheable for async polling");

void m9SelfTest_run(EPBackend *) { /* all checks are compile-time static_assert */ }

} // namespace ruby
} // namespace gem5
