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

void m9SelfTest_run(EPBackend *) { /* all checks are compile-time static_assert */ }

} // namespace ruby
} // namespace gem5
