#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDED_BOUNDARY_TYPES_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDED_BOUNDARY_TYPES_HH__
#include <cstdint>
namespace gem5 { namespace ruby {
enum class BoundaryResult : uint8_t {
    Applied, Duplicate, Stale, Invalid, Busy, Full, NotReady, Exhausted
};
enum class BoundaryOperation : uint8_t {
    None, Read, Grant, Upgrade, Invalidate, Release
};
enum class BoundaryPool : uint8_t { Ordinary, Control, Escape };
struct BoundaryStableToken {
    uint64_t generation = 0;
    uint16_t slot = 4096;
    bool valid() const { return generation && slot < 4096; }
};
struct BoundaryBorrowToken {
    uint64_t generation = 0;
    uint16_t slot = 73;
    bool valid() const { return generation && slot < 73; }
};
} }
#endif
