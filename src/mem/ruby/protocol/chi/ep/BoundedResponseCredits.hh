#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDED_RESPONSE_CREDITS_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDED_RESPONSE_CREDITS_HH__
#include <array>
#include <cstddef>
#include "BoundedBoundaryTypes.hh"
namespace gem5 { namespace ruby {
// Index-only queue. Payload remains in its original bounded owner. Reservation
// precedes request issue; enqueue/retire verify the same transaction generation.
template<std::size_t Capacity = 73>
class BoundedResponseCredits {
  public:
    struct Index { uint64_t generation = 0; uint16_t slot = Capacity; };
    enum class State : uint8_t { Free, Reserved, Ready };
    struct Entry { uint64_t generation = 0; State state = State::Free; };
    BoundaryResult reserve(Index t) {
        if (t.slot >= Capacity || !t.generation) return BoundaryResult::Invalid;
        auto &e = entries[t.slot];
        if (t.generation < e.generation) return BoundaryResult::Stale;
        if (t.generation == e.generation) return BoundaryResult::Duplicate;
        if (e.state != State::Free) return BoundaryResult::Busy;
        e.generation = t.generation; e.state = State::Reserved;
        return BoundaryResult::Applied;
    }
    BoundaryResult publish(Index t) {
        auto r = identity(t);
        if (r != BoundaryResult::Applied) return r;
        auto &e = entries[t.slot];
        if (e.state != State::Reserved) return BoundaryResult::Duplicate;
        e.state = State::Ready;
        queue[(front + count) % Capacity] = t.slot; ++count;
        return BoundaryResult::Applied;
    }
    bool peek(Index &out) const {
        if (!count) return false;
        out = {entries[queue[front]].generation, queue[front]};
        return true;
    }
    BoundaryResult retire(Index t) {
        auto r = identity(t);
        if (r != BoundaryResult::Applied) return r;
        auto &e = entries[t.slot];
        if (e.state == State::Free) return BoundaryResult::Duplicate;
        if (e.state != State::Ready || !count || queue[front] != t.slot)
            return BoundaryResult::NotReady;
        front = (front + 1) % Capacity; --count; e.state = State::Free;
        return BoundaryResult::Applied;
    }
    // Only for a request that was never issued, not a dropped wire response.
    BoundaryResult cancelUnissued(Index t) {
        auto r = identity(t);
        if (r != BoundaryResult::Applied) return r;
        auto &e = entries[t.slot];
        if (e.state == State::Free) return BoundaryResult::Duplicate;
        if (e.state != State::Reserved) return BoundaryResult::Busy;
        e.state = State::Free;
        return BoundaryResult::Applied;
    }
    std::size_t size() const { return count; }
  private:
    static_assert(Capacity > 0 && Capacity <= 65535, "index width");
    BoundaryResult identity(Index t) const {
        if (t.slot >= Capacity || !t.generation) return BoundaryResult::Invalid;
        return entries[t.slot].generation == t.generation ?
            BoundaryResult::Applied : BoundaryResult::Stale;
    }
    std::array<Entry, Capacity> entries{};
    std::array<uint16_t, Capacity> queue{};
    std::size_t front = 0, count = 0;
};
} }
#endif
