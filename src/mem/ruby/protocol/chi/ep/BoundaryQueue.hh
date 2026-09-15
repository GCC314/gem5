#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDARY_QUEUE_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDARY_QUEUE_HH__

#include <array>
#include <cassert>
#include <cstddef>
#include <utility>

namespace gem5 { namespace ruby {

// Inline descriptor storage. Admission failure leaves both the queue and the
// caller's descriptor unchanged. Erasure preserves FIFO age; callers must not
// retain pointers across erase (wire identities must use bank generations).
template<class T, std::size_t Capacity>
class BoundaryQueue
{
  public:
    static_assert(Capacity > 0, "a boundary queue needs storage");
    using iterator = typename std::array<T, Capacity>::iterator;
    using const_iterator = typename std::array<T, Capacity>::const_iterator;
    bool empty() const { return count == 0; }
    std::size_t size() const { return count; }
    constexpr std::size_t capacity() const { return Capacity; }
    iterator begin() { return entries.begin(); }
    iterator end() { return entries.begin() + count; }
    const_iterator begin() const { return entries.begin(); }
    const_iterator end() const { return entries.begin() + count; }
    bool push_back(const T &value) {
        if (count == Capacity) return false;
        entries[count++] = value;
        return true;
    }
    iterator erase(iterator pos) {
        assert(pos >= begin() && pos < end());
        for (auto next = pos + 1; next != end(); ++next)
            *(next - 1) = std::move(*next);
        entries[--count] = T{};
        return pos;
    }

  private:
    std::array<T, Capacity> entries{};
    std::size_t count = 0;
};

} }
#endif
