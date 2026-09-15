#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDARY_SLOTS_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDARY_SLOTS_HH__

#include <array>
#include <cassert>
#include <cstddef>
#include <utility>

namespace gem5 { namespace ruby {

// Fixed storage for independently completing descriptors. Unlike a compacting
// vector, removing one transaction cannot invalidate another live descriptor.
// Keys are wire identities allocated by the owning endpoint, not slot indices.
template<class Key, class Value, std::size_t Capacity>
class BoundarySlots
{
    struct Slot {
        std::pair<Key, Value> value{};
        bool live = false;
    };

  public:
    class iterator
    {
        friend class BoundarySlots;
        BoundarySlots *owner;
        std::size_t index;
        iterator(BoundarySlots *table, std::size_t slot)
            : owner(table), index(slot) { skip(); }
        void skip() {
            while (index < Capacity && !owner->slots[index].live) ++index;
        }
      public:
        std::pair<Key, Value> &operator*() const {
            assert(index < Capacity && owner->slots[index].live);
            return owner->slots[index].value;
        }
        std::pair<Key, Value> *operator->() const { return &**this; }
        iterator &operator++() { ++index; skip(); return *this; }
        iterator operator++(int) { auto old = *this; ++*this; return old; }
        bool operator==(const iterator &other) const {
            return owner == other.owner && index == other.index;
        }
        bool operator!=(const iterator &other) const { return !(*this == other); }
    };
    iterator begin() { return iterator(this, 0); }
    iterator end() { return iterator(this, Capacity); }
    std::size_t size() const { return used; }
    bool empty() const { return !used; }
    iterator find(const Key &key) {
        for (auto it = begin(); it != end(); ++it)
            if (it->first == key) return it;
        return end();
    }
    std::size_t count(const Key &key) { return find(key) != end(); }
    std::pair<iterator, bool> emplace(const Key &key, const Value &value) {
        auto old = find(key);
        if (old != end()) return {old, false};
        for (std::size_t i = 0; i < Capacity; ++i) {
            auto &slot = slots[i];
            if (slot.live) continue;
            slot.value = {key, value};
            slot.live = true;
            ++used;
            return {iterator(this, i), true};
        }
        return {end(), false};
    }
    void erase(iterator it) {
        assert(it.owner == this && it.index < Capacity);
        auto &slot = slots[it.index];
        assert(slot.live);
        slot.live = false;
        slot.value = {};
        --used;
    }

  private:
    std::array<Slot, Capacity> slots{};
    std::size_t used = 0;
};

} }
#endif
