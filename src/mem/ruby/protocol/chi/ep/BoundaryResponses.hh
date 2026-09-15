#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDARY_RESPONSES_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDARY_RESPONSES_HH__

#include <array>
#include <cassert>
#include <cstddef>
#include <utility>

namespace gem5 { namespace ruby {

// A send reserves the payload cell, not merely a counter. Arrival cannot
// allocate, even if every ordinary transaction is occupied. Invisible cells
// are outstanding promises; iterators expose only delivered responses.
template<class Key, class Message, std::size_t Capacity>
class BoundaryResponses
{
    struct Route {
        uint64_t homeLinePa = 0, epoch = 0;
        uint16_t srcNode = 0, srcSocket = 0, dstNode = 0, dstSocket = 0;
        template<class Header> void set(const Header &h) {
            homeLinePa = h.homeLinePa; epoch = h.epoch;
            srcNode = h.srcNode; srcSocket = h.srcSocket;
            dstNode = h.dstNode; dstSocket = h.dstSocket;
        }
    };
    struct Cell {
        std::pair<Key, Message> value{};
        Route request{};
        bool live = false;
        bool ready = false;
    };
    std::array<Cell, Capacity> cells{};
    std::size_t used = 0;
  public:
    class iterator {
        friend class BoundaryResponses;
        BoundaryResponses *owner;
        std::size_t index;
        iterator(BoundaryResponses *o, std::size_t i) : owner(o), index(i) {
            while (index < Capacity && !owner->cells[index].ready) ++index;
        }
      public:
        auto &operator*() const { return owner->cells[index].value; }
        auto *operator->() const { return &**this; }
        iterator &operator++() { *this = iterator(owner, index + 1); return *this; }
        bool operator==(const iterator &o) const {
            return owner == o.owner && index == o.index;
        }
        bool operator!=(const iterator &o) const { return !(*this == o); }
    };
    iterator begin() { return iterator(this, 0); }
    iterator end() { return iterator(this, Capacity); }
    iterator find(const Key &key) {
        for (auto it = begin(); it != end(); ++it)
            if (it->first == key) return it;
        return end();
    }
    std::size_t size() const { return used; }
    bool reserved(const Key &key) const {
        for (const auto &c : cells) if (c.live && c.value.first == key) return true;
        return false;
    }
    // Per response class isolation prevents a Read table from consuming the
    // cells required by its subsequent Clear or a replacement Evict response.
    bool reserve(const Key &key, const Message &request, std::size_t limit) {
        std::size_t count = 0;
        for (const auto &c : cells) {
            if (!c.live) continue;
            if (c.value.first == key)
                return sameRoute(c.request, request.h);
            count += c.value.first.respType == key.respType;
        }
        if (count >= limit) return false;
        for (auto &c : cells) {
            if (c.live) continue;
            c.value.first = key; c.request.set(request.h);
            c.live = true; c.ready = false; ++used;
            return true;
        }
        return false;
    }
    bool deliver(const Key &key, const Message &message) {
        for (auto &c : cells) {
            if (!c.live || !(c.value.first == key)) continue;
            const auto &h = c.request;
            if (message.h.homeLinePa != h.homeLinePa ||
                message.h.srcNode != h.dstNode ||
                message.h.srcSocket != h.dstSocket ||
                message.h.dstNode != h.srcNode ||
                message.h.dstSocket != h.srcSocket) return false;
            // The first terminal response owns the reserved cell; duplicates
            // cannot overwrite a result before its consumer has interpreted it.
            if (!c.ready) { c.value.second = message; c.ready = true; }
            return true;
        }
        return false;
    }
    iterator erase(iterator it) {
        assert(it.owner == this && it.index < Capacity);
        const auto next = it.index + 1;
        cells[it.index] = {}; --used;
        return iterator(this, next);
    }
    void erase(const Key &key) {
        for (auto &c : cells) if (c.live && c.value.first == key) {
            c = {}; --used; return;
        }
    }
  private:
    template<class HeaderA, class HeaderB>
    static bool sameRoute(const HeaderA &a, const HeaderB &b) {
        return a.homeLinePa == b.homeLinePa && a.epoch == b.epoch &&
            a.srcNode == b.srcNode && a.srcSocket == b.srcSocket &&
            a.dstNode == b.dstNode && a.dstSocket == b.dstSocket;
    }
};
} }
#endif
