#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDARY_AUTHORITY_TABLE_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDARY_AUTHORITY_TABLE_HH__
#include <array>
#include <cstdint>
#include <limits>
#include "BoundedBoundaryTypes.hh"
#include "NodeAddressMap.hh"
namespace gem5 { namespace ruby {
enum class RequesterLineState : uint8_t { R_I, R_WAIT_GRANT, R_S, R_E, R_M };
// No data, dynamic storage, implicit epoch recovery, or eviction on insertion.
// Tokens belong to one containing bank; banks must not be copied or moved.
class BoundaryAuthorityTable {
  public:
    static constexpr unsigned Capacity = 4096, Ways = 8, Sets = Capacity / Ways;
    static constexpr unsigned BorrowCapacity = 73;
    static constexpr uint64_t SegmentBytes = 128ULL * 1024 * 1024;
    enum class State : uint8_t { Free, Live, Releasing, Fenced };
    enum Flags : uint16_t { Custody = 1, Stage = 2 };
    enum class Admission { Reserved, Existing, ReleaseCandidate, Busy, Invalid };
    using Token = BoundaryStableToken;
    using Borrow = BoundaryBorrowToken;
    struct Entry {
        uint64_t epoch = 0;
        uint64_t generation = 0;
        uint32_t key = 0;
        uint16_t refs = 0;
        uint16_t flags = 0;
        State state = State::Free;
        RequesterLineState access = RequesterLineState::R_I;
        bool resident = false;
    };
    struct Reservation { Admission result; Token token; };
    struct BorrowRecord {
        uint64_t generation = 0;
        Token authority;
        bool live = false;
    };
    // The physical bank never grows. A smaller admission geometry is useful for
    // exercising production replacement paths without thousands of warmups.
    // Invalid limits fail closed: zero never means unlimited.
    explicit BoundaryAuthorityTable(unsigned generationBits = 64,
                                    unsigned entryLimit = Capacity)
        : generationLimit(generationBits == 64 ? UINT64_MAX :
          (generationBits > 0 && generationBits < 64 ?
           (uint64_t(1) << generationBits) - 1 : 0)),
          admissionLimit(entryLimit >= 1 && entryLimit <= Capacity ?
                         entryLimit : 0) {}
    BoundaryAuthorityTable(const BoundaryAuthorityTable &) = delete;
    BoundaryAuthorityTable &operator=(const BoundaryAuthorityTable &) = delete;
    static bool packKey(unsigned homeNode, unsigned homeSocket,
                        uint64_t lineOffsetAddr, unsigned domain, uint32_t &key) {
        if (homeNode >= 64 || homeSocket >= 4 || domain >= 4 ||
            lineOffsetAddr >= SegmentBytes || (lineOffsetAddr & 63)) return false;
        key = (uint32_t(domain) << 29) | (uint32_t(homeNode) << 23) |
              (uint32_t(homeSocket) << 21) | uint32_t(lineOffsetAddr >> 6);
        return true;
    }
    static bool keyFromAddress(const cc::glob::NodeAddressMap &map,
                               int localNode, uint64_t addr, unsigned domain,
                               uint32_t &key) {
        if (map.numNodes() < 1 || map.numNodes() > 64 ||
            map.numSockets() < 1 || map.numSockets() > 4 ||
            map.segSize() != SegmentBytes || localNode < 0 ||
            localNode >= map.numNodes() || (addr & 63) ||
            !map.isDsm(localNode, addr)) return false;
        return packKey(map.homeNode(localNode, addr), map.homeSocket(localNode, addr),
                       map.dsmOffset(addr), domain, key);
    }
    static unsigned setOf(uint32_t key) {
        return (key ^ (key >> 9) ^ (key >> 18) ^ (key >> 27)) & (Sets - 1);
    }
    unsigned capacity() const { return admissionLimit; }
    unsigned configuredSetOf(uint32_t key) const {
        const unsigned sets = (admissionLimit + Ways - 1) / Ways;
        return sets ? setOf(key) % sets : 0;
    }
    const Entry *get(Token t) const {
        if (!t.valid() || t.slot >= admissionLimit) return nullptr;
        const auto &e = entries[t.slot];
        return e.generation == t.generation &&
            (e.state == State::Live || e.state == State::Releasing) ? &e : nullptr;
    }
    Token find(uint32_t key) const {
        if (!admissionLimit || (key & 0x80000000U)) return {};
        const unsigned start = configuredSetOf(key) * Ways;
        const unsigned end = start + waysInSet(start);
        for (unsigned i = start; i < end; ++i) {
            const auto &e = entries[i];
            if (e.key == key && (e.state == State::Live || e.state == State::Releasing))
                return {e.generation, uint16_t(i)};
        }
        return {};
    }
    Reservation tryReserve(uint32_t key, uint64_t epoch, uint16_t flags = 0) {
        if (key & 0x80000000U || flags & ~(Custody | Stage) ||
            !generationLimit || !admissionLimit)
            return {Admission::Invalid, {}};
        auto old = find(key);
        if (const auto *e = get(old))
            return {e->state == State::Live && e->epoch == epoch ?
                    Admission::Existing : Admission::Busy, old};
        const unsigned set = configuredSetOf(key), start = set * Ways;
        const unsigned ways = waysInSet(start);
        Token candidate;
        for (unsigned n = 0; n < ways; ++n) {
            unsigned i = start + ((cursor[set] + n) % ways);
            auto &e = entries[i];
            if (e.state == State::Free) {
                if (e.generation >= generationLimit) { e.state = State::Fenced; continue; }
                ++e.generation;
                e.key = key; e.epoch = epoch; e.refs = 0; e.flags = flags;
                e.access = RequesterLineState::R_I; e.resident = false;
                e.state = State::Live;
                cursor[set] = (i - start + 1) % ways;
                return {Admission::Reserved, {e.generation, uint16_t(i)}};
            }
            if (!candidate.valid() && e.state == State::Live && !e.refs &&
                !(e.flags & Stage) && e.generation < generationLimit)
                candidate = {e.generation, uint16_t(i)};
        }
        return {candidate.valid() ? Admission::ReleaseCandidate : Admission::Busy,
                candidate};
    }
    // A fully revoking native control has handed its required data/ACK to the
    // reliable Home path. This is not inferred from R_I or Home absence. The
    // captured token and authority epoch must still name the same incarnation.
    // Borrowers remain live until their own native completion.
    BoundaryResult retireByControl(Token t, uint64_t epoch) {
        auto *e = edit(t);
        if (!e || !epoch || e->epoch != epoch) return BoundaryResult::Stale;
        if (e->state != State::Live) return BoundaryResult::Busy;
        e->epoch = 0;
        e->access = RequesterLineState::R_I;
        e->resident = false;
        return BoundaryResult::Applied;
    }
    BoundaryResult setFlags(Token t, uint16_t flags) {
        auto *e = edit(t);
        if (!e) return BoundaryResult::Stale;
        if (flags & ~(Custody | Stage)) return BoundaryResult::Invalid;
        if (e->state != State::Live) return BoundaryResult::Busy;
        e->flags = flags;
        return BoundaryResult::Applied;
    }
    BoundaryResult borrow(Token t, Borrow &out) {
        out = {};
        auto *e = edit(t);
        if (!e) return BoundaryResult::Stale;
        if (e->state != State::Live) return BoundaryResult::Busy;
        if (e->refs >= BorrowCapacity) return BoundaryResult::Full;
        for (unsigned i = 0; i < BorrowCapacity; ++i) {
            auto &b = borrowers[i];
            if (b.live || b.generation >= generationLimit) continue;
            ++b.generation; b.authority = t; b.live = true; ++e->refs;
            out = {b.generation, uint16_t(i)};
            return BoundaryResult::Applied;
        }
        return BoundaryResult::Full;
    }
    BoundaryResult dropBorrow(Borrow t) {
        if (!t.valid()) return BoundaryResult::Invalid;
        auto &b = borrowers[t.slot];
        if (b.generation != t.generation) return BoundaryResult::Stale;
        if (!b.live) return BoundaryResult::Duplicate;
        auto *e = edit(b.authority);
        if (!e || !e->refs) return BoundaryResult::Stale;
        --e->refs; b.live = false;
        return BoundaryResult::Applied;
    }
    bool ownsBorrow(Borrow b, Token t) const {
        if (!b.valid() || !get(t)) return false;
        const auto &record = borrowers[b.slot];
        return record.live && record.generation == b.generation &&
            record.authority.slot == t.slot &&
            record.authority.generation == t.generation;
    }
    BoundaryResult beginRelease(Token t, Token &release) {
        release = {};
        auto *e = edit(t);
        if (!e) return BoundaryResult::Stale;
        if (e->state != State::Live || e->refs || (e->flags & Stage))
            return BoundaryResult::Busy;
        if (e->generation >= generationLimit) return BoundaryResult::Exhausted;
        ++e->generation; e->state = State::Releasing;
        release = {e->generation, t.slot};
        return BoundaryResult::Applied;
    }
    // Callback enqueues REQUEST using a reserved escape ticket. false promises
    // nothing was issued; never cancel an issued or partially issued release.
    template<class Request>
    BoundaryResult requestRelease(Token candidate, Token &release, Request request) {
        auto result = beginRelease(candidate, release);
        if (result != BoundaryResult::Applied) return result;
        if (request(release, *get(release))) return result;
        cancelRelease(release); release = {};
        return BoundaryResult::NotReady;
    }
    BoundaryResult cancelRelease(Token t) {
        auto *e = edit(t);
        if (!e) return BoundaryResult::Stale;
        if (e->state != State::Releasing) return BoundaryResult::Duplicate;
        e->state = State::Live;
        return BoundaryResult::Applied;
    }
    // Caller asserts matching native + Home ACK and all data obligations done.
    // This bank cannot establish that fact itself. Custody stays until this call.
    BoundaryResult completeRelease(Token t, bool coherentDone) {
        if (!t.valid() || t.slot >= admissionLimit) return BoundaryResult::Invalid;
        auto &e = entries[t.slot];
        if (e.generation != t.generation) return BoundaryResult::Stale;
        if (e.state == State::Free || e.state == State::Fenced)
            return BoundaryResult::Duplicate;
        if (e.state != State::Releasing || !coherentDone || e.refs)
            return BoundaryResult::NotReady;
        e.state = e.generation == generationLimit ? State::Fenced : State::Free;
        e.flags = 0;
        return BoundaryResult::Applied;
    }
    unsigned occupancy() const {
        unsigned n = 0;
        for (const auto &e : entries)
            n += e.state == State::Live || e.state == State::Releasing;
        return n;
    }
    // Runtime metadata remains in the same 32-byte slot. A checked handle is
    // mandatory; callers must never retain this pointer across an event.
    Entry *metadata(Token t) { return edit(t); }
  private:
    unsigned waysInSet(unsigned start) const {
        const unsigned remaining = admissionLimit - start;
        return remaining < Ways ? remaining : Ways;
    }
    Entry *edit(Token t) { return const_cast<Entry *>(get(t)); }
    std::array<Entry, Capacity> entries{};
    std::array<BorrowRecord, BorrowCapacity> borrowers{};
    std::array<uint8_t, Sets> cursor{};
    const uint64_t generationLimit;
    const unsigned admissionLimit;
};
static_assert(sizeof(BoundaryAuthorityTable::Entry) <= 32, "honest authority bound");
} }
#endif
