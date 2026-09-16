#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDARY_TRANSACTIONS_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_BOUNDARY_TRANSACTIONS_HH__

#include <array>
#include <cstdint>
#include <limits>
#include "BoundedBoundaryTypes.hh"

namespace gem5 { namespace ruby {

// Native data remains in CHI TBEs / EPSNF PendingWrite. This bank contains only
// incarnation and completion descriptors, not a second copy of cache data.
// Node is implicit in the containing EPBackend; key is its canonical local PA.
class BoundaryTransactions
{
  public:
    static constexpr unsigned OrdinaryCapacity = 64;
    static constexpr unsigned ControlReserve = 8;
    static constexpr unsigned EscapeReserve = 1;
    static constexpr unsigned Capacity =
        OrdinaryCapacity + ControlReserve + EscapeReserve;
    struct Token {
        uint64_t generation = 0;
        unsigned slot = Capacity;
        bool valid() const { return slot < Capacity && generation != 0; }
    };
    struct Entry {
        uint64_t line = 0;
        uint64_t epoch = 0;
        uint64_t generation = 0;
        uint64_t recallId = 0;
        uint64_t writeId = 0;
        uint64_t mergedRecallId = 0;
        int owner = -1;
        int recallSocket = -1;
        int writeSocket = -1;
        bool live = false;
        bool custody = false;
        bool recallDone = false;
        bool writeDone = false;
        bool persisted = false;
        BoundaryOperation operation = BoundaryOperation::None;
        bool operationDone = false;
        uint64_t operationId = 0;
        int operationSocket = -1;
        BoundaryStableToken authority;
        BoundaryBorrowToken borrow;
        // Native foreground and Home control are independent credit owners,
        // even when they share this physical line descriptor.
        bool ordinaryCredit = false;
        bool controlCredit = false;
        bool escapeCredit = false;
        uint64_t foregroundId = 0;
        int foregroundSocket = -1;
        unsigned foregroundDone = 0;
        uint64_t invalidateId = 0;
        int invalidateSocket = -1;
        bool invalidateDone = false;
    };

    BoundaryTransactions() = default;
    bool setOrdinaryLimit(unsigned limit) {
        if (!limit || limit > OrdinaryCapacity || occupancy()) return false;
        ordinaryLimit = limit;
        return true;
    }
    unsigned demandLimit() const { return ordinaryLimit; }
    BoundaryTransactions(const BoundaryTransactions &) = delete;
    BoundaryTransactions &operator=(const BoundaryTransactions &) = delete;

    enum ForegroundCompletion : unsigned { HomeCommit = 1, NativeClose = 2 };
    bool canInvalidate(uint64_t line, uint64_t id, int socket) const {
        if ((line & 63) || !id || socket < 0 || socket >= 4) return false;
        const auto *e = get(find(line));
        if (e && e->invalidateId)
            return !e->invalidateDone && e->invalidateId == id &&
                e->invalidateSocket == socket;
        if (e && e->recallId && !e->recallDone) return false;
        return creditUsage(BoundaryPool::Control) < ControlReserve &&
            (e || occupancy() < Capacity);
    }
    Token invalidate(uint64_t line, uint64_t epoch, int owner,
                     uint64_t id, int socket) {
        if (!canInvalidate(line, id, socket)) return {};
        auto t = find(line);
        if (!t.valid()) t = allocatePool(line, epoch, owner, false, BoundaryPool::Control);
        auto *e = edit(t);
        if (!e) return {};
        e->invalidateId = id; e->invalidateSocket = socket;
        e->controlCredit = true;
        return t;
    }
    BoundaryResult finishInvalidate(Token t, uint64_t id, int socket) {
        const auto result = identity(t);
        if (result != BoundaryResult::Applied) return result;
        auto &e = entries[t.slot];
        if (!id || e.invalidateId != id || e.invalidateSocket != socket)
            return BoundaryResult::Invalid;
        if (e.invalidateDone) return BoundaryResult::Duplicate;
        if (!e.live) return BoundaryResult::Stale;
        e.invalidateDone = true;
        retire(e);
        return BoundaryResult::Applied;
    }
    Token foreground(uint64_t line, uint64_t epoch, int owner,
                     uint64_t id, int socket) {
        if ((line & 63) || owner < 0 || !id || socket < 0 || socket >= 4)
            return {};
        auto t = find(line);
        auto *e = edit(t);
        if (e) {
            if (e->foregroundId) {
                return e->foregroundId == id && e->foregroundSocket == socket &&
                    e->foregroundDone != (HomeCommit | NativeClose) ? t : Token{};
            }
            // A fresh acquisition cannot borrow a foreign control's identity.
            return {};
        }
        t = allocatePool(line, epoch, owner, false, BoundaryPool::Ordinary);
        e = edit(t);
        if (!e) return {};
        e->foregroundId = id;
        e->foregroundSocket = socket;
        return t;
    }
    BoundaryResult finishForeground(Token t, uint64_t id, int socket,
                                    ForegroundCompletion completion) {
        const auto result = identity(t);
        if (result != BoundaryResult::Applied) return result;
        auto &e = entries[t.slot];
        if (!id || e.foregroundId != id || e.foregroundSocket != socket ||
            (completion != HomeCommit && completion != NativeClose))
            return BoundaryResult::Invalid;
        if (e.foregroundDone & completion) return BoundaryResult::Duplicate;
        if (!e.live) return BoundaryResult::Stale;
        e.foregroundDone |= completion;
        retire(e);
        return BoundaryResult::Applied;
    }
    // A native ReadShared may need an outer upgrade before its HN transaction
    // can close. Attach that child to the committed foreground; do not wait for
    // NativeClose (which depends on this child), or replace the parent's ID.
    Token upgrade(uint64_t line, uint64_t epoch, int owner,
                  uint64_t id, int socket, bool &joined) {
        joined = false;
        auto t = find(line);
        auto *e = edit(t);
        if (!e) return foreground(line, epoch, owner, id, socket);
        if (!id || !e->foregroundId || e->owner != owner ||
            e->foregroundSocket != socket ||
            !(e->foregroundDone & HomeCommit) || e->controlCredit ||
            e->escapeCredit || e->writeId || e->recallId || e->invalidateId)
            return {};
        if (e->operation != BoundaryOperation::None &&
            (e->operation != BoundaryOperation::Upgrade ||
             e->operationId != id || e->operationSocket != socket)) return {};
        e->operation = BoundaryOperation::Upgrade;
        e->operationId = id;
        e->operationSocket = socket;
        e->operationDone = false;
        joined = true;
        return t;
    }
    unsigned creditUsage(BoundaryPool pool) const {
        unsigned count = 0;
        for (const auto &e : entries)
            if (e.live) count += pool == BoundaryPool::Ordinary ? e.ordinaryCredit :
                pool == BoundaryPool::Control ? e.controlCredit : e.escapeCredit;
        return count;
    }

    // One generic operation per line, plus the legacy recall/write obligations.
    // Handles are caller-owned references: drop the borrow only when the entire
    // ticket retires, not when one merged completion returns Applied.
    template<class AuthorityBank>
    BoundaryResult tryOperation(uint64_t line, uint64_t epoch, int owner,
            BoundaryOperation op, uint64_t id, int socket, BoundaryPool pool,
            BoundaryStableToken authority, BoundaryBorrowToken borrow,
            Token &out, const AuthorityBank &bank) {
        out = {};
        if ((line & 63) || owner < 0 || !id || socket < 0 || socket >= 4 ||
            op <= BoundaryOperation::None || op > BoundaryOperation::Release ||
            pool > BoundaryPool::Escape || !authority.valid() ||
            (op != BoundaryOperation::Release && !borrow.valid()) ||
            (op == BoundaryOperation::Release && borrow.valid()) ||
            (pool == BoundaryPool::Escape && op != BoundaryOperation::Release))
            return BoundaryResult::Invalid;
        const auto *stable = bank.get(authority);
        if (!stable || stable->epoch != epoch ||
            (op == BoundaryOperation::Release ?
             stable->state != decltype(stable->state)::Releasing :
             !bank.ownsBorrow(borrow, authority)))
            return BoundaryResult::Stale;
        auto t = find(line);
        if (const auto *e = get(t)) {
            if (e->epoch != epoch || e->owner != owner ||
                e->operation != op || e->operationId != id ||
                e->operationSocket != socket ||
                e->authority.slot != authority.slot ||
                e->authority.generation != authority.generation ||
                e->borrow.slot != borrow.slot || e->borrow.generation != borrow.generation)
                return BoundaryResult::Busy;
            out = t;
            return BoundaryResult::Duplicate;
        }
        for (const auto &entry : entries) {
            if (!entry.live || entry.operation == BoundaryOperation::None) continue;
            if ((borrow.valid() && entry.borrow.slot == borrow.slot &&
                 entry.borrow.generation == borrow.generation) ||
                (op == BoundaryOperation::Release && entry.operation == op &&
                 entry.authority.slot == authority.slot &&
                 entry.authority.generation == authority.generation))
                return BoundaryResult::Busy;
        }
        t = allocatePool(line, epoch, owner, false, pool);
        auto *e = edit(t);
        if (!e) return BoundaryResult::Full;
        e->operation = op; e->operationId = id; e->operationSocket = socket;
        e->authority = authority; e->borrow = borrow;
        out = t;
        return BoundaryResult::Applied;
    }
    BoundaryResult finishOperation(Token t, BoundaryOperation op,
                                   uint64_t id, int socket) {
        auto result = identity(t);
        if (result != BoundaryResult::Applied) return result;
        auto &e = entries[t.slot];
        if (!id || op == BoundaryOperation::None || e.operation != op ||
            e.operationId != id || e.operationSocket != socket)
            return BoundaryResult::Invalid;
        if (e.operationDone) return BoundaryResult::Duplicate;
        if (!e.live) return BoundaryResult::Stale;
        e.operationDone = true; retire(e);
        return BoundaryResult::Applied;
    }
    BoundaryResult finishWriteResult(Token t, uint64_t id, int socket) {
        auto result = identity(t);
        if (result != BoundaryResult::Applied) return result;
        auto &e = entries[t.slot];
        if (!id || e.writeId != id || e.writeSocket != socket)
            return BoundaryResult::Invalid;
        if (e.writeDone) return BoundaryResult::Duplicate;
        if (!e.live) return BoundaryResult::Stale;
        if (!e.persisted) return BoundaryResult::NotReady;
        e.writeDone = true; retire(e);
        return BoundaryResult::Applied;
    }
    BoundaryResult finishRecallResult(Token t, uint64_t id, int socket) {
        auto result = identity(t);
        if (result != BoundaryResult::Applied) return result;
        auto &e = entries[t.slot];
        if (!id || e.recallId != id || e.recallSocket != socket)
            return BoundaryResult::Invalid;
        if (e.recallDone) return BoundaryResult::Duplicate;
        if (!e.live) return BoundaryResult::Stale;
        e.recallDone = true; retire(e);
        return BoundaryResult::Applied;
    }

    const Entry *get(Token t) const {
        if (!t.valid()) return nullptr;
        const auto &e = entries[t.slot];
        return e.live && e.generation == t.generation ? &e : nullptr;
    }
    Token find(uint64_t line) const {
        for (unsigned i = 0; i < Capacity; ++i)
            if (entries[i].live && entries[i].line == line)
                return {entries[i].generation, i};
        return {};
    }
    bool canRecall(uint64_t line, uint64_t id, int socket) const {
        if (!id || socket < 0 || socket >= 4 || (line & 63)) return false;
        if (const auto *e = get(find(line)))
            return (e->controlCredit || creditUsage(BoundaryPool::Control) < ControlReserve) &&
                (!e->invalidateId || e->invalidateDone) &&
                (!e->mergedRecallId || e->mergedRecallId == id) &&
                (!e->recallId ||
                 (e->recallId == id && e->recallSocket == socket));
        if (creditUsage(BoundaryPool::Control) >= ControlReserve) return false;
        for (unsigned i = 0; i < Capacity; ++i) {
            const auto &e = entries[i];
            if (!e.live && e.generation != std::numeric_limits<uint64_t>::max())
                return true;
        }
        return false;
    }
    // First admission captures identity; merging never consults current access.
    Token recall(uint64_t line, uint64_t epoch, int owner, bool custody,
                 uint64_t id, int socket) {
        // Reject malformed admissions before taking a slot. A rejected request
        // must neither leak capacity nor create a line with fabricated custody.
        if (!id || socket < 0 || socket >= 4 || owner < 0 || (line & 63)) return {};
        auto t = find(line);
        if (!canRecall(line, id, socket)) return {};
        if (!t.valid()) t = allocate(line, epoch, owner, custody, true);
        auto *e = edit(t);
        if (e && e->foregroundId && !e->recallId && !e->writeId &&
            (e->operation == BoundaryOperation::None ||
             (e->operation == BoundaryOperation::Upgrade && e->operationDone)))
            e->epoch = epoch;
        if (!e || e->epoch != epoch || e->owner != owner || !id ||
            (e->mergedRecallId && e->mergedRecallId != id) ||
            (e->recallId && (e->recallId != id || e->recallSocket != socket)))
            return {};
        e->recallId = id;
        e->controlCredit = true;
        e->recallSocket = socket;
        e->custody = e->custody || custody;
        return t;
    }
    Token write(uint64_t line, uint64_t epoch, int owner, uint64_t id,
                 int socket) {
        if (!id || socket < 0 || socket >= 4 || owner < 0 || (line & 63)) return {};
        auto t = find(line);
        if (!t.valid()) t = allocate(line, epoch, owner, true, false);
        auto *e = edit(t);
        if (e && !e->ordinaryCredit &&
            creditUsage(BoundaryPool::Ordinary) >= ordinaryLimit) return {};
        if (e && e->foregroundId && !e->recallId && !e->writeId &&
            (e->operation == BoundaryOperation::None ||
             (e->operation == BoundaryOperation::Upgrade && e->operationDone)))
            e->epoch = epoch;
        if (!e || e->epoch != epoch || e->owner != owner || !id ||
            (e->writeId && (e->writeId != id || e->writeSocket != socket)))
            return {};
        e->writeId = id;
        e->ordinaryCredit = true;
        e->writeSocket = socket;
        e->custody = true;
        return t;
    }
    bool publication(Token t, uint64_t id, int socket, uint64_t mergedRecall) {
        auto *e = edit(t);
        if (!e || !id || e->writeId != id || e->writeSocket != socket ||
            (e->persisted && e->mergedRecallId != mergedRecall) ||
            (mergedRecall && e->recallId && e->recallId != mergedRecall))
            return false;
        e->persisted = true;
        e->mergedRecallId = mergedRecall;
        return true;
    }
    bool finishWrite(Token t, uint64_t id, int socket) {
        return finishWriteResult(t, id, socket) == BoundaryResult::Applied;
    }
    bool finishRecall(Token t, uint64_t id, int socket) {
        return finishRecallResult(t, id, socket) == BoundaryResult::Applied;
    }
    unsigned occupancy() const {
        unsigned n = 0;
        for (const auto &e : entries) n += e.live;
        return n;
    }
  private:
    BoundaryResult identity(Token t) const {
        if (!t.valid()) return BoundaryResult::Invalid;
        return entries[t.slot].generation == t.generation ?
            BoundaryResult::Applied : BoundaryResult::Stale;
    }
    Entry *edit(Token t) { return const_cast<Entry *>(get(t)); }
    Token allocate(uint64_t line, uint64_t epoch, int owner, bool custody,
                   bool control) {
        return allocatePool(line, epoch, owner, custody,
                            control ? BoundaryPool::Control : BoundaryPool::Ordinary);
    }
    Token allocatePool(uint64_t line, uint64_t epoch, int owner, bool custody,
                       BoundaryPool pool) {
        // Separate pools: ordinary traffic cannot consume control or escape
        // credits. Control traffic cannot consume the replacement escape slot.
        // Joining an existing line above consumes no additional slot.
        const unsigned limit = pool == BoundaryPool::Ordinary ? ordinaryLimit :
            pool == BoundaryPool::Control ? ControlReserve : EscapeReserve;
        if (creditUsage(pool) >= limit) return {};
        const unsigned begin = 0;
        const unsigned end = Capacity;
        for (unsigned i = begin; i < end; ++i) {
            auto &e = entries[i];
            if (e.live || e.generation == std::numeric_limits<uint64_t>::max())
                continue;
            const auto generation = e.generation + 1;
            e = Entry{};
            e.line = line;
            e.epoch = epoch;
            e.owner = owner;
            e.custody = custody;
            e.generation = generation;
            e.live = true;
            e.ordinaryCredit = pool == BoundaryPool::Ordinary;
            e.controlCredit = pool == BoundaryPool::Control;
            e.escapeCredit = pool == BoundaryPool::Escape;
            return {generation, i};
        }
        return {};
    }
    static void retire(Entry &e) {
        const bool foregroundLive = e.foregroundId &&
            e.foregroundDone != (HomeCommit | NativeClose);
        const bool operationLive = e.operation != BoundaryOperation::None &&
            !e.operationDone;
        // A Home write receipt can name an already-issued Recall whose frame
        // has not arrived. Keep the write's completion reservation (ordinary),
        // not an uncharged tombstone, until the exact local cleanup completes.
        // Recording that receipt itself never needs an execution control slot.
        const bool lateRecallReceipt = e.mergedRecallId &&
            (!e.recallId || !e.recallDone);
        if (!foregroundLive && (!e.writeId || e.writeDone) && !operationLive &&
            !lateRecallReceipt)
            e.ordinaryCredit = false;
        if ((!e.recallId || e.recallDone) &&
            (!e.invalidateId || e.invalidateDone) && !operationLive &&
            (!e.mergedRecallId || e.recallId)) e.controlCredit = false;
        if (foregroundLive) return;
        if (e.invalidateId && !e.invalidateDone) return;
        // Home may publish before the already-issued Recall reaches this EP.
        // Retain its exact identity until that native cleanup actually returns.
        if (e.mergedRecallId && !e.recallId) return;
        if (e.operation != BoundaryOperation::None && !e.operationDone) return;
        if ((!e.recallId || e.recallDone) && (!e.writeId || e.writeDone))
            e.live = false;
    }
    std::array<Entry, Capacity> entries{};
    unsigned ordinaryLimit = OrdinaryCapacity;
};

} }
#endif
