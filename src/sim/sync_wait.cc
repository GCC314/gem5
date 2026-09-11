#include "sim/sync_wait.hh"

#include <cerrno>

#include "cpu/thread_context.hh"

namespace gem5
{

void
SyncWaitManager::registerSocket(int socket, int barrierBit)
{
    if (socket < 0 || socket >= MAX_SOCKETS) return;
    _sockets[socket].barrierBit = barrierBit;
    _sockActive[socket] = true;
    if (socket >= _numSockets) _numSockets = socket + 1;
}

void
SyncWaitManager::registerSocketFn(int socket, BarrierSendFn fn)
{
    if (socket < 0 || socket >= MAX_SOCKETS) return;
    _sockets[socket].sendFn = std::move(fn);
    _sockActive[socket] = true;
    if (socket >= _numSockets) _numSockets = socket + 1;
}

int
SyncWaitManager::barrierArrive(ThreadContext *tc, uint32_t mask,
                                uint32_t activeThreads)
{
    // Bit 31 explicitly identifies portable startup, not an actual plane.
    // Keep the tagged mask as the state/wire key so ordinary barriers cannot
    // consume a startup release (or vice versa).
    const uint32_t planeMask = mask & ~0x80000000u;
    if (planeMask == 0)
        return -EINVAL;

    uint32_t max_valid = (1u << MAX_NODE_COUNT) - 1u;
    if (planeMask & ~max_valid)
        return -EINVAL;

    auto &bs = _barriers[mask];

    if (bs.activeThreads == 0)
        bs.activeThreads = __builtin_popcount(planeMask) * activeThreads;

    // Local-node expected thread count for this barrier generation.
    // The workload supplies the authoritative per-node thread count via the
    // second argument of sync_wait(mask, activeThreads).  Multi-socket
    // workloads (8n2s) must pass NUM_SOCKETS explicitly; single-socket
    // workloads (1s/2s/8n1s) use the default single-arg sync_wait(mask)
    // which sets activeThreads = 1 (one primary per node).
    if (bs.localExpected == 0)
        bs.localExpected = activeThreads;

    if (bs.waiting.find(tc) != bs.waiting.end())
        return 0;

    // Determine if cross-node: mask has bits that are NOT from any
    // registered socket (i.e., bits from other nodes' sockets).
    bool crossNode = false;
    uint32_t localBits = 0;
    for (int s = 0; s < _numSockets; s++) {
        if (_sockActive[s])
            localBits |= (1u << _sockets[s].barrierBit);
    }
    if (localBits > 0)
        crossNode = (planeMask & ~localBits) != 0;
    bs.crossNode = crossNode;

    for (uint32_t &seq : bs.earlyReleases) {
        if (seq == bs.generation) {
            seq = BarrierState::NoEarlyRelease;
            bs.remoteReleased = true;
            break;
        }
    }

    bs.waiting.insert(tc);

    if (bs.crossNode && _numSockets > 0) {
        // Per-node/per-socket barrier semantics (TC80/82/91/98 fix):
        // BarrierReached must be sent exactly ONCE per (node,socket) plane,
        // and only AFTER all local participating threads have arrived. The
        // ubio barrier coordinator aggregates by node/socket bit; firing
        // per-thread let a node's first arriving thread complete the global
        // barrier before its siblings arrived, desynchronizing `generation`.
        //
        // If a remote release arrived before all local socket threads, retain
        // it until every local participant reaches this generation. Releasing
        // the first arrival advances the generation underneath the remaining
        // socket thread and permanently desynchronizes dual-socket barriers.
        if (bs.remoteReleased) {
            if (bs.waiting.size() >= bs.localExpected) {
                for (ThreadContext *t : bs.waiting)
                    t->activate();
                bs.waiting.clear();
                bs.activeThreads = 0;
                bs.localExpected = 0;
                bs.remoteReleased = false;
                bs.reachedSent = false;
                bs.crossNode = false;
                bs.generation++;  // advance generation for next barrier
            } else {
                tc->suspend();
            }
            return 0;
        }

        // Only fire BarrierReached once all local threads have arrived, and
        // only once per generation.
        if (!bs.reachedSent && bs.waiting.size() >= bs.localExpected) {
            bs.reachedSent = true;
            uint32_t gen = bs.generation;  // tag with current generation
            for (int s = 0; s < _numSockets; s++) {
                if (_sockActive[s] && _sockets[s].sendFn) {
                    _sockets[s].sendFn(mask,
                        static_cast<uint32_t>(_sockets[s].barrierBit), gen);
                }
            }
        }

        tc->suspend();
        return 0;
    }

    // Legacy / local-only barrier.
    if (bs.waiting.size() >= bs.activeThreads) {
        for (ThreadContext *t : bs.waiting)
            t->activate();
        bs.waiting.clear();
        bs.activeThreads = 0;
        return 0;
    }

    tc->suspend();
    return 0;
}

void
SyncWaitManager::releaseBarrier(uint32_t mask, uint32_t seq)
{
    // A fast peer may finish the first barrier before this process reaches
    // sync_wait. Materialize state so that release is retained below.
    auto &bs = _barriers[mask];

    // Distributed releases are emitted by the single UBIO leader only after
    // it has observed exactly one arrival from every participating plane.
    // The seq values originate in independent gem5 processes and are not a
    // globally comparable clock, so the leader-authorized release applies to
    // this process's current waiting generation.

    if (seq == bs.generation && (bs.crossNode || !bs.waiting.empty())) {
        // A release may arrive after only the first local socket thread. Keep
        // it pending until every local participant reaches this generation;
        // otherwise the late socket thread is inserted into the next barrier.
        bs.remoteReleased = true;
        if (bs.localExpected > 0 &&
            bs.waiting.size() >= bs.localExpected) {
            for (ThreadContext *t : bs.waiting)
                t->activate();
            bs.waiting.clear();
            bs.activeThreads = 0;
            bs.localExpected = 0;
            bs.remoteReleased = false;
            bs.reachedSent = false;
            bs.crossNode = false;
            bs.generation++;  // advance generation for next barrier
        }
    } else if (seq >= bs.generation) {
        for (uint32_t &saved : bs.earlyReleases) {
            if (saved == seq)
                return;
            if (saved == BarrierState::NoEarlyRelease) {
                saved = seq;
                return;
            }
        }
    }
}

} // namespace gem5
