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
    if (mask == 0)
        return -EINVAL;

    uint32_t max_valid = (1u << MAX_NODE_COUNT) - 1u;
    if (mask & ~max_valid)
        return -EINVAL;

    auto &bs = _barriers[mask];

    if (bs.activeThreads == 0)
        bs.activeThreads = __builtin_popcount(mask) * activeThreads;

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
        crossNode = (mask & ~localBits) != 0;
    bs.crossNode = crossNode;

    if (bs.earlyReleases.erase(bs.generation) != 0)
        bs.remoteReleased = true;

    bs.waiting.insert(tc);

    if (bs.crossNode && _numSockets > 0) {
        // Per-node/per-socket barrier semantics (TC80/82/91/98 fix):
        // BarrierReached must be sent exactly ONCE per (node,socket) plane,
        // and only AFTER all local participating threads have arrived. The
        // ubio barrier coordinator aggregates by node/socket bit; firing
        // per-thread let a node's first arriving thread complete the global
        // barrier before its siblings arrived, desynchronizing `generation`.
        //
        // If a remote release already arrived for this generation, release
        // immediately (the local threads were the stragglers).
        if (bs.remoteReleased) {
            for (ThreadContext *t : bs.waiting)
                t->activate();
            bs.waiting.clear();
            bs.activeThreads = 0;
            bs.localExpected = 0;
            bs.remoteReleased = false;
            bs.reachedSent = false;
            bs.crossNode = false;
            bs.generation++;  // advance generation for next barrier
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
    auto it = _barriers.find(mask);
    if (it == _barriers.end())
        return;

    auto &bs = it->second;

    // Distributed releases are emitted by the single UBIO leader only after
    // it has observed exactly one arrival from every participating plane.
    // The seq values originate in independent gem5 processes and are not a
    // globally comparable clock, so the leader-authorized release applies to
    // this process's current waiting generation.

    if (bs.crossNode || !bs.waiting.empty()) {
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
        // Release arrived before any local thread reached this barrier
        // generation. Record it so the next barrierArrive() releases at once.
        bs.remoteReleased = true;
    }
}

} // namespace gem5
