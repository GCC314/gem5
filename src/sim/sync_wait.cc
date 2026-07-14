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
    // In per-socket split mode (_numSockets > 0) each registered socket
    // has one primary thread that will arrive. Using the workload-supplied
    // `activeThreads` (single-arg sync_wait defaults to 1) is too low when
    // multiple sockets independently participate (dual-socket: 2 primaries
    // per node). floorLocalExpected guarantees all local socket primaries
    // arrive before the node fires BarrierReached, preventing a late-arriving
    // primary from being counted in the next generation (phantom barrier,
    // TC96/97 dual-socket barrier mismatch).
    uint32_t floorLocalExpected = (uint32_t)(_numSockets > 0 ? _numSockets : 1);
    if (bs.localExpected == 0)
        bs.localExpected = (activeThreads >= floorLocalExpected)
            ? activeThreads : floorLocalExpected;

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

    // Ignore stale releases: a release whose generation does not match the
    // barrier's current generation belongs to an already-completed barrier
    // and must not release the next one that reuses this mask. `seq` values
    // strictly below the current generation are stale; a `seq` equal to the
    // current generation is the release we are waiting for.
    if (seq < bs.generation)
        return;

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
