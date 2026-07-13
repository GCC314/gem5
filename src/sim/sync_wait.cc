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
        // Per-socket split mode: fire ALL registered sockets' BarrierReached
        // callbacks, each with its own barrierBit. Workloads using per-node
        // masks (e.g. 0b111) in dual-socket mode need their masks enlarged
        // to account for per-socket senders (see per-TC audit in tests/).
        uint32_t gen = bs.generation;  // TC90 fix: tag with current generation
        for (int s = 0; s < _numSockets; s++) {
            if (_sockActive[s] && _sockets[s].sendFn) {
                _sockets[s].sendFn(mask,
                    static_cast<uint32_t>(_sockets[s].barrierBit), gen);
            }
        }
        if (!bs.remoteReleased) {
            tc->suspend();
        } else {
            for (ThreadContext *t : bs.waiting)
                t->activate();
            bs.waiting.clear();
            bs.activeThreads = 0;
            bs.remoteReleased = false;
            bs.crossNode = false;
            bs.generation++;  // TC90 fix: advance generation for next barrier
        }
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
SyncWaitManager::releaseBarrier(uint32_t mask)
{
    auto it = _barriers.find(mask);
    if (it == _barriers.end())
        return;

    auto &bs = it->second;
    if (bs.crossNode) {
        for (ThreadContext *t : bs.waiting)
            t->activate();
        bs.waiting.clear();
        bs.activeThreads = 0;
        bs.remoteReleased = false;
        bs.crossNode = false;
        bs.generation++;  // TC90 fix: advance generation for next barrier
    } else {
        bs.remoteReleased = true;
    }
}

} // namespace gem5
