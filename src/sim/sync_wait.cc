#include "sim/sync_wait.hh"

#include <cerrno>

#include "cpu/thread_context.hh"

namespace gem5
{

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

    // Multi-process split: determine if this barrier spans non-local nodes.
    // _localNodeId < 0 => legacy single-process (all nodes in-process).
    if (_localNodeId >= 0) {
        uint32_t localBit = 1u << _localNodeId;
        bs.crossNode = (mask & ~localBit) != 0;
    }

    bs.waiting.insert(tc);

    if (bs.crossNode && _localNodeId >= 0) {
        // Split mode cross-node barrier: send BARRIER_REACHED to ubio (which
        // forwards to the barrier_manager / other ubios). We suspend the local
        // thread; release happens when releaseBarrier() is called upon receipt
        // of BARRIER_RELEASE from the IPC path.
        if (_sendFn) {
            _sendFn(mask, static_cast<uint32_t>(_localNodeId));
        }
        // Only suspend if not yet released (release may have arrived already
        // from a prior send for the same mask).
        if (!bs.remoteReleased) {
            tc->suspend();
        } else {
            // Already released: release all waiting threads now.
            for (ThreadContext *t : bs.waiting)
                t->activate();
            bs.waiting.clear();
            bs.activeThreads = 0;
            bs.remoteReleased = false;
            bs.crossNode = false;
        }
        return 0;
    }

    // Legacy / local-only barrier: release when enough local threads arrive.
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
        // Release all local threads waiting on this cross-node barrier.
        for (ThreadContext *t : bs.waiting)
            t->activate();
        bs.waiting.clear();
        bs.activeThreads = 0;
        bs.remoteReleased = false;
        bs.crossNode = false;
    } else {
        // Mark as released so a subsequent barrierArrive (if the thread was
        // already suspended) can release immediately.
        bs.remoteReleased = true;
    }
}

} // namespace gem5
