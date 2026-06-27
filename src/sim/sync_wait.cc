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

    // First thread sets the expected count
    if (bs.activeThreads == 0)
        bs.activeThreads = activeThreads;

    // Duplicate arrival: ignore
    if (bs.waiting.find(tc) != bs.waiting.end())
        return 0;

    bs.waiting.insert(tc);

    if (bs.waiting.size() < bs.activeThreads) {
        tc->suspend();
        return 0;
    }

    // All local threads arrived — send BARRIER_REACHED to BarrierManager
    if (_sendFn) {
        _sendFn(mask, _nodeId);
    }

    // Suspend the last thread too (wait for BARRIER_RELEASE from BarrierManager)
    tc->suspend();
    return 0;
}

void
SyncWaitManager::barrierRelease(uint32_t mask)
{
    auto it = _barriers.find(mask);
    if (it == _barriers.end())
        return;

    for (ThreadContext *t : it->second.waiting)
        t->activate();

    it->second.waiting.clear();
    it->second.activeThreads = 0;
}

} // namespace gem5
