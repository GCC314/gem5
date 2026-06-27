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

    bs.waiting.insert(tc);

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

} // namespace gem5
