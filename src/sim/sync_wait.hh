#ifndef __SIM_SYNC_WAIT_HH__
#define __SIM_SYNC_WAIT_HH__

#include <cstdint>
#include <functional>
#include <map>
#include <set>

namespace gem5
{

class ThreadContext;

class SyncWaitManager
{
  public:
    static constexpr uint32_t MAX_NODE_COUNT = 3;
    using BarrierSendFn = std::function<void(uint32_t mask, uint32_t nodeId)>;

  private:
    struct BarrierState
    {
        uint32_t activeThreads = 0;
        std::set<ThreadContext *> waiting;
    };

    std::map<uint32_t, BarrierState> _barriers;

  public:
    SyncWaitManager() = default;
    ~SyncWaitManager() = default;

    int barrierArrive(ThreadContext *tc, uint32_t mask, uint32_t activeThreads);
};

} // namespace gem5

#endif
