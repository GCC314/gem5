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
    static constexpr uint32_t MAX_NODE_COUNT = 16;
    using BarrierSendFn = std::function<void(uint32_t mask, uint32_t nodeId)>;

  private:
    struct BarrierState
    {
        uint32_t activeThreads = 0;
        std::set<ThreadContext *> waiting;
        bool crossNode = false;       // mask includes non-local nodes
        bool remoteReleased = false;  // BARRIER_RELEASE received from IPC
    };

    std::map<uint32_t, BarrierState> _barriers;

    // Multi-process split: when set, barriers whose mask includes nodes other
    // than _localNodeId are routed through the IPC barrier_manager via _sendFn.
    // _localNodeId < 0 => legacy single-process mode (all in-process).
    int _localNodeId = -1;
    BarrierSendFn _sendFn;  // sends BARRIER_REACHED to ubio via Port

  public:
    SyncWaitManager() = default;
    ~SyncWaitManager() = default;

    void setLocalNodeId(int nid) { _localNodeId = nid; }
    void setBarrierSendFn(BarrierSendFn fn) { _sendFn = std::move(fn); }

    int barrierArrive(ThreadContext *tc, uint32_t mask, uint32_t activeThreads);

    // Called by UBAdapter when a BARRIER_RELEASE message is received from ubio.
    // Releases all local threads waiting on the given mask.
    void releaseBarrier(uint32_t mask);
};

} // namespace gem5

#endif // __SIM_SYNC_WAIT_HH__
