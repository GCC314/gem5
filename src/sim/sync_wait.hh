#ifndef __SIM_SYNC_WAIT_HH__
#define __SIM_SYNC_WAIT_HH__

#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <vector>

namespace gem5
{

class ThreadContext;

class SyncWaitManager
{
  public:
    static constexpr uint32_t MAX_NODE_COUNT = 16;
    static constexpr uint32_t MAX_SOCKETS = 2;
    using BarrierSendFn = std::function<void(uint32_t mask, uint32_t srcBit)>;

  private:
    struct BarrierState
    {
        uint32_t activeThreads = 0;
        std::set<ThreadContext *> waiting;
        bool crossNode = false;
        bool remoteReleased = false;
    };

    struct SocketReg {
        int barrierBit;
        BarrierSendFn sendFn;
    };

    std::map<uint32_t, BarrierState> _barriers;

    // Per-socket barrier registration: each socket independently sends
    // BarrierReached with its own barrierBit when the local barrier completes.
    // Indexed by socket id (0..MAX_SOCKETS-1). Empty slots (no registration)
    // are skipped when firing.
    SocketReg _sockets[MAX_SOCKETS] = {};
    bool _sockActive[MAX_SOCKETS] = {};
    int _numSockets = 0;

  public:
    SyncWaitManager() = default;
    ~SyncWaitManager() = default;

    // Legacy single-slot API — registers socket 0 (backward compat).
    void setLocalNodeId(int nid) { registerSocket(0, nid); }
    void setBarrierSendFn(BarrierSendFn fn) { registerSocketFn(0, std::move(fn)); }

    // Per-socket registration: each socket gets its own barrierBit.
    void registerSocket(int socket, int barrierBit);
    void registerSocketFn(int socket, BarrierSendFn fn);

    int barrierArrive(ThreadContext *tc, uint32_t mask, uint32_t activeThreads);

    void releaseBarrier(uint32_t mask);
};

} // namespace gem5

#endif // __SIM_SYNC_WAIT_HH__
