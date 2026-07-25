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
    using BarrierSendFn = std::function<void(uint32_t mask, uint32_t srcBit,
                                            uint32_t seq)>;

  private:
    struct BarrierState
    {
        uint32_t activeThreads = 0;
        std::set<ThreadContext *> waiting;
        bool crossNode = false;
        bool remoteReleased = false;
        uint32_t generation = 0;  // TC90 fix: distinguishes successive barriers
                                   // sharing the same mask
        // Independent gem5 processes can observe a release before their local
        // thread reaches that generation. Keep only bounded lookahead state.
        std::set<uint32_t> earlyReleases;

        // Local-node expected thread count for this barrier (= active_threads
        // arg from sync_wait). In cross-node mode BarrierReached must be sent
        // exactly ONCE per (node,socket) plane — only after all localExpected
        // threads have arrived locally. Sending per-thread (the old behavior)
        // let a node's FIRST arriving thread satisfy the ubio per-node
        // aggregation, releasing the barrier before the node's other threads
        // arrived, desynchronizing `generation` across barriers and
        // deadlocking later barriers (TC80/82/91/98).
        uint32_t localExpected = 0;

        // True once this node/socket plane has fired BarrierReached for the
        // current generation, so later local threads don't re-send.
        bool reachedSent = false;
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

    // seq: the barrier generation carried by the incoming BarrierRelease.
    // A release is honored only if it matches the current generation, so a
    // stale release for an already-completed generation cannot spuriously
    // release the next barrier that reuses the same mask.
    void releaseBarrier(uint32_t mask, uint32_t seq);
};

} // namespace gem5

#endif // __SIM_SYNC_WAIT_HH__
