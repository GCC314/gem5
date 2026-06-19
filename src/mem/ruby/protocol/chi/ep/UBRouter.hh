#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBROUTER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBROUTER_HH__

#include <cstdint>
#include <map>
#include <utility>

#include "mem/ruby/protocol/chi/ep/UBMsg.hh"
#include "mem/ruby/protocol/chi/ep/UBMsgQueue.hh"
#include "params/UBRouter.hh"
#include "sim/eventq.hh"
#include "sim/sim_object.hh"

namespace gem5
{
namespace ruby
{

class UBCCController;
class UBAdapter;

/**
 * Per-(node,socket) message router.
 *
 * Each (node,socket) pair has exactly one UBRouter.  It receives UBMsg
 * from the local UBAdapter, applies latency through per-pair MsgQueues,
 * and delivers the message to the destination UBCC or local UBAdapter.
 * v4-dual-socket: registry and queue keys expanded to include socketId.
 */
class UBRouter : public SimObject
{
  public:
    using RouterKey = std::pair<int,int>; // (nodeId, socketId)
    using QueueKey = std::pair<int,int>;  // (srcNode|srcSocket, dstNode|dstSocket)
                                          // packed as: key.first  = (srcNode<<16)|srcSocket
                                          //            key.second = (dstNode<<16)|dstSocket

    PARAMS(UBRouter);
    UBRouter(const Params &p);
    ~UBRouter();

    void init() override;

    int nodeId() const { return _nodeId; }
    int socketId() const { return _socketId; }

    /** Bind the local UBAdapter (for return-path delivery). */
    void setAdapter(UBAdapter *adapter) { _localAdapter = adapter; }

    /** Bind the local UBCCController (replaces direct static registry). */
    void bindUbcc(UBCCController *ubcc) { _localUbcc = ubcc; }

    /** Return the local UBCC (for synchronous Phase 2 callers). */
    UBCCController* localUbcc() const { return _localUbcc; }

    /** Return the local adapter. */
    UBAdapter* localAdapter() const { return _localAdapter; }

    /**
     * Main entry point: adapter → router.
     * Enqueues the message in the (srcNode,srcSocket,dstNode,dstSocket)
     * pair queue with configured latency, then drains ready messages
     * immediately for synchronous Phase 2 callers.
     */
    void sendMessage(const UBMsg &msg, Tick forcedLatency = -1);
    Tick crossNodeLatency() const { return _defaultLatency; }

    /** Deliver a message to the local UBCC (called by drain). */
    void deliverToUbcc(const UBMsg &msg, UBMsg &response);

    /** Deliver a message to the local adapter (called by drain). */
    void deliverToAdapter(const UBMsg &msg);

    /** Get or create the MsgQueue for a (srcNode,srcSocket,dstNode,dstSocket) tuple. */
    UBMsgQueue* getOrCreateQueue(int srcNode, int srcSocket,
                                  int dstNode, int dstSocket);

    /** Static router registry for cross-node, cross-socket routing. */
    static UBRouter* getRouter(int nodeId, int socketId);
    static void registerRouter(int nodeId, int socketId, UBRouter *router);

  private:
    int _nodeId;
    int _socketId;
    Tick _defaultLatency;

    UBAdapter *_localAdapter = nullptr;
    UBCCController *_localUbcc = nullptr;

    /**
     * Per-(srcNode,srcSocket,dstNode,dstSocket) FIFO queues.
     * Key packing: key.first  = (srcNode<<16) | srcSocket
     *              key.second = (dstNode<<16) | dstSocket
     */
    std::map<QueueKey, UBMsgQueue*> _pairQueues;

    /** Event for deferred queue drain. */
    EventFunctionWrapper _drainEvent;

    /** Drain all ready messages from all queues. */
    void drainReadyQueues();

    /** static registry keyed by (nodeId, socketId) */
    static std::map<RouterKey, UBRouter*> _routers;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBROUTER_HH__
