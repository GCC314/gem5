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
 * Per-node message router.
 *
 * Every node has exactly one UBRouter.  It receives UBMsg from the
 * local UBAdapter, applies latency through per-pair MsgQueues, and
 * delivers the message to the destination UBCC or local UBAdapter.
 */
class UBRouter : public SimObject
{
  public:
    PARAMS(UBRouter);
    UBRouter(const Params &p);
    ~UBRouter();

    void init() override;

    int nodeId() const { return _nodeId; }

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
     * Enqueues the message in the (src,dst) pair queue with configured
     * latency, then drains ready messages immediately for synchronous
     * Phase 2 callers.
     */
    void sendMessage(const UBMsg &msg);

    /** Deliver a message to the local UBCC (called by drain). */
    void deliverToUbcc(const UBMsg &msg, UBMsg &response);

    /** Deliver a message to the local adapter (called by drain). */
    void deliverToAdapter(const UBMsg &msg);

    /** Get or create the MsgQueue for a (src,dst) pair. */
    UBMsgQueue* getOrCreateQueue(int src, int dst);

    /** Static router registry for cross-node routing. */
    static UBRouter* getRouter(int nodeId);
    static void registerRouter(int nodeId, UBRouter *router);

  private:
    int _nodeId;
    Tick _defaultLatency;

    UBAdapter *_localAdapter = nullptr;
    UBCCController *_localUbcc = nullptr;

    /** Per-(src,dst) FIFO queues keyed by (srcNode, dstNode). */
    std::map<std::pair<int,int>, UBMsgQueue*> _pairQueues;

    /** Event for deferred queue drain. */
    EventFunctionWrapper _drainEvent;

    /** Drain all ready messages from all queues. */
    void drainReadyQueues();

    /** static registry */
    static std::map<int, UBRouter*> _routers;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBROUTER_HH__
