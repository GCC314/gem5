#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBROUTER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBROUTER_HH__

#include <cstdint>
#include <deque>
#include <map>
#include <string>
#include <utility>
#include <vector>

#include "mem/ruby/protocol/chi/ep/CoherenceMessage.hh"
#include "mem/ruby/protocol/chi/ep/CoherenceMessageQueue.hh"
#include "params/UBIOModule.hh"
#include "sim/eventq.hh"
#include "sim/sim_object.hh"

namespace gem5
{
namespace ruby
{

class UBCCController;
class UBAdapter;

// ── Debug Fault Injection (debug-only, compile-time guarded) ──

/** Action taken when a fault rule matches. */
enum class DebugFaultAction : uint8_t {
    Delay,      // Hold the message for N ticks before enqueue
    Drop,       // Silently discard the message
    Duplicate,  // Enqueue the message twice (original + copy)
};

/** Debug-only fault injection rule for transport-layer testing.
 *  Matches against message type, source/dest nodes, or PA.
 */
struct DebugFaultRule {
    std::string     name;          // Human-readable label for logging
    CoherenceMessageType       matchType;     // CoherenceMessageType to match, or ReadReq as wildcard
    int             matchSrcNode;  // -1 = any
    int             matchDstNode;  // -1 = any
    uint64_t        matchLinePa;   // 0 = any
    DebugFaultAction action;       // What to do on match
    Tick            delayTicks;    // Used only for Delay action
    int             matchCount;    // How many times to fire (0 = infinite)
    int             firedCount;    // Internal: times already fired

    DebugFaultRule()
        : name(""), matchType(CoherenceMessageType::ReadReq),
          matchSrcNode(-1), matchDstNode(-1), matchLinePa(0),
          action(DebugFaultAction::Drop), delayTicks(0),
          matchCount(0), firedCount(0) {}
};

/**
 * Per-(node,socket) message router.
 *
 * Each (node,socket) pair has exactly one UBIOModule *.  It receives UBMsg
 * from the local UBAdapter, applies latency through per-pair MsgQueues,
 * and delivers the message to the destination UBCC or local UBAdapter.
 * v4-dual-socket: registry and queue keys expanded to include socketId.
 */
class UBIOModule : public SimObject
{
  public:
    using RouterKey = std::pair<int,int>; // (nodeId, socketId)
    using QueueKey = std::pair<int,int>;  // (srcNode|srcSocket, dstNode|dstSocket)
                                          // packed as: key.first  = (srcNode<<16)|srcSocket
                                          //            key.second = (dstNode<<16)|dstSocket

    PARAMS(UBIOModule);
    UBIOModule(const Params &p);
    ~UBIOModule();

    void init() override;

    /** Parse fault rule strings from Params and populate internal rule table. */
    void parseFaultRules(const std::vector<std::string> &rules);

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
    void sendMessage(const CoherenceMessage &msg, Tick forcedLatency = -1);
    Tick crossNodeLatency() const { return _defaultLatency; }

    /** Deliver a message to the local UBCC (called by drain). */
    void deliverToUbcc(const CoherenceMessage &msg, CoherenceMessage &response);

    /** Deliver a message to the local adapter (called by drain). */
    void deliverToAdapter(const CoherenceMessage &msg);

    /** Get or create the MsgQueue for a (srcNode,srcSocket,dstNode,dstSocket) tuple. */
    CoherenceMessageQueue* getOrCreateQueue(int srcNode, int srcSocket,
                                  int dstNode, int dstSocket);

    /** Static router registry for cross-node, cross-socket routing. */
    static UBIOModule * getRouter(int nodeId, int socketId);
    static void registerRouter(int nodeId, int socketId, UBIOModule *router);

    // ── Debug Fault Injection API (debug-only) ──
    /** Add a fault rule to this router's rule table. */
    void addFaultRule(const DebugFaultRule &rule);

    /** Clear all fault rules. */
    void clearFaultRules();

    /** Get the current number of fault rules. */
    size_t faultRuleCount() const { return _faultRules.size(); }

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
    std::map<QueueKey, CoherenceMessageQueue*> _pairQueues;

    /** Event for deferred queue drain. */
    EventFunctionWrapper _drainEvent;

    /** Drain all ready messages from all queues. */
    void drainReadyQueues();

    // ── Debug Fault Injection internals ──
    /** Apply fault rules to a message before enqueue.
     *  Returns the number of copies to enqueue (0 = dropped, 1 = normal, 2 = dup). */
    int applyFaultRules(const CoherenceMessage &msg);
    /** Deferred enqueue event for Delay action. */
    void delayedEnqueue(CoherenceMessage msg, CoherenceMessageQueue *q, Tick lat);

    /** Fault rule table (debug-only). */
    std::vector<DebugFaultRule> _faultRules;

    /** static registry keyed by (nodeId, socketId) */
    static std::map<RouterKey, UBIOModule *> _routers;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBROUTER_HH__
