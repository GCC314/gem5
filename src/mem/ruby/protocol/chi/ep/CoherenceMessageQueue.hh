#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBMSGQUEUE_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBMSGQUEUE_HH__

#include <cstdint>
#include <deque>
#include <functional>

#include "base/types.hh"
#include "mem/ruby/protocol/chi/ep/CoherenceMessage.hh"

namespace gem5
{
namespace ruby
{

/**
 * Per-(srcNode,dstNode) FIFO message queue with configurable latency.
 *
 * Every UBAdapter->UBIOModule *message passes through exactly one MsgQueue,
 * even when srcNode == dstNode (local home).  This guarantees that all
 * UBCC access is queued and never 0-tick short-circuited.
 *
 * In Phase 2 the queue operates in synchronous "immediate" mode where
 * processReady() drains everything without scheduling — callers are
 * still synchronous.  Latency scheduling will be enabled in later phases.
 */
class CoherenceMessageQueue
{
  public:
    struct Entry {
        CoherenceMessage msg;
        Tick readyTick;
    };

    CoherenceMessageQueue()
        : _latency(0), _seqCounter(0) {}

    /**
     * Enqueue a message with optional latency.
     * @param msg      The message to enqueue
     * @param now      Current tick
     * @param latency  Delivery delay (ticks); 0 = immediate
     * @returns        The enqueued entry's sequence number
     */
    uint64_t enqueue(const CoherenceMessage &msg, Tick now, Tick latency);

    /** Return true if at least one entry's readyTick <= now. */
    bool hasReady(Tick now) const;

    /**
     * Pop and return the ready message with the smallest
     * (readyTick, seqNum) tuple.
     */
    CoherenceMessage popReady(Tick now);

    /** Number of entries currently in the queue. */
    size_t size() const { return _fifo.size(); }

    /** Set the per-hop latency (default 0 in Phase 2). */
    void setLatency(Tick lat) { _latency = lat; }
    Tick latency() const { return _latency; }

    /**
     * Process all ready messages immediately (synchronous drain).
     * Calls handler for each ready message in FIFO order.
     * Used in Phase 2 for synchronous callers.
     */
    void processAll(Tick now, std::function<void(const CoherenceMessage &)> handler);

  private:
    std::deque<Entry> _fifo;
    Tick _latency;
    uint64_t _seqCounter;
};

// ---- Inline method definitions ----

inline uint64_t
CoherenceMessageQueue::enqueue(const CoherenceMessage &msg, Tick now, Tick latency)
{
    Entry entry;
    entry.msg = msg;
    entry.readyTick = now + latency;
    _fifo.push_back(entry);
    return ++_seqCounter;
}

inline bool
CoherenceMessageQueue::hasReady(Tick now) const
{
    for (auto &e : _fifo) {
        if (e.readyTick <= now)
            return true;
    }
    return false;
}

inline CoherenceMessage
CoherenceMessageQueue::popReady(Tick now)
{
    // FIFO order — pop first ready entry from front
    for (size_t i = 0; i < _fifo.size(); ++i) {
        if (_fifo[i].readyTick <= now) {
            CoherenceMessage result = _fifo[i].msg;
            _fifo.erase(_fifo.begin() + i);
            return result;
        }
    }
    return CoherenceMessage(); // should not reach here if hasReady() checked first
}

inline void
CoherenceMessageQueue::processAll(Tick now, std::function<void(const CoherenceMessage &)> handler)
{
    while (hasReady(now)) {
        CoherenceMessage msg = popReady(now);
        handler(msg);
    }
}

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBMSGQUEUE_HH__
