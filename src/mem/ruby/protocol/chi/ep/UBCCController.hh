#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__

#include <cstdint>
#include <map>
#include <queue>
#include <string>

namespace gem5
{

namespace ruby
{

class RubySystem;
class SentinelHelper;
struct DirEntrySnapshot;

class UBCCController
{
  public:
    UBCCController(int node_id, RubySystem *ruby_system = nullptr);
    ~UBCCController();

    int nodeId() const { return _nodeId; }

    void wakeup();

    // ---- M4 Sentinel Registration Test Hooks ----

    /**
     * Install EP_RNF as a sentinel in the HN-F directory.
     * S_SHARER if as_owner=false, S_OWNER if as_owner=true.
     * Must be called on a DSM address belonging to this home node.
     */
    bool installSentinelForTest(uint64_t line_pa, bool as_owner);

    /**
     * Remove EP_RNF sentinel from the HN-F directory.
     */
    bool removeSentinelForTest(uint64_t line_pa);

    /**
     * Inspect the HN-F directory entry for a given line.
     * Returns a JSON-like string representation for Python consumption.
     */
    std::string inspectDirEntryForTest(uint64_t line_pa);

    /**
     * Get the raw DirEntrySnapshot for programmatic inspection.
     */
    bool getDirEntrySnapshot(uint64_t line_pa, DirEntrySnapshot &snap);

    /**
     * Check whether a PA is a DSM home address for this node.
     */
    bool isDsmAddr(uint64_t pa) const;

    /**
     * EP_RNF snoop counter accessors.
     */
    uint64_t getEpRnfSnoopCount() const;
    void resetEpRnfSnoopCount();
    void incrementEpRnfSnoopCount();

    // Sentinel semantic state per line
    enum SentinelState {
        SS_NONE    = 0,
        SS_SHARER  = 1,
        SS_OWNER   = 2,
        SS_PENDING = 3
    };

  private:
    const int _nodeId;

    struct OuterEntry {
        uint64_t addr;
        uint64_t state;
        uint64_t tick;
    };
    std::map<uint64_t, OuterEntry> _metadata;

    struct OuterQueueEntry {
        uint64_t addr;
        uint64_t tick;
        int latency;
    };
    std::queue<OuterQueueEntry> _outerQueue;

    /**
     * TEST-ONLY, NOT authoritative: a cache of sentinel states for
     * Python test harness convenience.
     *
     * The authoritative sentinel state lives in the HN native
     * Cache_DirEntry (sharers/owner). This map is a convenience
     * mirror used only by installSentinelForTest/removeSentinelForTest
     * so the test harness can query state without reading the full
     * HN directory.
     *
     * NOT used by any production protocol decision path.
     * Rely on HN directory (inspectDirEntryForTest) for authoritative state.
     */
    std::map<uint64_t, SentinelState> _sentinelStates;

    // SentinelHelper for HN directory access (test-only)
    SentinelHelper *_sentinelHelper = nullptr;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__
