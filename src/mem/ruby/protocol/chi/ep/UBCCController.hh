#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__

#include <cstdint>
#include <map>
#include <queue>
#include <set>
#include <string>

namespace gem5
{

namespace ruby
{

class RubySystem;
class SentinelHelper;
struct DirEntrySnapshot;

// Forward declarations for M5 outer protocol types.
// These mirror the enums in EPBackend.hh but are used internally.
enum class UBCC_OuterReqType {
    GlobalReadShared,
    GlobalReadUnique
};

enum class UBCC_OuterGrantType {
    GlobalGrantShared,
    GlobalGrantExclusive,
    GlobalGrantModified
};

class UBCCController
{
  public:
    UBCCController(int node_id, RubySystem *ruby_system = nullptr);
    ~UBCCController();

    int nodeId() const { return _nodeId; }

    void wakeup();

    // ---- Cross-Node Routing Registry ----
    // In single-gem5 prototype, all UBCC instances register themselves
    // so that requester nodes can find the home node's UBCC.
    static void registerInstance(int node_id, UBCCController *ubcc);
    static UBCCController* getInstance(int node_id);

    // ---- M5: Home UBCC Grant Decision ----
    /**
     * Process an outer protocol request from a requester node.
     *
     * @param line_pa   Physical address (home node's view)
     * @param reqType   GlobalReadShared or GlobalReadUnique
     * @param writeIntent  True if requester has write intent (for E/M split)
     * @return          Grant type (GlobalGrantShared/Exclusive/Modified)
     */
    UBCC_OuterGrantType processOuterRequest(
        uint64_t line_pa, UBCC_OuterReqType reqType, bool writeIntent);

    /**
     * Inspect the home UBCC directory entry for a given line.
     * Returns a JSON-like string for Python test consumption.
     */
    std::string inspectUbccDirForTest(uint64_t line_pa);

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

    // ---- M5: Home MESI directory states ----
    enum class MESIState {
        G_I,  // Invalid: no sharer, no owner
        G_S,  // Shared: one or more sharers, no owner
        G_E,  // Exclusive: one clean exclusive owner
        G_M   // Modified: one dirty modified owner
    };

    // ---- M5: Home directory entry ----
    struct DirEntry {
        uint64_t lineAddr;
        MESIState state;
        // Mask of node IDs that hold shared copies (bit i = node i)
        uint32_t sharersMask;
        // Node ID of the exclusive/modified owner (-1 if none)
        int ownerNode;
        // True if the owner holds dirty (modified) data
        bool dirty;
        // Epoch for stale detection (incremented per transaction)
        uint64_t epoch;
        // Pending operation type (0=none)
        int pendingOp;

        DirEntry() : lineAddr(0), state(MESIState::G_I),
                     sharersMask(0), ownerNode(-1),
                     dirty(false), epoch(0), pendingOp(0) {}
    };

  private:
    const int _nodeId;

    // ---- M5: Home directory ----
    // Per-line directory entries for lines homed at this node.
    std::map<uint64_t, DirEntry> _directory;

    // ---- Legacy M4 structures (retained for compatibility) ----
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

    // ---- Cross-Node Routing Registry ----
    static std::map<int, UBCCController*> _instances;

    // ---- M5 private helpers ----
    void ensureDirEntry(uint64_t line_pa);
    const char* mesiStateName(MESIState s) const;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__
