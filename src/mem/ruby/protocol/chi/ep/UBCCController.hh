#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__

#include <cstdint>
#include <cstring>
#include <map>
#include <queue>
#include <set>
#include <string>

#include "base/types.hh"

namespace gem5
{

namespace ruby
{

class RubySystem;

// Forward declarations for M5 outer protocol types.
// These mirror the enums in EPBackend.hh but are used internally.
enum class UBCC_OuterReqType {
    GlobalReadShared,
    GlobalReadUnique,
    GlobalWriteback,   // M7: dirty owner writeback
    GlobalEvict,       // M7: clean sharer/owner eviction
    GlobalInvalidate   // M8: invalidate sharers for exclusive upgrade
};

enum class UBCC_OuterGrantType {
    GlobalGrantShared,
    GlobalGrantExclusive,
    GlobalGrantModified
};

// ---- M6: Recall result codes ----
enum class UBCC_RecallResult {
    RecallInProgress,    // Recall has been initiated, caller must wait
    RecallCompleted,     // Recall response has been processed
    RecallRejected       // Line is busy, request rejected
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
     * @param line_pa             Physical address (home node's view)
     * @param reqType             GlobalReadShared or GlobalReadUnique
     * @param writeIntent         True if requester has write intent
     * @param requesterNode       Node ID of the requesting node
     * @param outGrantVisibleTick Output: tick when grant decision was made
     * @param outSentinelVisibleTick Output: tick when sentinel was installed
     * @param outRecallNeeded     Output (M6): set to true if recall is needed
     * @param outRecallOwnerNode  Output (M6): node ID of owner to recall (-1 if none)
     * @return                    Grant type (GlobalGrantShared/Exclusive/Modified)
     */
    UBCC_OuterGrantType processOuterRequest(
        uint64_t line_pa, UBCC_OuterReqType reqType, bool writeIntent,
        int requesterNode = -1,
        Tick *outGrantVisibleTick = nullptr,
        Tick *outSentinelVisibleTick = nullptr,
        bool *outRecallNeeded = nullptr,
        int *outRecallOwnerNode = nullptr);

    // ---- M6: Recall Management ----
    /**
     * Receive recall response from the owner node (data/ack).
     * Called by the home-side EPBackend when the owner's data arrives.
     *
     * @param line_pa           Physical address (home node's view)
     * @param ownerNode         Node that was recalled
     * @param dataReceived      True if dirty data was returned
     * @param responseEpoch     Epoch from the response message (M7: stale check)
     * @return                  True if recall completed successfully
     */
    bool processRecallResponse(uint64_t line_pa, int ownerNode,
                               bool dataReceived, uint64_t responseEpoch = 0);

    /**
     * Check if a line is currently busy (recall or other op in progress).
     */
    bool isLineBusy(uint64_t line_pa) const;

    // ---- Q3: Grant handshake completion callback ----
    /**
     * Called by the requester's EPBackend after the CHI CompData/CompAck
     * handshake completes.  This releases the grantInProgress flag so
     * subsequent requests for the same line can proceed.
     */
    void grantHandshakeComplete(uint64_t line_pa);

    // ---- M7: Writeback / Evict ----
    /**
     * Process a GlobalWriteback from a dirty owner.
     * The owner writes back dirty data and may keep or drop the line.
     *
     * @param line_pa        Physical address (home node's view)
     * @param requesterNode  Node performing the writeback
     * @param epochVal       Epoch from the writeback message (stale check)
     * @param keepAsClean    If true, owner retains clean exclusive (G_E);
     *                       if false, owner drops the line (G_I)
     * @return               True if writeback accepted (epoch matched)
     */
    bool processWriteback(uint64_t line_pa, int requesterNode,
                          uint64_t epochVal, bool keepAsClean);

    /**
     * Process a GlobalEvict from a clean sharer or clean owner.
     * Removes the node from the directory.
     *
     * @param line_pa        Physical address (home node's view)
     * @param evictingNode   Node performing the eviction
     * @param epochVal       Epoch from the evict message (stale check)
     * @return               True if evict accepted (epoch matched)
     */
    bool processEvict(uint64_t line_pa, int evictingNode,
                      uint64_t epochVal);

    /**
     * Check whether a response epoch is valid for the current line epoch.
     * Returns true if epoch matches, false if stale (must be dropped).
     */
    bool checkEpochForLine(uint64_t line_pa, uint64_t responseEpoch) const;

    /**
     * Get the current epoch for a line (-1 if line not found).
     */
    uint64_t getEpochForLine(uint64_t line_pa) const;

    /**
     * Get the writeback count (for test observation).
     */
    uint64_t getWritebackCount() const { return _writebackCount; }
    void resetWritebackCount() { _writebackCount = 0; }

    /**
     * Get the evict count (for test observation).
     */
    uint64_t getEvictCount() const { return _evictCount; }
    void resetEvictCount() { _evictCount = 0; }

    /**
     * Get the stale-epoch-rejected count (for test observation).
     */
    uint64_t getStaleEpochRejectedCount() const { return _staleRejectedCount; }
    void resetStaleEpochRejectedCount() { _staleRejectedCount = 0; }

    /**
     * Get the owner-mismatch-rejected count for writeback (for test observation).
     * P0-1: Writeback from a node that is not the current owner is rejected.
     */
    uint64_t getOwnerMismatchRejectedCount() const { return _ownerMismatchRejectedCount; }
    void resetOwnerMismatchRejectedCount() { _ownerMismatchRejectedCount = 0; }

    /**
     * Get the pending requester node for a busy line (-1 if not found).
     */
    int getPendingRequester(uint64_t line_pa) const;

    /**
     * Get the pending recall target node for a busy line (-1 if not found).
     */
    int getPendingRecallTarget(uint64_t line_pa) const;

    // ---- M8: Global Invalidation Management ----
    /**
     * Process an invalidation acknowledgment from a sharer node.
     * Called when a sharer completes its invalidation.
     *
     * @param line_pa        Physical address (home node's view)
     * @param ackNode        Node that has completed invalidation
     * @param responseEpoch  Epoch from the ack message (stale check)
     * @return               True if ack accepted and processed
     */
    bool processInvalidationAck(uint64_t line_pa, int ackNode,
                                uint64_t responseEpoch);

    /**
     * Get the pending invalidation count for a busy line.
     * Returns -1 if line not found or no pending invalidations.
     */
    int getPendingInvalidationCount(uint64_t line_pa) const;

    /**
     * Get the mask of nodes still waiting for invalidation ack.
     */
    uint64_t getPendingInvalidationMask(uint64_t line_pa) const;

    /**
     * Get the invalidation count (for test observation).
     */
    uint64_t getInvalidationCount() const { return _invalidationCount; }
    void resetInvalidationCount() { _invalidationCount = 0; }

    /**
     * Get the invalidation ack count (for test observation).
     */
    uint64_t getInvalidationAckCount() const { return _invalidationAckCount; }
    void resetInvalidationAckCount() { _invalidationAckCount = 0; }

    // ---- M6: Recall log/observability ----
    /**
     * Get the count of recall operations initiated by this home UBCC.
     */
    uint64_t getRecallCount() const { return _recallCount; }
    void resetRecallCount() { _recallCount = 0; }

    /**
     * Get the count of recall responses processed by this home UBCC.
     */
    uint64_t getRecallResponseCount() const { return _recallResponseCount; }
    void resetRecallResponseCount() { _recallResponseCount = 0; }

    // ---- M5: Home MESI directory states ----
    // Forward-declared here (before DirEntry and member functions that use it).
    enum class MESIState {
        G_I,  // Invalid: no sharer, no owner
        G_S,  // Shared: one or more sharers, no owner
        G_E,  // Exclusive: one clean exclusive owner
        G_M   // Modified: one dirty modified owner
    };

    /**
     * Inspect the home UBCC directory entry for a given line.
     * Returns a JSON-like string for Python test consumption.
     */
    std::string inspectUbccDirForTest(uint64_t line_pa);

    /**
     * Direct field access to UBCC directory entry for C++ self-test use.
     * Returns true if the entry exists, false otherwise.
     * Fills out-parameters with the current MESI state, ownerNode,
     * sharersMask, dirty flag, and busy flag.
     */
    bool getUbccDirFieldsForTest(uint64_t line_pa, MESIState &outState,
                                  int &outOwnerNode, uint64_t &outSharersMask,
                                  bool &outDirty) const;

    /**
     * Extended: also returns busy (pendingOp > 0) flag.
     */
    bool getUbccDirFieldsExtendedForTest(uint64_t line_pa, MESIState &outState,
                                          int &outOwnerNode,
                                          uint64_t &outSharersMask,
                                          bool &outDirty, bool &outBusy,
                                          int &outPendingRequester,
                                          int &outPendingRecallTarget) const;

    /**
     * Check whether a PA is a DSM home address for this node.
     * Pure computation using NodeAddressMap — no external dependency.
     */
    bool isDsmAddr(uint64_t pa) const;

    /**
     * EP_RNF snoop counter accessors (local, no SentinelHelper needed).
     */
    uint64_t getEpRnfSnoopCount() const { return _epRnfSnoopCount; }
    void resetEpRnfSnoopCount() { _epRnfSnoopCount = 0; }
    void incrementEpRnfSnoopCount() { _epRnfSnoopCount++; }

    // ---- M5/M6: Home directory entry ----
    struct DirEntry {
        uint64_t lineAddr;
        MESIState state;
        // Mask of node IDs that hold shared copies (bit i = node i)
        uint64_t sharersMask;
        // Node ID of the exclusive/modified owner (-1 if none)
        int ownerNode;
        // True if the owner holds dirty (modified) data
        bool dirty;
        // Epoch for stale detection (incremented per transaction)
        uint64_t epoch;
        // Pending operation type (0=none, 1=recall-in-progress)
        int pendingOp;
        // ---- Q3: Tick when grant was issued (for handshake delay) ----
        Tick grantTick;
        // ---- M6: Pending recall context ----
        // Node ID of the requester waiting for recall completion (-1 if none)
        int pendingRequester;
        // Node ID of the owner being recalled (-1 if none)
        int pendingRecallTarget;
        // Original request type that triggered the recall
        UBCC_OuterReqType pendingReqType;
        // Original write intent that triggered the recall
        bool pendingWriteIntent;

        // ---- M8: Sharer invalidation tracking ----
        int pendingInvalidationCount;
        uint64_t pendingInvalidationMask;
        uint64_t invalidatedAckMask;

        // ---- P0-3: Epoch-bound materialized data cache ----
        // When recall completes, the captured cache-line data is stored
        // here and bound to the current directory epoch.  On the next
        // write (epoch increment), the cache is invalidated.
        // This eliminates the phys_mem scavenge for cross-node data.
        uint8_t materializedData[64];
        bool materializedValid;
        uint64_t materializedEpoch;

        DirEntry() : lineAddr(0), state(MESIState::G_I),
                     sharersMask(0), ownerNode(-1),
                     dirty(false), epoch(0), pendingOp(0),
                     grantTick(0),
                     pendingRequester(-1), pendingRecallTarget(-1),
                     pendingReqType(UBCC_OuterReqType::GlobalReadShared),
                     pendingWriteIntent(false),
                     pendingInvalidationCount(0),
                     pendingInvalidationMask(0),
                     invalidatedAckMask(0),
                     materializedValid(false),
                     materializedEpoch(0)
        {
            memset(materializedData, 0, 64);
        }
    };

    // ---- P0-3: Materialized data access for grant path ----
    // Returns pointer to 64-byte cache-line data if available and
    // epoch-valid, or nullptr if data must be sourced elsewhere.
    const uint8_t* getMaterializedData(uint64_t linePa) const;
    bool hasMaterializedData(uint64_t linePa) const;
    // Write captured recall data into the directory for this line.
    void setMaterializedData(uint64_t linePa, const uint8_t* data,
                             int len, uint64_t epoch);

   private:
    const int _nodeId;

    // Q3: Estimated UBCC-to-remote-UBCC interconnect latency (ticks).
    // Controls how long pendingOp=3 blocks before grant is released.
    // Default: 1000 ticks (1μs at 1GHz, approximating CXL.mem + NUMA).
    Tick _interconnectLatency;

    // ---- M5: Home directory ----
    // Per-line directory entries for lines homed at this node.
    std::map<uint64_t, DirEntry> _directory;

    // ---- M6: Recall counters ----
    uint64_t _recallCount;
    uint64_t _recallResponseCount;

    // ---- M7: Writeback / Evict / Stale counters ----
    uint64_t _writebackCount;
    uint64_t _evictCount;
    uint64_t _staleRejectedCount;
    uint64_t _ownerMismatchRejectedCount;

    // ---- M8: Invalidation counters ----
    uint64_t _invalidationCount;
    uint64_t _invalidationAckCount;

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

    // EP_RNF snoop counter (local, test-only)
    uint64_t _epRnfSnoopCount = 0;

    // Precomputed DSM local base and segment size for isDsmAddr range check
    uint64_t _dsmLocalBase = 0;
    uint64_t _dsmSegSize = 0;

    // ---- Cross-Node Routing Registry ----
    static std::map<int, UBCCController*> _instances;

    // ---- M5 private helpers ----
    void ensureDirEntry(uint64_t line_pa);
    const char* mesiStateName(MESIState s) const;

    // ---- M6 private helpers ----
    /**
     * Initiate a recall of the current owner.
     * Marks the line busy (pendingOp=1) and records pending context.
     * The caller must complete the recall via processRecallResponse().
     */
    bool initiateRecall(uint64_t line_pa, DirEntry &entry,
                        UBCC_OuterReqType reqType, bool writeIntent,
                        int requesterNode);
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__
