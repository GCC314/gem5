#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__

#include <cstdint>
#include <cstring>
#include <deque>
#include <map>
#include <set>
#include <string>

#include "base/types.hh"
#include "mem/ruby/common/DataBlock.hh"
#include "mem/ruby/protocol/chi/ep/EPBackend.hh"  // F3: GrantDataSource
#include "mem/ruby/protocol/chi/ep/ResidentDir.hh"

namespace gem5
{

namespace ruby
{

class RubySystem;
class UBIOModule;
class EPBackend;

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

// §6.1: Upgrade cause enumeration
enum class UBCC_UpgradeCause {
    LocalCleanUnique,    // Local CleanUnique upgrade (sharer → exclusive)
    LocalStoreUpgrade    // Local store-triggered upgrade
};

using MESIState = UBCCMESIState;
using DirEntry = UBCCDirEntry;

// ---- Phase 1: Outstanding request state machine (v4 expanded) ----
// Per §4.1.5: normative state machine for all four operation types.
enum class OpType {
    RECALL,            // Recall owner data before granting access
    INVALIDATE,        // Invalidate sharers before upgrading to unique
    GRANT_HANDSHAKE,   // Grant commit pending Clear from requester
    UPGRADE_PENDING    // Local upgrade four-message handshake in progress
};

enum class OpStage {
    CREATED,               // Just created, no response yet
    WAITING_TARGET_RESP,   // RECALL: waiting for owner recall response
    WAITING_ALL_ACKS,      // INVALIDATE: waiting for all sharer acks
    WAITING_LOCAL_DONE,    // UPGRADE_PENDING: waiting for OuterUpgradeDone
    WAITING_CLEAR,         // GRANT_HANDSHAKE: waiting for matching Clear
    DONE,                  // Terminal: operation completed successfully
    CANCELLED,             // Terminal: rejected or validation failed
    TIMED_OUT,             // Terminal: retry budget exhausted
    PERSISTENT_BUSY        // Terminal: irrevocable-after-ack, only accept Done
};

// §7.2: OutstandingRequest with full v4 fields for all four op types.
struct OutstandingRequest {
    uint64_t linePa;           // Associated cache line address (home PA view)
    uint64_t baseEpoch;        // Requester-observed committed epoch (validation baseline)
    uint64_t reservedEpoch;    // Epoch to be committed on Clear or UpgradeDone
    uint64_t reqId;            // Requester-allocated ID, home echoes back
    OpType   opType;           // Type of operation
    OpStage  stage;            // Current stage in normative state machine
    int      requesterNode;    // Node waiting for completion
    int      homeNode;         // Home node for this line
    int      targetNode;       // Recall target / upgrade requester
    uint64_t targetMask;       // Invalidation target mask (sharers to invalidate)

    // Intended directory result (§4.1.3): reserved but NOT committed until Clear/UpgradeDone
    MESIState intendedState;
    uint64_t  intendedSharersMask;
    int       intendedOwnerNode;
    bool      intendedDirty;

    // Original request parameters
    UBCC_OuterReqType reqType;
    bool              writeIntent;

    // Recall / Invalidate barrier flags
    bool     recallBarrierDone;
    bool     invalidateBarrierDone;
    bool     replayArmed;        // True if this grant was created by replay (retry-hit allowed)

    // Timing
    Tick     createTick;
    Tick     respTick;
    Tick     deadlineTick;

    bool     accepted;           // True if upgrade ack was true

    // Recall data buffer (P0-3)
    uint8_t  dataBuf[64];
    bool     dataValid;

    // F3: Data source for the grant (HomeMemory / RecallBuffer / NoData)
    GrantDataSource dataSource;

    // Invalidation tracking
    int      pendingAckCount;
    uint64_t ackMask;
    uint64_t totalMask;

    // Upgrade context (§4.1.4)
    UBCC_UpgradeCause upgradeCause;

    // upgrade_invalidate_fix: UPGRADE_PENDING-specific fields
    uint64_t upgradeTargetMask;      // frozen sharers snapshot (without requester)
    int      upgradePendingAckCount; // remaining ack count before Ack(true)
    uint64_t upgradeAckMask;         // bitmask of received InvalidationAck
    bool     upgradeDoneArrived;     // TENTATIVE: Done arrived before acks complete
    uint64_t upgradeDoneEpoch;       // TENTATIVE: cached Done epoch
    uint64_t upgradeDoneReqId;       // TENTATIVE: cached Done reqId
    OpStage  upgradeSavedStage;      // saved stage when Done arrived early (TENTATIVE)

    OutstandingRequest()
        : linePa(0), baseEpoch(0), reservedEpoch(0), reqId(0),
          opType(OpType::GRANT_HANDSHAKE), stage(OpStage::CREATED),
          requesterNode(-1), homeNode(-1), targetNode(-1), targetMask(0),
          intendedState(MESIState::G_I), intendedSharersMask(0),
          intendedOwnerNode(-1), intendedDirty(false),
          reqType(UBCC_OuterReqType::GlobalReadShared),
          writeIntent(false),
          recallBarrierDone(false), invalidateBarrierDone(false),
          replayArmed(false),
          createTick(0), respTick(0), deadlineTick(0),
          accepted(false), dataValid(false),
          dataSource(GrantDataSource::HomeMemory),  // F3
          pendingAckCount(0), ackMask(0), totalMask(0),
          upgradeCause(UBCC_UpgradeCause::LocalCleanUnique),
          upgradeTargetMask(0), upgradePendingAckCount(0), upgradeAckMask(0),
          upgradeDoneArrived(false), upgradeDoneEpoch(0), upgradeDoneReqId(0),
          upgradeSavedStage(OpStage::CREATED)
    {
        memset(dataBuf, 0, 64);
    }
};

// §3.5 / §7.2: GrantHandshakeTombstone for duplicate Clear replay within window W.
// When GRANT_HANDSHAKE reaches DONE, it converts to this tombstone instead of
// being kept as a live outstanding.  Duplicate Clear within W returns the
// identical cached ClearAck.
struct GrantHandshakeTombstone {
    uint64_t linePa;
    uint64_t epoch;
    uint64_t reqId;
    OpType   opType;       // always GRANT_HANDSHAKE
    bool     accepted;
    Tick     expireTick;   // createTick + W

    GrantHandshakeTombstone()
        : linePa(0), epoch(0), reqId(0),
          opType(OpType::GRANT_HANDSHAKE),
          accepted(false), expireTick(0) {}
};

class UBCCController
{
  public:
    // §3.1: Pending requester atom — queued behind live outstanding.
    // Per recall_done_fix.md: RECALL.DONE is requester-private; foreign
    // requesters are queued here until the head requester's Clear commits.
    struct PendingRequester {
        int node;                 // Requester node ID
        UBCC_OuterReqType reqType; // RS or RU
        bool writeIntent;          // True for RU with write intent
        uint64_t epoch;            // Observed epoch at enqueue time
        uint64_t reqId;            // Requester-allocated ID, reused on replay

        PendingRequester()
            : node(-1), reqType(UBCC_OuterReqType::GlobalReadShared),
              writeIntent(false), epoch(0), reqId(0) {}
    };

    // Maximum pending requesters per PA (configurable queue depth)
    static constexpr size_t MAX_PENDING_PER_PA = 4;

    // v4-dual-socket: constructor now takes socket_id.
    UBCCController(int node_id, int socket_id = 0,
                   RubySystem *ruby_system = nullptr,
                   uint32_t epoch_bits = 64,
                   uint32_t resident_bf_bytes = ResidentDir::DefaultBloomBytes,
                   uint32_t resident_force_entries = 0);
    ~UBCCController();

    int nodeId() const { return _nodeId; }
    int socketId() const { return _socketId; }

    void wakeup();

    /** Set the local router for sending messages (e.g., UpgradeAckNotify). */
    void setRouter(UBIOModule *router) { _router = router; }
    void setBackend(EPBackend *backend) { _backend = backend; }

    // ---- v4-dual-socket: Query Line Metadata (read-only snapshot) ----
    /**
     * Query committed directory metadata for a line without creating
     * an outstanding request or modifying any state.
     * Used by EPBackend for writeback fallback when _requesterLines miss.
     *
     * @param linePa      Home PA
     * @param outEpoch    Output: committed epoch (0 if not found)
     * @param outOwnerNode Output: owner node (-1 if none)
     * @param outState    Output: committed MESI state
     * @param outFound    Output: true if entry exists
     */
    void queryLineMeta(uint64_t linePa,
                       uint64_t &outEpoch,
                       int &outOwnerNode,
                       MESIState &outState,
                       bool &outFound) const;

    // ---- v4-dual-socket: HomeWritebackNotify handler ----
    /**
     * Process a HomeWritebackNotify from HN-F via EPBackend.
     * Releases directory ownership after DDR4 writeback completes.
     * Implements optimistic stale drop: if epoch no longer matches,
     * the notification is silently dropped.
     *
     * @param homePa      Home PA that was written to DRAM
     * @param notifyEpoch Epoch from the notify message (for stale check)
     */
    void processHomeWritebackNotify(uint64_t homePa, uint64_t notifyEpoch);

    struct BackstoreEntry {
        MESIState state;
        uint64_t sharersMask;
        uint64_t epoch;
    };

    // ---- Cross-Node Routing Registry ----
    // v4-dual-socket: keyed by (node_id, socket_id) pair.
    static void registerInstance(int node_id, int socket_id, UBCCController *ubcc);
    static UBCCController* getInstance(int node_id, int socket_id = 0);

    // ---- M5: Home UBCC Grant Decision (v4: reserve-then-commit) ----
    /**
     * Process an outer protocol request from a requester node.
     * Per §4.1.3: creates OutstandingRequest with intended result,
     * BUT NEVER directly modifies committed DirEntry.
     * Commit only on matching Clear (§3.3, §3.5).
     *
     * @param line_pa             Physical address (home node's view)
     * @param reqType             GlobalReadShared or GlobalReadUnique
     * @param writeIntent         True if requester has write intent
     * @param requesterNode       Node ID of the requesting node
     * @param baseEpoch           Requester-observed committed epoch
     * @param reqId               Requester-allocated transaction ID
     * @param outGrantVisibleTick Output: tick when grant decision was made
     * @param outSentinelVisibleTick Output: tick when sentinel was installed
     * @param outRecallNeeded     Output (M6): set to true if recall is needed
     * @param outRecallOwnerNode  Output (M6): node ID of owner to recall (-1 if none)
     * @param outDataSource       Output (F3): data source for the grant
     * @return                    Grant type (GlobalGrantShared/Exclusive/Modified)
     *                            or -1 cast to enum if BUSY
     */
    UBCC_OuterGrantType processOuterRequest(
        uint64_t line_pa, UBCC_OuterReqType reqType, bool writeIntent,
        int requesterNode,
        uint64_t baseEpoch = 0, uint64_t reqId = 0,
        Tick *outGrantVisibleTick = nullptr,
        Tick *outSentinelVisibleTick = nullptr,
        bool *outRecallNeeded = nullptr,
        int *outRecallOwnerNode = nullptr,
        GrantDataSource *outDataSource = nullptr,
        uint64_t *outAuthEpoch = nullptr);

    // ---- v4: Local Upgrade Management (§4.1.4) ----
    /**
     * Process an OuterUpgradeReq from a sharer upgrading to unique.
     * Creates UPGRADE_PENDING outstanding, reserves epoch, sends OuterUpgradeAck.
     * Never modifies committed DirEntry.
     *
     * @param line_pa         Home PA
     * @param requesterNode   Node requesting the upgrade
     * @param epoch           Requester-observed committed epoch
     * @param reqId           Requester-allocated ID
     * @param desiredPerm     Desired permission (Shared=0, Unique=1)
     * @param cause           Upgrade cause
     * @return                true if accepted (Ack with accepted=true), false otherwise
     */
    bool processOuterUpgradeReq(
        uint64_t line_pa, int requesterNode,
        uint64_t epoch, uint64_t reqId,
        int desiredPerm, UBCC_UpgradeCause cause);

    /**
     * Process an OuterUpgradeDone from a requester that completed local upgrade.
     * Commits owner/state/epoch to DirEntry, retires UPGRADE_PENDING.
     * Per §4.1.4 step 5.
     *
     * @param line_pa         Home PA
     * @param requesterNode   Node that completed upgrade
     * @param epoch           reservedEpoch from the UpgradeAck
     * @param reqId           Original requester-allocated ID
     * @return                true if accepted
     */
    bool processOuterUpgradeDone(
        uint64_t line_pa, int requesterNode,
        uint64_t epoch, uint64_t reqId);

    // ---- v4: Clear / ClearAck (§3.5) ----
    /**
     * Process a Clear from the requester to commit a GRANT_HANDSHAKE.
     * If prerequisites are DONE, commits intended DirEntry and retires
     * the handshake to tombstone(W).
     *
     * @param line_pa         Home PA
     * @param srcNode         Requester node
     * @param epoch           Epoch from the grant
     * @param reqId           Requester-allocated ID
     * @return                true if Clear accepted and committed
     */
    bool processClear(
        uint64_t line_pa, int srcNode,
        uint64_t epoch, uint64_t reqId);

    bool copyOutstandingGrantData(uint64_t line_pa, DataBlock &outBlk) const;

    // ---- M6: Recall Management ----
    /**
     * Receive recall response from the owner node (data/ack).
     * Called by the home-side EPBackend when the owner's data arrives.
     *
     * @param line_pa           Physical address (home node's view)
     * @param ownerNode         Node that was recalled
     * @param dataReceived      True if dirty data was returned
     * @param responseEpoch     Epoch from the response message (M7: stale check)
     * @param reqId             Transaction ID
     * @param dataBlk           F2: actual data payload (64 bytes), nullptr if none
     * @return                  True if recall completed successfully
     */
    bool processRecallResponse(uint64_t line_pa, int ownerNode,
                               bool dataReceived, uint64_t responseEpoch,
                               uint64_t reqId = 0,
                               const DataBlock *dataBlk = nullptr);

    /**
     * Check if a line is currently busy (recall or other op in progress).
     */
    bool isLineBusy(uint64_t line_pa) const;

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
     * Notify UBCC that dirty data for a home PA has been written to DRAM
     * by the HN-F → EP-SNF path. Releases ownership by transitioning the
     * directory entry to G_I. No epoch/owner validation needed — the HN-F
     * has already committed the data to DRAM.
     *
     * @param homePa    Physical address (home node's view)
     */
    void notifyHomeWritebackComplete(uint64_t homePa);

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
     * Get the owner node for a line (-1 if no entry or no owner).
     */
    int getOwnerForLine(uint64_t line_pa) const;

    /**
     * Get the baseEpoch for an outstanding request (0 if not found).
     * upgrade_invalidate_fix D5: used by EPBackend Clear tuple fix.
     */
    uint64_t getOutstandingBaseEpoch(uint64_t line_pa) const;

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
                                uint64_t responseEpoch,
                                uint64_t reqId = 0);

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
     * upgrade_invalidate_fix: get the frozen target mask for an
     * UPGRADE_PENDING outstanding (0 if not found or not UPGRADE_PENDING).
     */
    uint64_t getUpgradePendingTargetMask(uint64_t line_pa) const;

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

    // ---- v4: Outstanding request API ----
    OutstandingRequest* findOutstanding(uint64_t linePa);
    OutstandingRequest* createOutstanding(uint64_t linePa, OpType opType,
                                          int requesterNode, int targetNode);
    void removeOutstanding(uint64_t linePa);

    void onBackstoreFillComplete(uint64_t linePa, bool found,
                                 const BackstoreEntry &entry);
    void onBackstoreWriteAck(uint64_t linePa);
    void onBackstoreDeleteAck(uint64_t linePa, bool existed);
    bool snapshotResidentForBackstore(uint64_t linePa, BackstoreEntry &entry) const;

    std::string inspectOffloadLineForTest(uint64_t linePa) const;
    bool debugSeedBackstoreForTest(uint64_t linePa, int mesi,
                                   uint64_t sharersMask, uint64_t epoch);
    bool debugSeedResidentForTest(uint64_t linePa, int mesi,
                                  uint64_t sharersMask, uint64_t epoch,
                                  bool residentDirty);
    bool debugForceResidentEvictForTest(uint64_t linePa);

    // ---- v4: Backstore Organization access ----
    ResidentDir& directory() { return _directory; }
    const ResidentDir& directory() const { return _directory; }

   private:
    const int _nodeId;
    int _socketId;                // v4-dual-socket

    /** Local UBIOModule *for sending messages (e.g., UpgradeAckNotify). */
    UBIOModule *_router = nullptr;
    EPBackend *_backend = nullptr;

    // Q3: Estimated UBCC-to-remote-UBCC interconnect latency (ticks).
    // Controls how long pendingOp=3 blocks before grant is released.
    // Default: 1000 ticks (1μs at 1GHz, approximating CXL.mem + NUMA).
    Tick _interconnectLatency;

    // ---- M5: Home directory ----
    // Per-line directory entries for lines homed at this node.
    ResidentDir _directory;

    // ---- v4: Outstanding request table ----
    // Per-line in-flight operations.
    std::map<uint64_t, OutstandingRequest> _outstandingReqs;

    // ---- v4: Grant handshake tombstone table (§3.5) ----
    // Completed GRANT_HANDSHAKE operations become tombstones for W ticks,
    // enabling idempotent duplicate Clear replay.
    // §7.4 / recall_done_fix.md: per-PA multi-entry deque so queued replay
    // doesn't clobber earlier tombstones within window W.
    std::map<uint64_t, std::deque<GrantHandshakeTombstone>> _tombstones;

    // ---- recall_done_fix.md: Pending requester queue per PA ----
    // Foreign requesters that arrive while a live outstanding exists for
    // the same PA are queued here.  Replayed on Clear commit.
    std::map<uint64_t, std::deque<PendingRequester>> _pendingRequesters;
    std::map<uint64_t, std::deque<PendingRequester>> _residentWaiters;
    std::set<uint64_t> _evictionPendingRemoval;

    // ---- v4: Tombstone window (configurable, default 100000 ticks) ----
    Tick _tombstoneWindowW = 100000;

    // ---- v4: Recall orphan timeout (configurable) ----
    Tick _recallTimeout = 1000000;

    // Configurable epoch width for wrap-around experiments.
    uint32_t _epochBits = 64;

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

    // EP_RNF snoop counter (local, test-only)
    uint64_t _epRnfSnoopCount = 0;

    // Precomputed DSM local base and segment size for isDsmAddr range check
    uint64_t _dsmLocalBase = 0;
    uint64_t _dsmSegSize = 0;

    // ---- Cross-Node Routing Registry ----
    // v4-dual-socket: keyed by (nodeId, socketId) pair.
    static std::map<std::pair<int,int>, UBCCController*> _instances;

    // ---- M5 private helpers ----
    void ensureDirEntry(uint64_t line_pa);
    enum class ResidentAccessResult {
        Ready,
        Queued,
        Busy,
    };
    ResidentAccessResult ensureResidentForAccess(
        uint64_t line_pa, UBCC_OuterReqType reqType, bool writeIntent,
        int requesterNode, uint64_t baseEpoch, uint64_t reqId, DirEntry &entry);
    ResidentAccessResult handleResidentMiss(
        uint64_t line_pa, UBCC_OuterReqType reqType, bool writeIntent,
        int requesterNode, uint64_t baseEpoch, uint64_t reqId, DirEntry &entry);
    void enqueueResidentWaiter(uint64_t linePa, const PendingRequester &pr);
    void replayResidentWaiters(uint64_t linePa);
    void refreshPinnedBit(uint64_t linePa);
    bool evictOneVictim(uint64_t avoidPa);
    void scheduleBackstoreWrite(uint64_t linePa);
    void scheduleBackstoreDelete(uint64_t linePa);
    const char* mesiStateName(MESIState s) const;

    // ---- M6 private helpers ----
    /**
     * Initiate a recall of the current owner.
     * Marks the line busy and records pending context in OutstandingRequest.
     */
    bool initiateRecall(uint64_t line_pa, DirEntry &entry,
                        UBCC_OuterReqType reqType, bool writeIntent,
                        int requesterNode);

    // ---- v4 private helpers ----
    /**
     * Half-range epoch comparison (§3.1.2).
     * Returns true if a is newer than b.
     */
    bool isNewerEpoch(uint64_t a, uint64_t b) const;

    /**
     * Normalize an epoch to the configured epoch width.
     */
    uint64_t normalizeEpoch(uint64_t epoch) const;

    /**
     * Bitmask for the configured epoch width.
     */
    uint64_t epochMask() const;

    /**
     * Allocate a new reserved epoch (increments committed epoch + 1).
     */
    uint64_t allocateReservedEpoch(DirEntry &entry);

    /**
     * Commit intended directory result from OutstandingRequest to DirEntry.
     * Only called from processClear() or processOuterUpgradeDone().
     */
    void commitIntendedResult(DirEntry &entry, const OutstandingRequest &ost);

    /**
     * Retire a GRANT_HANDSHAKE to tombstone(W) for duplicate Clear replay.
     */
    void retireToTombstone(const OutstandingRequest &ost, bool accepted);

    /**
     * Check tombstone for matching (pa, epoch, reqId) and return cached result.
     * Returns true if a matching tombstone was found (and not expired).
     */
    bool checkTombstone(uint64_t linePa, uint64_t epoch, uint64_t reqId,
                        bool &outAccepted);

    /**
     * Remove expired tombstones.
     */
    void cleanupTombstones();

    /**
     * Recall orphan cleanup — expire stale RECALL entries.
     */
    bool isExpiredRecall(const OutstandingRequest &ost) const;
    bool cleanupExpiredRecallIfNeeded(uint64_t linePa, bool replayWaiters);
    void cleanupExpiredRecalls();

    /**
     * Replay queued pending requesters after a Clear commit.
     * Called from processClear() after committing intended result.
     * Dequeues the head requester and calls processOuterRequest()
     * with rebased epoch against the NEW committed state.
     */
    void replayPendingRequesters(uint64_t linePa);

    /**
     * Allocate a monotonic reqId from the directory.
     */
    uint64_t allocateReqId(DirEntry &entry);

    // ---- UBInvariant: runtime invariant checker (debug-only) ----
    /**
     * Validate epoch monotonicity: assert newEpoch >= oldEpoch.
     * Called before every epoch write to DirEntry.
     * Fatal under --debug-flags=UBInvariant.
     */
    void validateEpochMonotonic(uint64_t oldEpoch, uint64_t newEpoch,
                                uint64_t pa) const;

    /**
     * Validate SharersMask canonical form after _directory.update().
     * Calls ResidentDir::validateCanonical explicitly.
     * Enabled under --debug-flags=UBInvariant; otherwise no-op.
     */
    void validateSharersCanonical(uint64_t pa) const;

    // Per-PA commit counter: warns on double-commit
    std::map<uint64_t, int> _commitCount;

    // Tombstone replay counter (warning-level)
    uint64_t _tombstoneReplayCount;

    // Generic invariant warning counter
    uint64_t _invariantWarnCount;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__
