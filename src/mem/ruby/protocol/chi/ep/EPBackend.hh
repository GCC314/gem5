#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "mem/ruby/common/DataBlock.hh"
#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"
#include "params/EPBackend.hh"
#include "sim/sim_object.hh"

namespace gem5
{

namespace ruby
{

class UBCCController;
class EPRNFController;
class RubySystem;

// ---- M5 Outer Protocol Types ----
// Outer protocol request types sent from EP_SNF (requester) to home UBCC.
enum class OuterReqType {
    GlobalReadShared,   // Requester needs shared read permission
    GlobalReadUnique    // Requester needs unique/exclusive permission
};

// Outer protocol grant types sent from home UBCC to requester EPBackend.
enum class OuterGrantType {
    GlobalGrantShared,     // Shared read granted (result code 0)
    GlobalGrantExclusive,  // Clean exclusive owner granted (result code 1)
    GlobalGrantModified    // Dirty modified owner granted (result code 2)
};

// ---- M7: Outer Writeback / Evict Message Types ----
// Writeback request sent from requester node to home UBCC.
struct OuterWritebackMsg {
    uint64_t linePa;         // Physical address (home node's view)
    int requesterNode;       // Node performing the writeback
    int homeNode;            // Home node for this line
    uint64_t epoch;          // Per-transaction epoch
    bool keepAsClean;        // True if owner retains clean exclusive after writeback

    OuterWritebackMsg() : linePa(0), requesterNode(-1), homeNode(-1),
                          epoch(0), keepAsClean(false) {}
};

// Evict request sent from requester node to home UBCC.
struct OuterEvictMsg {
    uint64_t linePa;         // Physical address (home node's view)
    int evictingNode;        // Node performing the eviction
    int homeNode;            // Home node for this line
    uint64_t epoch;          // Per-transaction epoch

    OuterEvictMsg() : linePa(0), evictingNode(-1), homeNode(-1),
                      epoch(0) {}
};

// Ack response from home UBCC after writeback/evict.
struct OuterAckMsg {
    uint64_t linePa;         // Physical address (home node's view)
    int homeNode;            // Home node that sent the ack
    uint64_t epoch;          // Per-transaction epoch
    bool success;            // True if operation succeeded

    OuterAckMsg() : linePa(0), homeNode(-1), epoch(0), success(false) {}
};

// ---- M8: Global Invalidation Message Types ----
// Invalidation request sent from home UBCC to a sharer node's EPBackend.
struct OuterInvalidateMsg {
    uint64_t linePa;           // Physical address (home node's view)
    uint64_t sharerLocalPa;    // PA in sharer node's local view
    int sharerNode;            // Node being invalidated
    int homeNode;              // Home node that sent the invalidation
    uint64_t epoch;            // Per-transaction epoch

    OuterInvalidateMsg() : linePa(0), sharerLocalPa(0),
        sharerNode(-1), homeNode(-1), epoch(0) {}
};

// Invalidation acknowledgment sent from sharer node back to home UBCC.
struct OuterInvalidationAck {
    uint64_t linePa;         // Physical address (home node's view)
    int ackNode;             // Node that completed invalidation
    int homeNode;            // Home node that initiated the invalidation
    uint64_t epoch;          // Per-transaction epoch
    bool success;            // True if invalidation succeeded

    OuterInvalidationAck() : linePa(0), ackNode(-1), homeNode(-1),
                              epoch(0), success(false) {}
};

// ---- M6: Outer Recall Message Types ----
// Recall request sent from home UBCC to the owner node's EPBackend.
struct OuterRecallMsg {
    uint64_t linePa;         // Physical address (home node's view)
    uint64_t ownerLocalPa;   // Physical address in owner node's local view
    int ownerNode;           // Node being recalled
    int homeNode;            // Node that initiated the recall
    uint64_t epoch;          // Per-transaction epoch
    bool isReadRequest;      // True if recall triggered by read (downgrade to shared)
    bool dataNeeded;         // True if dirty data must be returned

    OuterRecallMsg() : linePa(0), ownerLocalPa(0), ownerNode(-1), homeNode(-1),
                       epoch(0), isReadRequest(false), dataNeeded(false) {}
};

// Recall response sent from owner node's EPBackend back to home UBCC.
struct OuterRecallResponse {
    uint64_t linePa;         // Physical address (home node's view)
    int ownerNode;           // Node that was recalled
    int homeNode;            // Home node that initiated the recall
    uint64_t epoch;          // Per-transaction epoch
    bool dataReturned;       // True if dirty data was returned
    bool ackReceived;        // True if recall completed

    OuterRecallResponse() : linePa(0), ownerNode(-1), homeNode(-1),
                            epoch(0), dataReturned(false), ackReceived(false) {}
};

// ---- M5 Phase 2: Outer Message Envelope ----
// Structured wire format for outer protocol messages between EP_SNF
// (requester) and home UBCC.  Replaces Phase 1's ad-hoc parameter passing
// with a proper message envelope for audit, log, and future network
// migration.
struct OuterReqEnvelope {
    uint64_t linePa;         // Physical address (home node's view)
    OuterReqType reqType;    // GlobalReadShared or GlobalReadUnique
    bool writeIntent;        // True if requester has write intent
    int srcNode;             // Requester node ID
    uint64_t epoch;          // Per-transaction epoch

    OuterReqEnvelope() : linePa(0), reqType(OuterReqType::GlobalReadShared),
                         writeIntent(false), srcNode(-1), epoch(0) {}
};

struct OuterGrantEnvelope {
    uint64_t linePa;            // Physical address (home node's view)
    OuterGrantType grantType;   // GrantShared/Exclusive/Modified
    int homeNode;               // Home node ID
    uint64_t epoch;             // Per-transaction epoch
    Tick grantVisibleTick;      // Tick when grant decision was made
    Tick sentinelVisibleTick;   // Tick when sentinel was installed

    OuterGrantEnvelope() : linePa(0),
        grantType(OuterGrantType::GlobalGrantShared),
        homeNode(-1), epoch(0), grantVisibleTick(0), sentinelVisibleTick(0) {}
};

// Requester-side per-line bookkeeping state.
// Tracks the global permission held by this requester for a remote DSM line.
enum class RequesterLineState {
    R_I,            // No global permission
    R_WAIT_GRANT,   // Remote miss issued, waiting for home grant
    R_S,            // Shared read permission held
    R_E,            // Clean exclusive owner permission (GrantExclusive)
    R_M             // Dirty modified owner permission (GrantModified)
};

// Per-line requester bookkeeping entry.
struct RequesterLineEntry {
    uint64_t lineAddr;
    RequesterLineState state;
    OuterReqType pendingReq;
    uint64_t epoch;
    bool writeIntent;
    int homeNode;      // Home node for this remote line (-1 if local)
};

// ---- M5 Inspection API return types ----
struct RequesterLineSnapshot {
    bool valid;
    uint64_t lineAddr;
    int state;      // RequesterLineState cast to int
    int pendingReq; // OuterReqType cast to int
    bool writeIntent;
    uint64_t epoch;
    int homeNode;   // Home node for this remote line (-1 if none)
};

// Snapshot of the last UBCC sideband observed by EP_SNF on a recvRequestMsg.
// Used for Python test inspection of HN→EP_SNF sideband field values.
struct SidebandSnapshot {
    bool valid;         // true if a sideband has been observed
    uint64_t lineAddr;  // PA of the request that carried the sideband
    int neededPerm;     // 0=Shared, 1=Unique
    bool writeIntent;   // write intent flag
    int outerReqType;   // mapped outer request: 0=GlobalReadShared, 1=GlobalReadUnique
    int grantResult;    // grant result code (OuterGrantType cast to int, -1=none)
    int homeNode;       // home node determined from PA routing (-1=none)
};

class EPBackend : public SimObject
{
  public:
    PARAMS(EPBackend);
    EPBackend(const Params &p);
    ~EPBackend();

    void init() override;
    void wakeup();

    int nodeId() const { return _nodeId; }

    bool checkAddr(uint64_t pa) const;
    bool checkDsmAddr(uint64_t pa) const;
    const NodeAddressMap& addrMap() const { return _addrMap; }

    // ---- M5: Remote Miss Request Dispatch ----
    // Called by EP_SNF when it receives a ReadNoSnp for a remote DSM line.
    // Returns the home_node for this request (-1 if invalid).
    int handleRemoteMiss(uint64_t line_pa, int neededPerm, bool writeIntent,
                         int& outHomeNode);

    // Called after home UBCC makes a grant decision.
    // Returns the OuterGrantType that was granted.
    OuterGrantType handleGrant(uint64_t line_pa, OuterGrantType grant,
                                int homeNode);

    // ---- M6: Recall Management ----
    /**
     * Handle an incoming recall request from a home UBCC.
     * This is called on the owner node's EPBackend when the home
     * UBCC needs to recall the line.
     *
     * @param recallMsg  Recall message from home UBCC
     * @return          True if recall was accepted/processed
     */
    bool handleRecallRequest(const OuterRecallMsg &recallMsg);

    /**
     * Send a recall response back to the home UBCC.
     * Called after the owner node has completed the recall (data gathered,
     * permissions downgraded).
     *
     * @param response  Recall response to send to home UBCC
     * @return          True if response was routed successfully
     */
    bool sendRecallResponse(const OuterRecallResponse &response);

    /**
     * Get the count of recall requests received by this EPBackend.
     */
    uint64_t getRecallReceivedCount() const { return _recallReceivedCount; }
    void resetRecallReceivedCount() { _recallReceivedCount = 0; }

    /**
     * Get the count of recall responses sent by this EPBackend.
     */
    uint64_t getRecallResponseSentCount() const { return _recallResponseSentCount; }
    void resetRecallResponseSentCount() { _recallResponseSentCount = 0; }

    // ---- M5 Phase 2: Outer Message Envelope Accessors ----
    // Returns the last outer request envelope for test inspection.
    const OuterReqEnvelope& lastOuterReqEnvelope() const { return _lastReqEnv; }
    // Returns the last outer grant envelope for test inspection.
    const OuterGrantEnvelope& lastOuterGrantEnvelope() const { return _lastGrantEnv; }

    // ---- M6: Recall envelope accessors ----
    const OuterRecallMsg& lastRecallMsg() const { return _lastRecallMsg; }
    const OuterRecallResponse& lastRecallResponse() const { return _lastRecallResponse; }

    // ---- M7: Writeback / Evict ----
    /**
     * Handle a writeback from a dirty owner (requester→home).
     * Called by EPSNFController when HN sends a writeback.
     *
     * @param line_pa     Physical address (requester's view)
     * @param keepAsClean True if owner wants to keep clean exclusive copy
     * @return            True if writeback was accepted by home
     */
    bool handleWriteback(uint64_t line_pa, bool keepAsClean);

    /**
     * Handle a clean evict from a sharer or clean owner (requester→home).
     * Called by EPSNFController when HN sends an eviction.
     *
     * @param line_pa     Physical address (requester's view)
     * @return            True if evict was accepted by home
     */
    bool handleEvict(uint64_t line_pa);

    /**
     * Get writeback count for test observation.
     */
    uint64_t getWritebackCount() const { return _writebackCount; }
    void resetWritebackCount() { _writebackCount = 0; }

    /**
     * Get evict count for test observation.
     */
    uint64_t getEvictCount() const { return _evictCount; }
    void resetEvictCount() { _evictCount = 0; }

    /**
     * Get stale-epoch-rejected count for test observation.
     */
    uint64_t getStaleRejectedCount() const;
    void resetStaleRejectedCount();

    /**
     * Get owner-mismatch-rejected count for test observation (P0-1).
     * Writeback from a node that is not the current owner is rejected.
     */
    uint64_t getOwnerMismatchRejectedCount() const;
    void resetOwnerMismatchRejectedCount();

    // M7: Envelope accessors for test inspection
    const OuterWritebackMsg& lastWritebackMsg() const { return _lastWritebackMsg; }
    const OuterEvictMsg& lastEvictMsg() const { return _lastEvictMsg; }
    const OuterAckMsg& lastAckMsg() const { return _lastAckMsg; }

    // ---- M8: Global Invalidation Management ----
    /**
     * Handle an incoming invalidation request from a home UBCC.
     * Called on the sharer node's EPBackend when the home UBCC
     * needs to invalidate a shared line.
     *
     * @param invMsg  Invalidation message from home UBCC
     * @return        True if invalidation was accepted/processed
     */
    bool handleInvalidationRequest(const OuterInvalidateMsg &invMsg);

    /**
     * Send an invalidation acknowledgment back to the home UBCC.
     *
     * @param ack  Invalidation ack to send to home UBCC
     * @return     True if ack was routed successfully
     */
    bool sendInvalidationAck(const OuterInvalidationAck &ack);

    /**
     * Get the count of invalidation requests received by this EPBackend.
     */
    uint64_t getInvalidationReceivedCount() const { return _invalidationReceivedCount; }
    void resetInvalidationReceivedCount() { _invalidationReceivedCount = 0; }

    /**
     * Get the count of invalidation acks sent by this EPBackend.
     */
    uint64_t getInvalidationAckSentCount() const { return _invalidationAckSentCount; }
    void resetInvalidationAckSentCount() { _invalidationAckSentCount = 0; }

    // M8: Envelope accessors for invalidation
    const OuterInvalidateMsg& lastInvalidateMsg() const { return _lastInvalidateMsg; }
    const OuterInvalidationAck& lastInvalidationAck() const { return _lastInvalidationAck; }

    /**
     * Diagnose the expected grant for a given sideband combination
     * without actually issuing the request.  Used by ARM_SYNC tests
     * to verify protocol path setup before real execution.
     *
     * @param neededPerm  0=Shared, 1=Unique
     * @param writeIntent write intent flag
     * @return String describing the expected grant type ("Shared"/"Exclusive"/"Modified")
     */
    std::string diagnoseExpectedGrant(int neededPerm, bool writeIntent) const;

    // ---- Q1: Grant Data Accessor ----
    /**
     * Return the data block for the last grant.
     * Populated by handleRemoteMiss() after the home UBCC grant decision.
     * Used by EPSNFController to construct a real CompData response.
     *
     * @return Pointer to the grant data buffer (cache line size bytes),
     *         or nullptr if no grant has been processed.
     */
    const uint8_t* lastGrantData() const;

    /**
     * Return the size of the last grant data buffer in bytes.
     */
    int lastGrantDataSize() const;

    bool isDsmAddr(uint64_t pa) const;

    /** EP_RNF snoop counter for test verification */
    uint64_t getEpRnfSnoopCount() const;
    void resetEpRnfSnoopCount();
    void incrementEpRnfSnoopCount();

    /** Access to UBCCController for Python inspection */
    UBCCController* getUBCC() const { return _ubcc; }

    // ---- M5 Inspection API ----
    /** Inspect requester-side bookkeeping for a given line. */
    RequesterLineSnapshot inspectRequesterState(uint64_t line_pa) const;

    /**
     * Record the sideband values from a recvRequestMsg for later
     * inspection by Python test harness.
     * Called by EPSNFController::recvRequestMsg().
     */
    void recordSideband(uint64_t line_pa, int neededPerm, bool writeIntent,
                        int outerReqType, int grantResult, int homeNode);

    /**
     * Retrieve the last recorded sideband snapshot.
     * Returns a snapshot with valid=false if no sideband has been recorded.
     */
    SidebandSnapshot inspectLastSideband() const;

    /** Clear the last sideband snapshot (for test reset). */
    void clearSidebandSnapshot();

    // ---- M6: EP_RNF delayed response hook ----
    /**
     * Register the EPRNFController for delayed HN response support.
     */
    void setEpRnfController(EPRNFController *ctrl) { _epRnfCtrl = ctrl; }

    /**
     * Get the registered EPRNFController (may be nullptr).
     */
    EPRNFController* getEpRnfController() const { return _epRnfCtrl; }

    // ---- M6: Cross-Node EPBackend Routing Registry ----
    /**
     * Static registry of EPBackend instances keyed by node ID.
     * Used to route recall requests to the owner node's EPBackend.
     */
    static EPBackend* getBackendInstance(int node_id);

  private:
    const int _nodeId;
    NodeAddressMap _addrMap;
    UBCCController *_ubcc = nullptr;
    EPRNFController *_epRnfCtrl = nullptr;
    RubySystem *_ruby_system = nullptr;

    // ---- Q1: Grant Data Buffer ----
    // Cache-line-sized buffer populated after each grant decision.
    // Used by EPSNFController to construct CompData response payload.
    DataBlock _lastGrantDataBlock;
    bool _lastGrantDataValid = false;

    /**
     * Populate the grant data buffer by reading from the home node's
     * DL_SNF memory via functional access.
     *
     * @param homePa  Physical address in home node's PA view
     * @param homeNode Node ID of the home node
     */
    void populateGrantData(uint64_t homePa, int homeNode);

    // ---- M5: Requester-Side Bookkeeping ----
    // Per-line entries tracking global permissions for remote DSM lines.
    std::map<uint64_t, RequesterLineEntry> _requesterLines;
    uint64_t _epochCounter = 0;

    // ---- M5: Sideband Inspection ----
    // Last sideband values recorded by recvRequestMsg.
    // Only for test/observation hooks; not used in protocol decisions.
    SidebandSnapshot _lastSideband;

    // ---- M5 Phase 2: Outer Message Envelopes ----
    // Last outer request and grant envelopes for test inspection.
    OuterReqEnvelope _lastReqEnv;
    OuterGrantEnvelope _lastGrantEnv;

    // ---- M6: Recall message envelopes ----
    OuterRecallMsg _lastRecallMsg;
    OuterRecallResponse _lastRecallResponse;

    // ---- M6: Recall counters ----
    uint64_t _recallReceivedCount;
    uint64_t _recallResponseSentCount;

    // ---- M7: Writeback / Evict counters and envelopes ----
    uint64_t _writebackCount;
    uint64_t _evictCount;
    OuterWritebackMsg _lastWritebackMsg;
    OuterEvictMsg _lastEvictMsg;
    OuterAckMsg _lastAckMsg;

    // ---- M8: Invalidation counters and envelopes ----
    uint64_t _invalidationReceivedCount;
    uint64_t _invalidationAckSentCount;
    OuterInvalidateMsg _lastInvalidateMsg;
    OuterInvalidationAck _lastInvalidationAck;

    // ---- M6: Cross-Node EPBackend Routing Registry ----
    static std::map<int, EPBackend*> _backendInstances;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__
