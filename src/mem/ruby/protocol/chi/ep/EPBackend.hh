#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__

#include <cstdint>
#include <array>
#include <map>
#include <string>
#include <tuple>
#include <vector>

namespace gem5 { namespace ruby { class EPRNFController; } }
#include "mem/ruby/common/DataBlock.hh"
#include "mem/ruby/protocol/chi/ep/CoherenceMessage.hh"
#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"
#include "params/EPBackend.hh"
#include "sim/sim_object.hh"

namespace gem5
{

namespace ruby
{

class EPRNFController;
class EPSNFController;
class UBAdapter;
class RubySystem;
class MetaRNFController;
class CacheMemory;
struct UBMsg;       // v4-dual-socket: forward decl for handleQueryLineMetaResp

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
    uint64_t reqId;          // v4: transaction ID (from outer layer)
    bool keepAsClean;        // True if owner retains clean exclusive after writeback

    OuterWritebackMsg() : linePa(0), requesterNode(-1), homeNode(-1),
                          epoch(0), reqId(0), keepAsClean(false) {}
};

// Evict request sent from requester node to home UBCC.
struct OuterEvictMsg {
    uint64_t linePa;         // Physical address (home node's view)
    int evictingNode;        // Node performing the eviction
    int homeNode;            // Home node for this line
    uint64_t epoch;          // Per-transaction epoch
    uint64_t reqId;          // v4: transaction ID

    OuterEvictMsg() : linePa(0), evictingNode(-1), homeNode(-1),
                      epoch(0), reqId(0) {}
};

// Ack response from home UBCC after writeback/evict.
struct OuterAckMsg {
    uint64_t linePa;         // Physical address (home node's view)
    int homeNode;            // Home node that sent the ack
    uint64_t epoch;          // Per-transaction epoch
    uint64_t reqId;          // v4: transaction ID
    bool success;            // True if operation succeeded

    OuterAckMsg() : linePa(0), homeNode(-1), epoch(0), reqId(0), success(false) {}
};

// ---- M8: Global Invalidation Message Types ----
// Invalidation request sent from home UBCC to a sharer node's EPBackend.
struct OuterInvalidateMsg {
    uint64_t linePa;           // Physical address (home node's view)
    uint64_t sharerLocalPa;    // PA in sharer node's local view
    int sharerNode;            // Node being invalidated
    int homeNode;              // Home node that sent the invalidation
    int sourceSocket;          // Local socket that received InvalidateReq
    uint64_t epoch;            // Per-transaction epoch
    uint64_t reqId;            // v4: transaction ID

    OuterInvalidateMsg() : linePa(0), sharerLocalPa(0),
        sharerNode(-1), homeNode(-1), sourceSocket(0), epoch(0), reqId(0) {}
};

// Invalidation acknowledgment sent from sharer node back to home UBCC.
struct OuterInvalidationAck {
    uint64_t linePa;         // Physical address (home node's view)
    int ackNode;             // Node that completed invalidation
    int homeNode;            // Home node that initiated the invalidation
    int sourceSocket;        // Local socket that must source InvalidateAck
    uint64_t epoch;          // Per-transaction epoch
    uint64_t reqId;          // v4: transaction ID

    OuterInvalidationAck() : linePa(0), ackNode(-1), homeNode(-1),
                              sourceSocket(0), epoch(0), reqId(0) {}
};

// ---- M6: Outer Recall Message Types ----
// Recall request sent from home UBCC to the owner node's EPBackend.
struct OuterRecallMsg {
    uint64_t linePa;         // Physical address (home node's view)
    uint64_t ownerLocalPa;   // Physical address in owner node's local view
    int ownerNode;           // Node being recalled
    int homeNode;            // Node that initiated the recall
    uint64_t epoch;          // Per-transaction epoch
    uint64_t reqId;          // v4: transaction ID
    bool isReadRequest;      // True if recall triggered by read (downgrade to shared)
    bool dataNeeded;         // True if dirty data must be returned
    int requesterNode;       // C4: direct-forward target node (data goes here)
    int requesterSocket;     // C4: direct-forward target socket
    int sourceSocket;        // Local socket that received RecallReq

    OuterRecallMsg() : linePa(0), ownerLocalPa(0), ownerNode(-1), homeNode(-1),
                       epoch(0), reqId(0), isReadRequest(false), dataNeeded(false),
                       requesterNode(-1), requesterSocket(-1), sourceSocket(0) {}
};

// Recall response sent from owner node's EPBackend back to home UBCC.
struct OuterRecallResponse {
    uint64_t linePa;         // Physical address (home node's view)
    int ownerNode;           // Node that was recalled
    int homeNode;            // Home node that initiated the recall
    uint64_t epoch;          // Per-transaction epoch
    uint64_t reqId;          // v4: transaction ID
    bool dataReturned;       // True if dirty data was returned
    bool ackReceived;        // True if recall completed
    DataBlock dataPayload;   // F2: actual 64-byte cache line data from owner
    bool hasDataPayload;     // F2: true if dataPayload is valid
    bool dataForwarded;      // C4: true if data was sent directly to requester
    int  dataForwardedTo;    // C4: requester node that received direct data
    int sourceSocket;        // Local socket that must source RecallResp

    OuterRecallResponse() : linePa(0), ownerNode(-1), homeNode(-1),
                            epoch(0), reqId(0), dataReturned(false), ackReceived(false),
                            dataPayload(64), hasDataPayload(false),
                            dataForwarded(false), dataForwardedTo(-1), sourceSocket(0) {}
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
    uint64_t reqId;          // v4: requester-allocated transaction ID

    OuterReqEnvelope() : linePa(0), reqType(OuterReqType::GlobalReadShared),
                         writeIntent(false), srcNode(-1), epoch(0), reqId(0) {}
};

struct OuterGrantEnvelope {
    uint64_t linePa;            // Physical address (home node's view)
    OuterGrantType grantType;   // GrantShared/Exclusive/Modified
    int homeNode;               // Home node ID
    uint64_t epoch;             // Per-transaction epoch
    uint64_t reqId;             // v4: transaction ID
    Tick grantVisibleTick;      // Tick when grant decision was made
    Tick sentinelVisibleTick;   // Tick when sentinel was installed

    OuterGrantEnvelope() : linePa(0),
        grantType(OuterGrantType::GlobalGrantShared),
        homeNode(-1), epoch(0), reqId(0),
        grantVisibleTick(0), sentinelVisibleTick(0) {}
};

// ---- v4: Clear / ClearAck (§3.5, §6.1) ----
enum class ClearReason {
    GrantHandshake    // Commit GRANT_HANDSHAKE intended result
};

struct OuterClearMsg {
    uint64_t linePa;
    int srcNode;
    int homeNode;
    uint64_t epoch;
    uint64_t reqId;
    ClearReason reason;

    OuterClearMsg() : linePa(0), srcNode(-1), homeNode(-1),
                      epoch(0), reqId(0), reason(ClearReason::GrantHandshake) {}
};

struct OuterClearAckMsg {
    uint64_t linePa;
    int homeNode;
    int dstNode;
    uint64_t epoch;
    uint64_t reqId;
    bool accepted;

    OuterClearAckMsg() : linePa(0), homeNode(-1), dstNode(-1),
                         epoch(0), reqId(0), accepted(false) {}
};

// ---- v4: Local Upgrade Messages (§4.1.4, §6.1) ----
enum class UpgradeCause {
    LocalCleanUnique,
    LocalStoreUpgrade
};

struct OuterUpgradeReq {
    uint64_t linePa;
    int srcNode;
    uint64_t epoch;       // requester-observed committed epoch
    uint64_t reqId;       // requester-allocated ID
    int desiredPerm;      // 0=Shared, 1=Unique
    UpgradeCause cause;

    OuterUpgradeReq() : linePa(0), srcNode(-1), epoch(0), reqId(0),
                        desiredPerm(0), cause(UpgradeCause::LocalCleanUnique) {}
};

struct OuterUpgradeAck {
    uint64_t linePa;
    int homeNode;
    int dstNode;
    uint64_t epoch;       // reservedEpoch if accepted
    uint64_t reqId;
    bool accepted;

    OuterUpgradeAck() : linePa(0), homeNode(-1), dstNode(-1),
                        epoch(0), reqId(0), accepted(false) {}
};

struct OuterUpgradeDone {
    uint64_t linePa;
    int srcNode;
    int homeNode;
    uint64_t epoch;
    uint64_t reqId;

    OuterUpgradeDone() : linePa(0), srcNode(-1), homeNode(-1),
                         epoch(0), reqId(0) {}
};

struct OuterUpgradeDoneAck {
    uint64_t linePa;
    int homeNode;
    int dstNode;
    uint64_t epoch;
    uint64_t reqId;
    bool accepted;

    OuterUpgradeDoneAck() : linePa(0), homeNode(-1), dstNode(-1),
                            epoch(0), reqId(0), accepted(false) {}
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
    uint64_t reqId;        // v4: outer transaction ID
    bool writeIntent;
    int homeNode;      // Home node for this remote line (-1 if local)
    Tick outerStartTick = 0; // First issue tick for protocol latency evidence
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

// GrantDataSource is defined in modules/ubiomodule/CoherenceMessage.hh
// (included via ep/CoherenceMessage.hh forward shim).
// OuterReqType, OuterGrantType, UpgradeCause are defined locally below.

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

    // SimObject-param getters (replaces env-var reads)
    bool silentUpgradeEnabled() const { return params().silent_upgrade; }
    bool directFwdEnabled() const { return params().direct_fwd; }
    bool haEndpointEnabled() const { return _haEndpointEnabled; }
    bool losslessOneWayClearEnabled() const {
        return _losslessOneWayClearEnabled;
    }

    bool checkAddr(uint64_t pa) const;
    bool checkDsmAddr(uint64_t pa) const;
    bool isDsmAddrCrossNode(uint64_t pa) const;
    int homeNodeCrossNode(uint64_t pa) const;
    const NodeAddressMap& addrMap() const { return _addrMap; }

    // ---- M5: Remote Miss Request Dispatch ----
    // Called by EP_SNF when it receives a ReadNoSnp for a remote DSM line.
    // Returns the home_node for this request (-1 if invalid).
    int handleRemoteMiss(uint64_t line_pa, int neededPerm, bool writeIntent,
                         int& outHomeNode);

    // v4-dual-socket: overload with ingressSocket from EP-SNF sideband.
    int handleRemoteMiss(uint64_t line_pa, int neededPerm, bool writeIntent,
                         int ingressSocket, int& outHomeNode);

    // ReadNoSnp from HN-F proves the local cache hierarchy has missed, so any
    // older R_S/R_E/R_M bookkeeping cannot suppress the required outer fetch.
    int handleRemoteDemandMiss(uint64_t line_pa, int neededPerm,
                               bool writeIntent, int ingressSocket,
                               int& outHomeNode);

    /**
     * Complete the HA permission install handshake for an EP-SNF fill.
     * EPSNF calls this only after the final reliable CompData beat has been
     * accepted by its outgoing MessageBuffer.  Receiving HAPermissionResp is
     * deliberately not sufficient to acknowledge installation.
     */
    void completeHARemoteGrant(uint64_t linePa, int neededPerm,
                               bool writeIntent, int ingressSocket);

    // Called after home UBCC makes a grant decision.
    // Returns the OuterGrantType that was granted.
    OuterGrantType handleGrant(uint64_t line_pa, OuterGrantType grant,
                                int homeNode);

    // ---- v4: Local Upgrade Management (§4.1.4, §4.2.3) ----
    /**
     * Initiate a local write upgrade for a remote sharer.
     * Sends OuterUpgradeReq to home UBCC, waits for OuterUpgradeAck.
     * Only after Ack(true) can EP-RNF reply SnpResp_I to local HN-F.
     *
     * @param line_pa   Local PA of the line being upgraded
     * @param homeNode  Home node for this line
     * @param desiredPerm Desired permission (0=Shared, 1=Unique)
     * @param cause     Upgrade cause
     * @param outEpoch  Output: reserved epoch from UpgradeAck
     * @param outReqId  Output: reqId for this upgrade
     * @param outRejected Output (optional): set true when the home explicitly
     *                  REJECTED the upgrade (e.g. another upgrade is already
     *                  outstanding for this line). Distinguishes a hard reject
     *                  (return false, *outRejected=true) from an async-pending
     *                  not-yet-answered upgrade (return false,
     *                  *outRejected=false). A rejected upgrade must NOT keep
     *                  holding the snoop — the snoop has to fall back to a
     *                  plain SnpResp_I so the line can be invalidated.
     * @return          True if UpgradeAck(true) received
     */
    // outRejected: set true on any reject. outNotSharer: set true only for a
    // PERMANENT reject (requester lost a dual-upgrade race and is no longer a
    // committed sharer) — the caller must abandon + ReadUnique rather than
    // retry the upgrade. A temporary reject (outNotSharer stays false) should
    // be retried once the home drains.
    bool notifyLocalWriteUpgrade(uint64_t line_pa, int homeNode,
                                  int sourceSocket,
                                  int desiredPerm, UpgradeCause cause,
                                     uint64_t &outEpoch, uint64_t &outReqId,
                                     bool *outRejected = nullptr,
                                     bool *outNotSharer = nullptr,
                                     bool *outDeferred = nullptr,
                                     bool forceResend = false);

    /**
     * Send OuterUpgradeDone after local upgrade completes.
     *
     * @param line_pa   Local PA
     * @param homeNode  Home node
     * @param epoch     reservedEpoch from UpgradeAck
     * @param reqId     Original upgrade reqId
     * @return          True if accepted by home
     */
    bool sendUpgradeDone(uint64_t line_pa, int homeNode, int sourceSocket,
                         uint64_t epoch, uint64_t reqId);

    // ---- v4: Clear / ClearAck (§3.5, §4.2.3) ----
    /**
     * Send a Clear to the home UBCC to commit a GRANT_HANDSHAKE.
     *
     * @param line_pa   Home PA
     * @param homeNode  Home node
     * @param epoch     Epoch from the grant
     * @param reqId     Transaction reqId
     * @return          True if Clear accepted (ClearAck.accepted==true)
     */
    int sendClear(uint64_t line_pa, int homeNode,
                  uint64_t epoch, uint64_t reqId, int sourceAdapter);

    /**
     * Publication hook for legacy UBCC grants. The Sequencer invokes this from
     * the existing HA SLICC Final action, after cache/data and directory
     * publication and immediately before TBE retirement. In lossless-oneway
     * mode it queues the matching Clear without waiting for ClearResp.
     */
    void notifyLocalLinePublished(uint64_t localLinePa, int sourceSocket);
    void notifyOneWayClearHandedOff(
        uint64_t homePa, int sourceSocket, uint64_t reqId);
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
     * Check if there is an active recall (between handleRecallRequest and
     * sendRecallResponse) for a given physical address.
     *
     * Used by EPRNFController to distinguish RECALL-induced SnpCleanInvalid
     * from genuine local upgrade SnpCleanInvalid (§§4.3.3, 5.5).
     *
     * @param pa  Physical address (local node view)
     * @return    True if a recall is active for this PA
     */
     bool hasActiveRecall(uint64_t pa) const;

    /**
     * Check if the local requester-side state for this PA is R_E or R_M
     * (Clean or dirty exclusive owner). Used for silent upgrade optimization.
     */
    bool hasRequesterExclusive(uint64_t pa) const;

    /**
     * Clear active recall tracking for a PA after the associated
     * SnpCleanInvalid has been handled (RECALL-SNOOP path).
     */
    void clearActiveRecall(uint64_t pa);

    /**
     * F2: Set recall capture data from EPRNFController before callback fires.
     * Called by EPRNFController::finishChiTxn() to transfer data from
     * PendingChiTxn.recallDataBlk to EPBackend.
     */
    void setRecallCaptureData(const DataBlock &data, bool valid) {
        _recallCaptureDataBlock = data;
        _recallCaptureDataValid = valid;
    }

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
     * @param dirtyData   64-byte dirty cacheline data (nullptr if unavailable)
     * @return            True if writeback was accepted by home
     */
    /**
     * Handle a writeback from a dirty owner (requester→home).
     * Called by EPSNFController when HN sends a writeback.
     *
     * Phase 2 async: populates *outQueryReqId with a stable QLM reqId
     * when the internal QueryLineMetaReq is sent. If queryMeta is
     * non-null and queryMeta->valid, skips QLM entirely and uses the
     * supplied epoch/owner.
     *
     * @param line_pa      Physical address (requester's view)
     * @param keepAsClean  True if owner wants to keep clean exclusive copy
     * @param dirtyData    64-byte dirty cacheline data (nullptr if unavailable)
     * @param queryMeta    Pre-resolved metadata (nullptr = query fresh)
     * @param outQueryReqId If non-null, receives the QLM reqId sent
     * @return             >0 writeback accepted, 0 rejected, -2 pending (retry)
     */
    struct WritebackQueryMeta {
        bool valid;
        uint64_t epochVal;
        int requesterNode;
        WritebackQueryMeta() : valid(false), epochVal(0), requesterNode(-1) {}
    };
    int handleWriteback(uint64_t line_pa, bool keepAsClean,
                        const uint8_t *dirtyData = nullptr,
                        const WritebackQueryMeta *queryMeta = nullptr,
                        uint64_t *outQueryReqId = nullptr,
                        uint64_t cachedQlmReqId = 0,
                        int sourceSocket = 0,
                        uint64_t *ioWritebackReqId = nullptr);

    /**
     * Phase 2 async: fire a WritebackReq with previously-resolved metadata
     * (epoch + owner). Bypasses the QueryLineMetaReq entirely. Called by
     * EPSNFController when a cached QLM response is available.
     *
     * @return same semantics as handleWriteback
     */
    int handleWritebackWithMeta(uint64_t line_pa, bool keepAsClean,
                                const uint8_t *dirtyData,
                                uint64_t epochVal, int requesterNode,
                                int sourceSocket = 0,
                                 uint64_t *ioWritebackReqId = nullptr);

    int commitStore(uint64_t homePa, int requesterNode,
                    uint64_t permissionEpoch, uint64_t commitId,
                    uint64_t byteMask, const uint8_t *data,
                    int homeNode, int homeSocket, int sourceSocket,
                    uint64_t &ioWritebackReqId);
    int publishInternalWriteback(uint64_t homePa, uint64_t publicationId,
                                 uint64_t byteMask, const uint8_t *data,
                                 int homeNode, int homeSocket, int sourceSocket,
                                 uint64_t &ioWritebackReqId);

    /**
     * Called by EPSNFController when HN-F completes a WriteNoSnp write
     * to DRAM. Notifies UBCC to release directory ownership.
     *
     * @param homePa  Home-node physical address that was written to DRAM
     */
    void handleHomeWritebackComplete(uint64_t homePa);

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

    /**
     * Get owner-mismatch-rejected count for test observation (P0-1).
     * Writeback from a node that is not the current owner is rejected.
     */

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

    // v4: Clear / Upgrade envelope accessors
    const OuterClearMsg& lastClearMsg() const { return _lastClearMsg; }
    const OuterClearAckMsg& lastClearAckMsg() const { return _lastClearAckMsg; }
    const OuterUpgradeReq& lastUpgradeReq() const { return _lastUpgradeReq; }
    const OuterUpgradeAck& lastUpgradeAck() const { return _lastUpgradeAck; }
    const OuterUpgradeDone& lastUpgradeDone() const { return _lastUpgradeDone; }
    const OuterUpgradeDoneAck& lastUpgradeDoneAck() const { return _lastUpgradeDoneAck; }

    // ---- upgrade_invalidate_fix: upgrade ack callback ----
    /**
     * Called by home UBCC when all invalidation acks for an upgrade
     * have been received (upgrade_invalidate_fix D2).
     * Triggers the deferred receiveUpgradeAck() on EPRNFController.
     */
    void notifyUpgradeAckReady(uint64_t linePa);

    /**
     * Event-driven completion of a held SnpCleanInvalid-upgrade. Called when
     * an OuterUpgradeResp arrives on the async Port path. Drives the matching
     * held upgrade (by reqId) to completion, replacing the prior busy-wait
     * where a re-issued snoop had to pull the cached UpgradeResp.
     */
    void onUpgradeRespArrived(uint64_t reqId);

    /** Invalidate any in-flight pending-upgrade bookkeeping for a line so a
     *  later store re-issues a fresh upgrade. Used by the upgrade-reject
     *  fallback path in EPRNFController. */
    void clearPendingUpgradeTxn(uint64_t linePa);

    /** Clear any rejected UpgradeResp cached in the UBAdapter for this line,
     *  so a retry sends a fresh UpgradeReq. */
    void clearCachedUpgradeResp(uint64_t linePa, int sourceSocket);

    /** Process a deferred InvalidateReq after the held snoop that blocked it
     *  has been resolved (SnpResp_I sent). Acks directly since the local copy
     *  is already invalidated. */
    void flushDeferredInvalidation(uint64_t linePa);

    /** Check whether a deferred InvalidateReq exists for this line. */
    bool hasDeferredInvalidation(uint64_t linePa) const {
        return _deferredInvalidationReqs.find(linePa) !=
               _deferredInvalidationReqs.end();
    }

    /** Send SnpResp_I for a rejected upgrade's held snoop (stale completion
     *  of the local HN-F's CleanUnique). The L2 will get CompUCRespStale and
     *  retry the store with a fresh ReadUnique. */
    void sendSnpRespIForRejected(uint64_t linePa, uint64_t hnfDestRaw);

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

    /**
     * Return the data source of the last grant data (F3).
     * Indicates whether data came from HomeMemory, RecallBuffer, or NoData.
     */
    GrantDataSource lastGrantDataSource() const { return _lastGrantDataSource; }

    /** Consume per-line grant data: 1=data, 0=explicit NoData, -1=missing. */
    int takeGrantData(uint64_t linePa, DataBlock &data,
                       GrantDataSource &source);
    bool getStoreAuthorization(uint64_t linePa, int sourceSocket,
                               int &requesterNode,
                               uint64_t &permissionEpoch) const;

    bool isDsmAddr(uint64_t pa) const;

    /** EP_RNF snoop counter for test verification */
    uint64_t getEpRnfSnoopCount() const;
    void resetEpRnfSnoopCount();
    void incrementEpRnfSnoopCount();


    /** Bind the UBAdapter for message-path access to UBCC (legacy single-socket). */
    void setUBAdapter(UBAdapter *adapter) {
        if (_ubAdapters.empty()) {
            _ubAdapters.resize(_numSockets, nullptr);
        }
        _ubAdapters[0] = adapter;
    }
    /** Get UBAdapter for socket (default 0 for backward compat). */
    UBAdapter* getUBAdapter(int socket = 0) const {
        if (socket >= 0 && socket < (int)_ubAdapters.size())
            return _ubAdapters[socket];
        return nullptr;
    }
    /** Register a per-socket UBAdapter (v4-dual-socket). */
    void registerAdapter(int socketId, UBAdapter *adapter) {
        if (socketId >= (int)_ubAdapters.size())
            _ubAdapters.resize(socketId + 1, nullptr);
        _ubAdapters[socketId] = adapter;
    }
    int numSockets() const { return _numSockets; }

    /** v4-dual-socket: register per-socket EP-SNF controller (§3.5) */
    void registerEpSnf(int socketId, EPSNFController *ctrl);
    EPSNFController* getEpSnf(int socketId) const;

    /** Handle QueryLineMetaResp from UBCC via UBAdapter (v4-dual-socket). */
    void handleQueryLineMetaResp(const CoherenceMessage &msg);

    /** Send HomeWritebackNotify to home UBCC (v4-dual-socket). */
    void sendHomeWritebackNotify(uint64_t homePa, int homeSocket);

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

    void setMetaRnfController(MetaRNFController *ctrl);





    // ---- M6: EP_RNF delayed response hook ----
    /**
     * Register the EPRNFController for delayed HN response support.
     */
    void setEpRnfController(EPRNFController *ctrl) { _epRnfCtrl = ctrl; }

    /**
     * Get the registered EPRNFController (may be nullptr).
     */
    EPRNFController* getEpRnfController() const { return _epRnfCtrl; }

    // ---- HA endpoint API for a later per-store SLICC hook ----
    // The caller owns ioReqId.  Initialize it to zero for a new operation and
    // retain the returned value across retries.  Return values are identical to
    // UBAdapter: 1 completed, -2 pending, -1 transport/setup failure.
    int requestHAPermission(uint64_t linePa, HAOperation operation,
                             uint64_t permissionEpoch, uint64_t byteMask,
                             const uint8_t *writeData,
                            int dstNode, int dstSocket, uint64_t &ioReqId,
                            UBHAPermissionRespBody &outResp,
                             int sourceSocket = 0);
    // Convert a requester-local DSM line into the home-plane address and
    // capture the latest requester epoch used by the permission transaction.
    bool resolveHAStoreTarget(uint64_t localLinePa, int sourceSocket,
                              uint64_t &homeLinePa, int &homeNode,
                              int &homeSocket, uint64_t &permissionEpoch) const;
    bool acknowledgeHAPermission(uint64_t linePa, HAOperation operation,
                                 HAStatus status, uint64_t permissionEpoch,
                                 int dstNode, int dstSocket, uint64_t reqId,
                                 int sourceSocket = 0);
    void recordHAInstall(uint64_t localLinePa, HAOperation operation,
                         uint64_t permissionEpoch, int homeNode,
                         uint64_t reqId, int sourceSocket);
    void recordHADirtyData(uint64_t localLinePa, int sourceSocket,
                           const DataBlock &data);
    void registerHADataCache(int sourceSocket, CacheMemory *cache);
    void invalidateHADataCaches(int sourceSocket, uint64_t localLinePa);
    bool hasHADataCacheLine(int sourceSocket, uint64_t localLinePa) const;
    bool readHADataCacheLine(int sourceSocket, uint64_t localLinePa,
                             DataBlock &data) const;
    int requestHAPresenceProbe(uint64_t linePa, HAProbeAction action,
                               uint64_t expectedEpoch, int dstNode,
                               int dstSocket, uint64_t &ioReqId,
                               UBHAPresenceProbeRespBody &outResp,
                               int sourceSocket = 0);

    // UBAdapter entry point for peer-initiated probes.  Completion is based on
    // an actual EPRNF→HN-F CHI transaction; requester bookkeeping is not used
    // as the presence decision.
    void handleHAPresenceProbeRequest(const CoherenceMessage &request,
                                      UBAdapter *sourceAdapter);
    void handleHAPermissionRequest(const CoherenceMessage &request,
                                   UBAdapter *sourceAdapter);

    // ---- M6: Cross-Node EPBackend Routing Registry ----
    /**
     * Static registry of EPBackend instances keyed by node ID.
     * Used to route recall requests to the owner node's EPBackend.
     */
    static EPBackend* getBackendInstance(int node_id);

  private:
    const int _nodeId;
    NodeAddressMap _addrMap;
    uint64_t _epRnfSnoopCount = 0;
    MetaRNFController *_metaRnf = nullptr;
    std::vector<UBAdapter*> _ubAdapters;  // v4-dual-socket: per-socket adapters
    std::vector<EPSNFController*> _epSnfs; // v4-dual-socket: per-socket EP-SNF
    int _numSockets = 1;                   // v4-dual-socket
    EPRNFController *_epRnfCtrl = nullptr;
    RubySystem *_ruby_system = nullptr;
    uint64_t _metadataPrivateBase = 0;
    uint64_t _metadataPrivateSize = 0;

    // ---- Q1: Grant Data Buffer ----
    // Cache-line-sized buffer populated after each grant decision.
    // Used by EPSNFController to construct CompData response payload.
    DataBlock _lastGrantDataBlock;
    bool _lastGrantDataValid = false;
    GrantDataSource _lastGrantDataSource = GrantDataSource::NoData;
    struct PendingGrantData {
        DataBlock data{64};
        GrantDataSource source = GrantDataSource::NoData;
        bool valid = false;
    };
    std::map<uint64_t, PendingGrantData> _pendingGrantData;

    // ---- F2: Recall Capture Data Buffer ----
    // Data captured from CHI completion during recall, transferred from
    // EPRNFController::PendingChiTxn.recallDataBlk before the callback fires.
    // Used to construct OuterRecallResponse with actual data payload.
    DataBlock _recallCaptureDataBlock;
    bool _recallCaptureDataValid = false;

    // Phase 4: verbose diagnostic logging gate (I14)
    bool _verboseLog = false;

    /**
     * F3: Populate the grant data buffer from a formal data source.
     * Replaces the old functionalRead/scavenge/back-fill approach.
     *
     * @param homePa     Physical address in home node's PA view
     * @param dataSource Formal data source (HomeMemory/RecallBuffer/NoData)
     */
    void populateGrantData(uint64_t homePa, GrantDataSource dataSource);

    // ---- M5: Requester-Side Bookkeeping ----
    // Per-line entries tracking global permissions for remote DSM lines.
    std::map<uint64_t, RequesterLineEntry> _requesterLines;
    using HALineKey = std::pair<int, uint64_t>;
    std::map<HALineKey, RequesterLineEntry> _haRequesterLines;
    std::map<HALineKey, DataBlock> _haDirtyData;
    std::vector<std::vector<CacheMemory *>> _haDataCachesBySocket;
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

    // Active recall PAs (local view).
    // Inserted on handleRecallRequest, erased on sendRecallResponse.
    // Used by EPRNFController::handleSnpCleanInvalid to detect RECALL-induced
    // snoops and avoid spurious OuterUpgradeReq.
    std::map<uint64_t, bool> _activeRecallPAs;

    // ---- M7: Writeback / Evict counters and envelopes ----
    uint64_t _writebackCount;
    uint64_t _evictCount;
    OuterWritebackMsg _lastWritebackMsg;
    OuterEvictMsg _lastEvictMsg;
    OuterAckMsg _lastAckMsg;

    // ---- M8: Invalidation counters and envelopes ----
    uint64_t _invalidationReceivedCount;
    uint64_t _invalidationAckSentCount;
    bool _haEndpointEnabled = false;
    bool _losslessOneWayClearEnabled = false;
    OuterInvalidateMsg _lastInvalidateMsg;
    OuterInvalidationAck _lastInvalidationAck;

    // ---- v4: Clear / ClearAck envelopes ----
    OuterClearMsg _lastClearMsg;
    OuterClearAckMsg _lastClearAckMsg;

    // ---- v4: Local Upgrade envelopes ----
    OuterUpgradeReq _lastUpgradeReq;
    OuterUpgradeAck _lastUpgradeAck;
    OuterUpgradeDone _lastUpgradeDone;
    OuterUpgradeDoneAck _lastUpgradeDoneAck;

    // ---- upgrade_invalidate_fix: PendingGrantTxn (§3.3.3) ----
    // Independent grant tuple context for Clear replay correctness (D5).
    struct PendingGrantTxn {
        bool valid;
        uint64_t linePa;
        uint64_t localLinePa;
        int homeNode;
        uint64_t baseEpoch;   // home-approved GRANT_HANDSHAKE baseEpoch
        uint64_t reqId;
        int sourceAdapter;
        OuterReqType reqType;
        bool writeIntent;
        OuterGrantType grantType;
        Tick outerStartTick;
        bool clearQueued;
        bool clearSendLogged;

        PendingGrantTxn() : valid(false), linePa(0), localLinePa(0), homeNode(-1),
                            baseEpoch(0), reqId(0), sourceAdapter(0),
                            reqType(OuterReqType::GlobalReadShared),
                            writeIntent(false),
                            grantType(OuterGrantType::GlobalGrantShared),
                            outerStartTick(0), clearQueued(false),
                            clearSendLogged(false) {}
    };
    // One Home PA has one node-level Grant/Clear transaction. The originating
    // socket is routing context inside the tuple, not part of transaction
    // identity; keying by socket would allow two reqIds for the same Home line.
    std::map<uint64_t, PendingGrantTxn> _pendingGrantTxns;

    struct PendingReadTxn {
        uint64_t homePa = 0;
        uint64_t localLinePa = 0;
        int homeNode = -1;
        int homeSocket = 0;
        int sourceAdapter = 0;
        uint64_t epoch = 0;
        uint64_t reqId = 0;
        int neededPerm = 0;
        bool writeIntent = false;
        Tick outerStartTick = 0;
        uint64_t retryCount = 0;
    };
    // Created before the first ReadReq so an async response window, local PA
    // alias, or requester-state mutation cannot allocate a second reqId for the
    // same Home PA.
    std::map<uint64_t, PendingReadTxn> _pendingReadTxns;

    // ---- Async upgrade pending txn (mirrors PendingGrantTxn for clear) ----
    // When sendUpgradeReq returns -2 (UpgradeResp not yet arrived), we save the
    // reqId/epoch so a snoop retry reuses the SAME reqId and hits the cached
    // UpgradeResp, instead of allocating a fresh reqId that the home rejects
    // (existing outstanding) — the death loop that hung TC3/8/10/11.
    struct PendingUpgradeTxn {
        bool valid;
        uint64_t linePa;
        int homeNode;
        int sourceSocket;
        uint64_t epoch;
        uint64_t reqId;
        Tick startTick;
        bool acceptedPending;
        PendingUpgradeTxn() : valid(false), linePa(0), homeNode(-1),
                               sourceSocket(0),
                               epoch(0), reqId(0), startTick(0),
                               acceptedPending(false) {}
    };
    std::map<uint64_t, PendingUpgradeTxn> _pendingUpgradeTxns;

    // InvalidateReqs that arrived while a SnpCleanInvalid-upgrade was held.
    // They are deferred until the held snoop is resolved (SnpResp_I sent),
    // because their startCleanUnique would collide with the HN-F's pending
    // SnpCleanInvalid TBE. Once SnpResp_I has invalidated the local copy,
    // the deferred InvalidateReq can be ack'd directly (no CleanUnique needed).
    std::map<uint64_t, OuterInvalidateMsg> _deferredInvalidationReqs;

    struct HAPendingProbe {
        CoherenceMessage request;
        UBAdapter *adapter = nullptr;
    };
    using HAProbeKey = std::tuple<uint16_t, uint16_t, uint64_t, uint64_t,
                                  uint64_t, uint8_t, uint64_t>;
    std::map<HAProbeKey, HAPendingProbe> _haPendingProbes;
    std::map<HAProbeKey, CoherenceMessage> _haCompletedProbeResponses;

    // EP-SNF bypasses the CPU Sequencer, so HA ReadNoSnp fills need their own
    // stable asynchronous permission transaction.  Request semantics are part
    // of the key: retries must poll the exact wire reqId, while a later request
    // with different intent must not inherit an earlier authorization.
    using HARemoteMissKey = std::tuple<uint64_t, int, int, bool>;
    struct HARemoteMissContext {
        uint64_t homePa = 0;
        int homeNode = -1;
        int homeSocket = 0;
        int sourceSocket = 0;
        uint64_t permissionEpoch = 0;
        uint64_t reqId = 0;
        HAOperation operation = HAOperation::Read;
        bool awaitingInstall = false;
    };
    std::map<HARemoteMissKey, HARemoteMissContext> _haRemoteMisses;

    // ---- M6: Cross-Node EPBackend Routing Registry ----
    static std::map<int, EPBackend*> _backendInstances;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__
