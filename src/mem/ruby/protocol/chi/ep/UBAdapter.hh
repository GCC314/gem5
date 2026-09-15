#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBADAPTER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBADAPTER_HH__

#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <memory>
#include <set>
#include <vector>

#include "mem/ruby/common/DataBlock.hh"
#include "mem/ruby/protocol/chi/ep/CoherenceMessage.hh"
#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"
#include "mem/ruby/protocol/chi/ep/BoundaryResponses.hh"
#include "protocol/ControlCredits.hh"
#include "protocol/ControlFrame.hh"
#include "protocol/ResponseFrame.hh"
#include "protocol/ProgressFrame.hh"
#include "protocol/FixedQueue.hh"
#include "mem/ruby/protocol/chi/ep/BoundaryQueue.hh"
#include "params/UBAdapter.hh"
#include "sim/sim_object.hh"
#include "sim/eventq.hh"

namespace pseudo { class PseudoMemPort; class PseudoManager; }
namespace framework { struct Message; struct Port; }

namespace gem5
{
namespace ruby
{

class EPBackend;

// Forward-declare EPBackend message types (defined in EPBackend.hh)
struct OuterRecallMsg;
struct OuterInvalidateMsg;

/**
 * EPBackend's message-layer facade.
 *
 * UBAdapter is the ONLY interface EPBackend uses to access UBCC.
 * All calls (even local-home) go through:
 *   EPBackend → UBAdapter → UBIOModule *→ UBCCController
 *
 * EPBackend 侧所有 UBCC 交互都必须经过这里的消息路径。
 */
class UBAdapter : public SimObject
{
  public:
    PARAMS(UBAdapter);
     UBAdapter(const Params &p);
     ~UBAdapter();

     void init() override;
     void startup() override;

    int nodeId() const { return _nodeId; }
    int socketId() const { return _socketId; }

    /** Bind the EPBackend that owns this adapter. */
    void bindBackend(EPBackend *backend) { _backend = backend; }


    /** Set PseudoMemPort for async message transport (multi-process path). */
    void setPseudoPort(pseudo::PseudoMemPort *port) { _pseudoPort = port; }
    pseudo::PseudoMemPort* pseudoPort() const { return _pseudoPort; }

    /** Opaque transport availability (used by EPBackend during init). */
    framework::Port* port() const { return _port; }

    void setOnResponseWired(std::function<void()> cb) { _onResponseWired = std::move(cb); }

    /**
     * Phase 2: Synchronous Read Request.
     *
     * Packages an OuterRequest as a ReadReq CoherenceMessage, sends it through
     * the local UBIOModule *, waits for the ReadResp, and returns the
     * grant decision.
     *
     * v4-dual-socket: adds ingressSocket and homeSocket parameters.
     */
    int sendReadReq(
        uint64_t homePa, int reqType, bool writeIntent,
        int requesterNode, uint64_t epoch, uint64_t reqId,
        int homeNode, int ingressSocket, int homeSocket,
        Tick *outGrantVisibleTick, Tick *outSentinelVisibleTick,
        bool *outRecallNeeded, int *outRecallOwnerNode,
        GrantDataSource *outDataSource, uint64_t *outAuthEpoch,
        uint64_t *outGrantEpoch,
        int *outPendingInvCount, uint64_t *outPendingInvMask,
        uint64_t *outCommittedEpoch,
        DataBlock *outGrantData, bool *outGrantDataValid);

    // ---- Phase 3: EPBackend→UBCC synchronous paths ----
    int sendWritebackReq(uint64_t homePa, int requesterNode,
                          uint64_t epochVal, UBWritebackKind kind,
                          UBWriteDisposition disposition, uint64_t byteMask,
                          int homeNode, int homeSocket,
                         const uint8_t *dirtyData = nullptr,
                         uint64_t *ioReqId = nullptr,
                         uint64_t parentReqId = 0, uint64_t parentEpoch = 0,
                         uint64_t *mergedRecallReqId = nullptr);

    // Caller owns the single serial escape identity until matched Home ACK.
    // 1=applied/duplicate, 2=exact foreign retirement, 3=stale (NOT success),
    // 0=retry/rejected, negative=not ready/invalid. Identity is checked here.
    int sendConditionalRelease(uint64_t homePa, uint64_t lease,
        int homeNode, int homeSocket, uint64_t reqId);
    int sendEvictReq(uint64_t homePa, int evictingNode,
                     uint64_t epochVal, int homeNode, int homeSocket);

    int sendUpgradeReq(uint64_t homePa, int requesterNode,
                       uint64_t epoch, uint64_t reqId,
                       int desiredPerm, int cause,
                       uint64_t *outUpgradeTargetMask,
                       uint64_t *outCommittedEpoch,
                       int homeNode, int homeSocket,
                       bool checkOnly = false,
                       bool forceWire = false);

    int sendUpgradeDoneReq(uint64_t homePa, int requesterNode,
                           uint64_t epoch, uint64_t reqId,
                           int homeNode, int homeSocket);

    int sendClearReq(uint64_t linePa, int srcNode,
                     uint64_t epoch, uint64_t reqId,
                             int homeNode, int homeSocket);
    bool sendClearReqOneWay(uint64_t linePa, int srcNode,
                            uint64_t epoch, uint64_t reqId,
                            int homeNode, int homeSocket);

    // Cross-node EPBackend→EPBackend (fire-and-forget via router)
    bool sendRecallReqToOwner(int targetNode,
                              const OuterRecallMsg &recallMsg,
                              int homeSocket);
    bool sendInvalidateReqToSharer(int targetNode,
                                    const OuterInvalidateMsg &invMsg,
                                    int homeSocket);

    // EPBackend→UBCC fire-and-forget messages
    bool sendRecallResp(uint64_t linePa, int ownerNode,
                         bool ackReceived, bool dataReturned, uint64_t epoch,
                        uint64_t reqId,
                        const DataBlock *dataBlk,
                        int homeNode, int homeSocket,
                        bool dataForwarded = false);
    bool sendInvalidateAck(uint64_t linePa, int ackNode,
                            uint64_t epoch, uint64_t reqId,
                            int homeNode, int homeSocket);

    // C4: Direct data forward from owner to requester (bypasses home)
    bool sendDirectData(const CoherenceMessage &msg);

    // ---- v4-dual-socket: new message types ----
    /**
     * Query line metadata (epoch, ownerNode) from home UBCC.
     * Used for writeback fallback when _requesterLines has no entry.
     */
    /**
     * Query line metadata from home UBCC.
     *
     * @param cachedReqId  If >0, check _readyResponses by exact key
     *                     (QueryLineMetaResp, cachedReqId).  If found
     *                     returns immediately without sending.  If NOT
     *                     found returns -2 (pending — the original
     *                     request is still in-flight).  MUST NOT scan
     *                     by PA.
     * @param outReqId     If non-null, receives the stable reqId.
     */
    int sendQueryLineMetaReq(uint64_t homePa, int homeNode, int homeSocket,
                              uint64_t &outEpoch, int &outOwnerNode,
                              bool &outFound,
                              uint64_t *outReqId = nullptr,
                              uint64_t cachedReqId = 0);

    /**
     * Send HomeWritebackNotify to home UBCC after HN-F DDR4 write complete.
     */
    void sendHomeWritebackNotify(uint64_t homePa, uint64_t epoch,
                                  int homeNode, int homeSocket);

    /**
     * Phase 2 async: try to retrieve a cached QueryLineMetaResp from
     * _readyResponses. Used by EPSNFController to resolve pending
     * writeback metadata without a blocking transportRecv.
     */
    bool tryGetQueryLineMetaResp(uint64_t reqId,
                                  uint64_t &outEpoch, int &outOwnerNode,
                                  bool &outFound);

    // ---- HA endpoint wire API ----
    // ioReqId == 0 allocates a transaction ID.  A retry must pass the returned
    // non-zero ID; the adapter then polls the exact response and never emits a
    // second request.  Return: 1=response consumed, -2=pending, -1=send error.
    int sendHAPermissionReq(uint64_t linePa, HAOperation operation,
                             uint64_t permissionEpoch, uint64_t byteMask,
                             const uint8_t *writeData,
                            int dstNode, int dstSocket, uint64_t &ioReqId,
                            UBHAPermissionRespBody &outResp);
    bool sendHAPermissionAck(uint64_t linePa, HAOperation operation,
                             HAStatus status, uint64_t permissionEpoch,
                             int dstNode, int dstSocket, uint64_t reqId);
    int sendHAPresenceProbeReq(uint64_t linePa, HAProbeAction action,
                               uint64_t expectedEpoch, int dstNode,
                               int dstSocket, uint64_t &ioReqId,
                               UBHAPresenceProbeRespBody &outResp);

    // Responses to requests initiated by the HA peer.  These use the reliable
    // output queue because they are produced from asynchronous CHI callbacks.
    bool sendHAPermissionResp(const CoherenceMessage &request,
                              const UBHAPermissionRespBody &body);
    bool sendHAPresenceProbeResp(const CoherenceMessage &request,
                                 const UBHAPresenceProbeRespBody &body);

    /** Clear cached ready-responses for a given line PA (e.g. a rejected
     *  UpgradeResp) so a retry sends a fresh request. */
    void clearReadyResponsesForLine(
        uint64_t linePa, CoherenceMessageType responseType);

    /**
     * Receive a message from the local UBIOModule *.
     * Routes to the appropriate handler based on message type.
     */
    void recvFromRouter(const CoherenceMessage &msg);

    /** Event-driven response processing. Replaces busy-poll transportRecv. */
    void wakeup();
    void scheduleResponseCheck();
    void handleResponse(const framework::Message *m);

    /** Set transport mode — instance-level fixed after init. */

    // ---- Accessors ----
    const NodeAddressMap& addrMap() const { return _addrMap; }

    /** The router for UBCC→Adapter messages. */

  private:
    int _nodeId;
    int _socketId;
    int _numNodes;
    int _numSockets;
    int _localNode;
    EPBackend *_backend = nullptr;
    pseudo::PseudoMemPort *_pseudoPort = nullptr;
    framework::Port *_port = nullptr;
    struct PortExitState {
        framework::Port *port = nullptr;
        bool terminated = false;
    };
    std::shared_ptr<PortExitState> _portExitState;
    NodeAddressMap _addrMap;
    uint64_t _nextSeq = 1;

    /** Send through the owned opaque framework port. */
    bool transportSend(const CoherenceMessage &msg);
    bool reserveResponse(const CoherenceMessage &msg, bool cancel = false);
    void cacheResponse(const CoherenceMessage &msg);
    bool transportSendReliable(const CoherenceMessage &msg);
    bool sendControlReply(CoherenceMessage msg);
    bool reserveMetadataReply(const CoherenceMessage &request);
    bool sendMetadataReply(const CoherenceMessage &reply);
    void sendMetaRNFLineErrorResponse(framework::Port *port,
        CoherenceMessageType type, MetaRNFLineStatus status,
        uint64_t reqId, uint64_t bucketOffset, int node, int socket,
        int dstNode, int dstSocket, uint32_t source, uint32_t target);
    void drainReliableOutputs();
    size_t pollVisibleMessages(Tick curT, size_t budget);

    static void registerClockAdapter(UBAdapter *adapter);
    static void unregisterClockAdapter(UBAdapter *adapter);
    static void armClockPump(Tick when);
    static std::vector<UBAdapter *> _clockAdapters;
    static UBAdapter *_clockOwner;
    static bool _clockPumpRunning;

    /** Multi-process split: send a BarrierReached CoherenceMessage (PAYLOAD)
     *  to ubio via Port.  @p seq is the barrier generation (TC90 fix). */
    void sendBarrierReached(uint32_t mask, uint32_t nodeId, uint32_t seq);

    /** Poll the opaque framework port and dispatch to recvFromRouter(). */
    bool transportRecv(CoherenceMessageType expectedType, uint64_t expectedReqId);

    void drainDeferredControls();

    /**
     * Per-transaction pending state.
     * Tracks in-flight requests waiting for responses.
     */
    struct PendingKey {
        CoherenceMessageType respType;
        uint64_t reqId;
        bool operator==(const PendingKey &o) const {
            return respType == o.respType && reqId == o.reqId;
        }
        bool operator<(const PendingKey &o) const {
            return std::tie(respType, reqId) < std::tie(o.respType, o.reqId);
        }
    };

    static constexpr size_t MaxInflightReadReqs = 64;
    static constexpr Tick ReadReqRetryTicks = 1000000;
    std::map<uint64_t, Tick> _inflightReadReqs;
    std::set<uint64_t> _inflightClearReqs;
    std::set<uint64_t> _inflightWritebackReqs;
    std::set<uint64_t> _inflightHAPermissionReqs;
    std::set<uint64_t> _inflightHAPresenceProbeReqs;
    std::map<uint64_t, Tick> _clearRetryTick;

    // Nine response classes, 64 pre-reserved cells per class. These are
    // transport obligations, not independent line authority or BTT entries.
    BoundaryResponses<PendingKey, cc::glob::ResponseFrame, 9 * 64> _readyResponses;
    uint64_t _releaseReqId = 0;
    uint64_t _releasePa = 0, _releaseLease = 0;
    int _releaseHome = -1, _releaseSocket = -1;

    /** Last response (for sync transportRecv fallback and recvFromRouter). */
    CoherenceMessage _lastResponse;
    bool _lastResponseValid = false;

    uint64_t _nextLocalReqId = 1;
    uint64_t allocLocalReqId();

    /** Event for periodic Port response polling. */
    EventFunctionWrapper _responseCheckEvent;
    int _responseCheckCount = 0;
    bool _eventArmed = false;

    std::function<void()> _onResponseWired;
    friend class EPSNFController;

    // Deferred async control messages (InvalidateReq/RecallReq/UpgradeAckNotify)
    // Four receive leases from each of at most 256 Home planes. No generic
    // notifications are staged here: their storage is in their original root.
    cc::glob::FixedQueue<cc::glob::ControlFrame> _deferredControls;
    cc::glob::ControlCredits<CoherenceMessageHeader> _controlReceipts;
    struct ControlOutput {
        cc::glob::ProgressFrame reply;
        bool queued = false;
        bool reserved = false;
    };
    struct LeasedControlOutput {
        cc::glob::ControlReply reply;
        bool queued = false;
    };
    std::unique_ptr<std::array<LeasedControlOutput, 4>[]> _controlOutputs;
    std::array<ControlOutput, 64> _clearOutputs{};
    std::array<ControlOutput, 64> _haAckOutputs{};
    struct MetadataReply {
        CoherenceMessage message;
        bool reserved = false;
        bool ready = false;
    };
    std::array<MetadataReply, 64> _metadataReplies{};
    std::array<MetadataReply, 64> _pageReplies{};
    std::unique_ptr<MetadataReply[]> _permissionReplies;
    struct DirectChild {
        CoherenceMessageHeader request;
        CoherenceMessageHeader dataHeader;
        std::array<uint8_t, 64> data{};
        bool live = false;
        bool ready = false;
        Tick grantWaitStart = 0;
    };
    std::array<DirectChild, 64> _directChildren{};
    BoundaryQueue<CoherenceMessage, 64> _directOutputs;
    bool _drainingDeferredControls = false;

    // Ordered, lossless-at-the-adapter-boundary output queue.  A full opaque
    // transport is retried by wakeup() without changing reqId or wire fields.
    BoundaryQueue<CoherenceMessage, 64 * 4> _reliableOutputs;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBADAPTER_HH__
