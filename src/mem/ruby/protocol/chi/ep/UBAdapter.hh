#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBADAPTER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBADAPTER_HH__

#include <cstdint>
#include <deque>
#include <functional>
#include <map>
#include <set>

#include "mem/ruby/common/DataBlock.hh"
#include "mem/ruby/protocol/chi/ep/CoherenceMessage.hh"
#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"
#include "params/UBAdapter.hh"
#include "sim/sim_object.hh"
#include "sim/eventq.hh"

namespace pseudo { class PseudoMemPort; class PseudoManager; }
namespace framework { class Port; struct MemMessage; }

namespace gem5
{
namespace ruby
{

class EPBackend;
class UBCCController;

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

    /** Optional external transport port (injected by launcher/module). */
    void setPort(framework::Port *port) { _port = port; }
    framework::Port* port() const { return _port; }

    void setOnResponseWired(std::function<void()> cb) { _onResponseWired = std::move(cb); }

    /**
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
        int *outPendingInvCount, uint64_t *outPendingInvMask,
        uint64_t *outCommittedEpoch,
        DataBlock *outGrantData, bool *outGrantDataValid);

    // ---- Phase 3: EPBackend→UBCC synchronous paths ----
    int sendWritebackReq(uint64_t homePa, int requesterNode,
                         uint64_t epochVal, bool keepAsClean,
                         int homeNode, int homeSocket);

    int sendEvictReq(uint64_t homePa, int evictingNode,
                     uint64_t epochVal, int homeNode, int homeSocket);

    int sendUpgradeReq(uint64_t homePa, int requesterNode,
                       uint64_t epoch, uint64_t reqId,
                       int desiredPerm, int cause,
                       uint64_t *outUpgradeTargetMask,
                       uint64_t *outCommittedEpoch,
                       int homeNode, int homeSocket,
                       bool checkOnly = false);

    int sendUpgradeDoneReq(uint64_t homePa, int requesterNode,
                           uint64_t epoch, uint64_t reqId,
                           int homeNode, int homeSocket);

    int sendClearReq(uint64_t linePa, int srcNode,
                     uint64_t epoch, uint64_t reqId,
                     int homeNode, int homeSocket);

    // Cross-node EPBackend→EPBackend (fire-and-forget via router)
    void sendRecallReqToOwner(int targetNode,
                              const OuterRecallMsg &recallMsg,
                              int homeSocket);
    void sendInvalidateReqToSharer(int targetNode,
                                    const OuterInvalidateMsg &invMsg,
                                    int homeSocket);

    // EPBackend→UBCC fire-and-forget messages
    bool sendRecallResp(uint64_t linePa, int ownerNode,
                        bool dataReturned, uint64_t epoch,
                        uint64_t reqId,
                        const DataBlock *dataBlk,
                        int homeNode, int homeSocket);
    bool sendInvalidateAck(uint64_t linePa, int ackNode,
                            uint64_t epoch, uint64_t reqId,
                            int homeNode, int homeSocket);

    // ---- v4-dual-socket: new message types ----
    /**
     * Query line metadata (epoch, ownerNode) from home UBCC.
     * Used for writeback fallback when _requesterLines has no entry.
     */
    int sendQueryLineMetaReq(uint64_t homePa, int homeNode, int homeSocket,
                              uint64_t &outEpoch, int &outOwnerNode,
                              bool &outFound);

    /**
     * Send HomeWritebackNotify to home UBCC after HN-F DDR4 write complete.
     */
    void sendHomeWritebackNotify(uint64_t homePa, uint64_t epoch,
                                  int homeNode, int homeSocket);

    /**
     * Receive a message from the local UBIOModule *.
     * Routes to the appropriate handler based on message type.
     */
    void recvFromRouter(const CoherenceMessage &msg);

    /** Event-driven response processing. Replaces busy-poll transportRecv. */
    void wakeup();
    void checkResponseCallbacks();
    void scheduleResponseCheck();
    void handleResponse(framework::MemMessage *m);

    /** Set transport mode — instance-level fixed after init. */

    // ---- Accessors ----
    const NodeAddressMap& addrMap() const { return _addrMap; }

    /** The router for UBCC→Adapter messages. */

  private:
    int _nodeId;
    int _socketId;
    EPBackend *_backend = nullptr;
    pseudo::PseudoMemPort *_pseudoPort = nullptr;
    framework::Port *_port = nullptr;
    NodeAddressMap _addrMap;
    uint64_t _nextSeq = 1;

    /** Send via injected framework::Port when available, else via router. */
    bool transportSend(const CoherenceMessage &msg);

    /** Poll injected framework::Port and dispatch to recvFromRouter(). */
    bool transportRecv(CoherenceMessageType expectedType, uint64_t expectedReqId);

    void drainDeferredControls();

    /**
     * Per-transaction pending state.
     * Tracks in-flight requests waiting for responses.
     */
    struct PendingKey {
        CoherenceMessageType respType;
        uint64_t reqId;
        bool operator<(const PendingKey &o) const {
            return std::tie(respType, reqId) < std::tie(o.respType, o.reqId);
        }
    };

    struct PendingTxn {
        CoherenceMessageType reqType;
        CoherenceMessageType respType;
        uint64_t reqId;
        uint64_t homeLinePa;
        int homeNode;
        uint64_t epoch;
        std::function<void(const CoherenceMessage&)> onResp;

        PendingTxn()
            : reqType(CoherenceMessageType::ReadReq),
              respType(CoherenceMessageType::ReadResp),
              reqId(0), homeLinePa(0),
              homeNode(-1), epoch(0) {}
    };

    std::set<uint64_t> _inflightReadReqs;

    std::map<PendingKey, PendingTxn> _pendingByReqId;
    std::map<PendingKey, CoherenceMessage> _readyResponses;

    /** Last response (for sync transportRecv fallback and recvFromRouter). */
    CoherenceMessage _lastResponse;
    bool _lastResponseValid = false;

    uint64_t _nextLocalReqId = 1;
    uint64_t allocLocalReqId();

    /** Event for periodic Port response polling. */
    EventFunctionWrapper _responseCheckEvent;
    uint64_t _safeTick = 0;
    int _responseCheckCount = 0;
    bool _eventArmed = false;

    std::function<void()> _onResponseWired;
    friend class EPSNFController;

    // Deferred async control messages (InvalidateReq/RecallReq/UpgradeAckNotify)
    std::deque<CoherenceMessage> _deferredControls;
    bool _drainingDeferredControls = false;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBADAPTER_HH__
