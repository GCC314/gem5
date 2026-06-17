#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBADAPTER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBADAPTER_HH__

#include <cstdint>
#include <functional>
#include <map>

#include "mem/ruby/common/DataBlock.hh"
#include "mem/ruby/protocol/chi/ep/UBMsg.hh"
#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"
#include "params/UBAdapter.hh"
#include "sim/sim_object.hh"

namespace gem5
{
namespace ruby
{

class EPBackend;
class UBRouter;
class UBCCController;

// Forward-declare EPBackend message types (defined in EPBackend.hh)
struct OuterRecallMsg;
struct OuterInvalidateMsg;

/**
 * EPBackend's message-layer facade.
 *
 * UBAdapter is the ONLY interface EPBackend uses to access UBCC.
 * All calls (even local-home) go through:
 *   EPBackend → UBAdapter → UBRouter → UBCCController
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

    int nodeId() const { return _nodeId; }

    /** Bind the EPBackend that owns this adapter. */
    void bindBackend(EPBackend *backend) { _backend = backend; }

    /** Bind the local UBRouter for message dispatch. */
    void setRouter(UBRouter *router) { _router = router; }

    /**
     * Wire the local UBCC to the router.
     * Called from EPBackend::init() after SimObject wiring is complete.
     */
    void bindUbccToRouter(UBCCController *ubcc);

    /**
     * Phase 2: Synchronous Read Request.
     *
     * Packages an OuterRequest as a ReadReq UBMsg, sends it through
     * the local UBRouter, waits for the ReadResp, and returns the
     * grant decision.
     *
     * This is the ONLY entry point for processOuterRequest in Phase 2.
     * All other UBCC access paths remain unchanged (direct calls).
     */
    int sendReadReq(
        uint64_t homePa, int reqType, bool writeIntent,
        int requesterNode, uint64_t epoch, uint64_t reqId,
        int homeNode,
        Tick *outGrantVisibleTick, Tick *outSentinelVisibleTick,
        bool *outRecallNeeded, int *outRecallOwnerNode,
        int *outDataSource, uint64_t *outAuthEpoch,
        int *outPendingInvCount, uint64_t *outPendingInvMask,
        uint64_t *outCommittedEpoch,
        DataBlock *outGrantData, bool *outGrantDataValid);

    // ---- Phase 3: EPBackend→UBCC synchronous paths ----
    bool sendWritebackReq(uint64_t homePa, int requesterNode,
                          uint64_t epochVal, bool keepAsClean,
                          int homeNode);

    bool sendEvictReq(uint64_t homePa, int evictingNode,
                      uint64_t epochVal, int homeNode);

    bool sendUpgradeReq(uint64_t homePa, int requesterNode,
                        uint64_t epoch, uint64_t reqId,
                        int desiredPerm, int cause,
                        uint64_t *outUpgradeTargetMask,
                        uint64_t *outCommittedEpoch,
                        int homeNode);

    bool sendUpgradeDoneReq(uint64_t homePa, int requesterNode,
                            uint64_t epoch, uint64_t reqId,
                            int homeNode);

    bool sendClearReq(uint64_t linePa, int srcNode,
                      uint64_t epoch, uint64_t reqId,
                      int homeNode);

    // Cross-node EPBackend→EPBackend (fire-and-forget via router)
    void sendRecallReqToOwner(int targetNode,
                              const OuterRecallMsg &recallMsg);
    void sendInvalidateReqToSharer(int targetNode,
                                   const OuterInvalidateMsg &invMsg);

    // EPBackend→UBCC fire-and-forget messages
    bool sendRecallResp(uint64_t linePa, int ownerNode,
                        bool dataReturned, uint64_t epoch,
                        uint64_t reqId,
                        const DataBlock *dataBlk,
                        int homeNode);
    bool sendInvalidateAck(uint64_t linePa, int ackNode,
                           uint64_t epoch, uint64_t reqId,
                           int homeNode);

    /**
     * Receive a message from the local UBRouter.
     * Routes to the appropriate handler based on message type.
     */
    void recvFromRouter(const UBMsg &msg);

    // ---- Accessors ----
    const NodeAddressMap& addrMap() const { return _addrMap; }

    /** The router for UBCC→Adapter messages. */
    UBRouter* router() const { return _router; }

  private:
    int _nodeId;
    EPBackend *_backend = nullptr;
    UBRouter *_router = nullptr;
    NodeAddressMap _addrMap;
    uint64_t _nextSeq = 1;

    /**
     * Per-transaction pending state.
     * Tracks in-flight requests waiting for responses.
     */
    struct PendingTxn {
        UBMsgType reqType;
        uint64_t homeLinePa;
        uint64_t localLinePa;
        int homeNode;
        std::function<void(const UBMsg&)> onResp;

        PendingTxn()
            : reqType(UBMsgType::ReadReq),
              homeLinePa(0), localLinePa(0),
              homeNode(-1) {}
    };

    std::map<uint64_t, PendingTxn> _pendingByReqId;

    /** Last response received from router (for synchronous callers). */
    UBMsg _lastResponse;
    bool _lastResponseValid = false;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBADAPTER_HH__
