#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__

#include <cstring>
#include <deque>
#include <functional>
#include <map>
#include <set>
#include <vector>

#include "mem/ruby/common/DataBlock.hh"
#include "mem/ruby/protocol/chi/ep/BoundaryQueue.hh"
#include "mem/ruby/protocol/chi/ep/BoundarySlots.hh"
#include "mem/ruby/protocol/chi/ep/EPBackend.hh"
#include "mem/ruby/protocol/chi/ep/EPRNFController.hh"
#include "params/EPSNFController.hh"

namespace gem5
{

namespace ruby
{

class EPSNFController : public EPController
{
  public:
    PARAMS(EPSNFController);
    EPSNFController(const Params &p);

    void init() override;
    void wakeup() override;
    void print(std::ostream& out) const override;

    void selfTest();
    bool parkAcquisition(Addr line, uint64_t control,
                         std::function<void()> onParked);
    void resumeAcquisition(Addr line, uint64_t control);
    void nativeAcquireDone(Addr line, uint64_t incarnation);
    // Admission is checked for every socket before any Park is emitted.
    bool canParkAcquisitions(Addr line, uint64_t control) const;
    unsigned acquisitionCount(Addr line) const;

  protected:
    bool recvRequestMsg(const CHIRequestMsg *msg) override;
    bool recvSnoopMsg(const CHIRequestMsg *msg) override;
    bool recvResponseMsg(const CHIResponseMsg *msg) override;
    bool recvDataMsg(const CHIDataMsg *msg) override;

    EPBackend *_backend = nullptr;
    int _socketId = 0;  // v4-dual-socket
    bool _parkMicrotestIssued = false;

    // A WriteNoSnp grants the DBID before its data beats arrive.  In HA mode
    // this is also the store's permission context: the final line is assembled
    // here and is not published to memory until its own HA Write is granted.
    struct PendingWrite {
        BoundaryTransactions::Token boundaryToken;
        uint64_t dbid = 0;
        uint64_t storeCommitId = 0;
        uint64_t originalTxnId = 0;
        Addr linePa = 0;
        uint64_t expectedMask = 0;
        uint64_t receivedMask = 0;
        int sourceSocket = 0;
        uint8_t data[64]{};
        MachineID requestor;
        bool haWrite = false;
        bool internalPublication = false;
        uint64_t parentInvalidateReqId = 0;
        uint64_t parentInvalidateEpoch = 0;
        bool replacementOwnerRelease = false;
        int releaseRequester = -1;
        uint64_t releaseEpoch = 0;
        bool dataComplete = false;
        bool granted = false;
        bool completionQueued = false;
        uint64_t homePa = 0;
        int homeNode = -1;
        int homeSocket = -1;
        uint64_t permissionEpoch = 0;
        uint64_t permissionReqId = 0;
        int requesterNode = -1;
        UBWriteDisposition disposition = UBWriteDisposition::MemoryOnly;
    };
    BoundarySlots<uint64_t, PendingWrite,
                  BoundaryTransactions::OrdinaryCapacity> _pendingWrites;
    // Identity layout: node[63:60], socket[59], reserved[58:48],
    // controller version[47:32], monotonic sequence[31:0].
    uint64_t _nextWriteIdentity = 1;
    uint64_t allocateWriteIdentity();
    void processPendingHAWrites();
    void publishHAWrite(uint64_t transactionId, PendingWrite &pending);

    // Q3: Retry queue for blocked grants
    struct RetryEntry {
        BoundaryTransactions::Token foreground;
        uint64_t linePa;
        int neededPerm;
        bool writeIntent;
        int ingressSocket;
        MachineID hnReq;      // HN-F requestor (for CompData routing)
        MachineID fwdReq;     // fwdRequestor (if dataToFwdReq)
        bool dataToFwdReq;
        bool publishOnData;
        bool needsReadReceipt;
        uint64_t nativeTxnId;
        uint64_t nativeIncarnation = 0;
        uint64_t parkControl = 0;
        unsigned parkPhase = 0;
        bool nativeDataPublished = false;
        bool nativeDone = false;
        bool closeSent = false;
        bool microHeld = false;
        BoundaryBorrowToken authorityBorrow;
        std::function<void()> onParked;
    };
    BoundaryQueue<RetryEntry, BoundaryTransactions::OrdinaryCapacity> _retryQueue;

    // This is a response replay window, not storage for live obligations.
    // Entries reach it only after nativeDone AND the Resume acknowledgement.
    struct RetiredControl {
        Addr line = 0;
        uint64_t incarnation = 0;
        uint64_t control = 0;
        MachineID responder;
    };
    std::array<RetiredControl, BoundaryTransactions::OrdinaryCapacity>
        _retiredControls{};
    unsigned _retiredControlCursor = 0;
    unsigned _microOtherClosed = 0;
    std::function<void()> _microLatePark;
    void rememberRetiredControl(const RetryEntry &entry);

    // Q3: Deferred CompData sends (1-tick delay for TBE race fix)
    struct PendingDataOutput {
        std::shared_ptr<CHIDataMsg> msg;
        std::function<void()> onSent;
    };
    std::vector<PendingDataOutput> _deferredCompData;
    void processDeferredData();

    // Output backpressure must not drop CHI responses. Keep messages in FIFO
    // order until the corresponding MessageBuffer accepts them.
    struct PendingResponseOutput {
        std::shared_ptr<CHIResponseMsg> msg;
        std::function<void()> onSent;
    };
    std::deque<PendingResponseOutput> _pendingResponses;
    std::deque<PendingDataOutput> _pendingData;
    void sendResponseReliable(std::shared_ptr<CHIResponseMsg> msg,
                              std::function<void()> onSent = nullptr);
    void sendDataReliable(std::shared_ptr<CHIDataMsg> msg,
                          std::function<void()> onSent = nullptr);
    void processPendingOutputs();

    // ---- v4: Deferred Grant Entry (§4.4.2, §7.6) ----
    /**
     * v4: Deferred grant data pending send.  Used when CompData
     * must be deferred by at least 1 tick to satisfy TBE timing
     * invariant (I10).  Stores epoch/reqId for audit but does NOT
     * re-check epoch at send time (§4.4.2 item 2).
     */
    struct DeferredGrantEntry {
        uint64_t linePa;
        int homeNode;
        uint64_t epoch;
        uint64_t reqId;
        OuterGrantType grantType;
        bool sharedHint;          // true → CompData_SC with m_shared_hint
        DataBlock data;           // cache-line-sized data payload
    };
    std::vector<DeferredGrantEntry> _deferredGrants;
    void processDeferredGrants();

    // Phase 4: verbose diagnostic logging gate (I14)
    bool _verboseLog = false;

    // Bounded, cumulative proof markers for HA Write request/response/ack.
    uint64_t _haWriteReqCount = 0;
    uint64_t _haWriteRespCount = 0;
    uint64_t _haWriteAckCount = 0;
    uint64_t _haWriteAssembledCount = 0;
    uint64_t _haWritePublishCount = 0;
    static constexpr uint64_t kHAWriteTraceLimit = 256;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__
