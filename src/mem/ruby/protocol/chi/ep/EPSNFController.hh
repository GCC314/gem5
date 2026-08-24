#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__

#include <cstring>
#include <deque>
#include <functional>
#include <map>
#include <set>
#include <vector>

#include "mem/ruby/common/DataBlock.hh"
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

  protected:
    bool recvRequestMsg(const CHIRequestMsg *msg) override;
    bool recvSnoopMsg(const CHIRequestMsg *msg) override;
    bool recvResponseMsg(const CHIResponseMsg *msg) override;
    bool recvDataMsg(const CHIDataMsg *msg) override;

    EPBackend *_backend = nullptr;
    int _socketId = 0;  // v4-dual-socket

    // A WriteNoSnp grants the DBID before its data beats arrive.  In HA mode
    // this is also the store's permission context: the final line is assembled
    // here and is not published to memory until its own HA Write is granted.
    struct PendingWrite {
        uint64_t expectedMask = 0;
        uint64_t receivedMask = 0;
        int sourceSocket = 0;
        uint8_t data[64]{};
        MachineID requestor;
        bool haWrite = false;
        bool dataComplete = false;
        bool granted = false;
        bool completionQueued = false;
        uint64_t homePa = 0;
        int homeNode = -1;
        int homeSocket = -1;
        uint64_t permissionEpoch = 0;
        uint64_t permissionReqId = 0;
    };
    std::map<Addr, PendingWrite> _pendingWrites;
    void processPendingHAWrites();
    void publishHAWrite(Addr linePa, PendingWrite &pending);

    // Q3: Retry queue for blocked grants
    struct RetryEntry {
        uint64_t linePa;
        int neededPerm;
        bool writeIntent;
        int ingressSocket;
        MachineID hnReq;      // HN-F requestor (for CompData routing)
        MachineID fwdReq;     // fwdRequestor (if dataToFwdReq)
        bool dataToFwdReq;
        bool publishOnData;
    };
    std::deque<RetryEntry> _retryQueue;

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

    // Pending writeback retry (Phase 2 async: metadata-resolution aware)
    struct PendingWriteback {
        uint64_t linePa;
        bool keepAsClean;
        int sourceSocket;
        uint8_t data[64];
        bool hasData;

        // Phase 2 async: QueryLineMeta state
        uint64_t queryReqId;       // stable QLM reqId (0 = no query sent yet)
        bool queryInFlight;        // true while QLM is outstanding
        uint64_t cachedEpoch;      // metadata from QLM response
        int cachedOwnerNode;
        bool cachedFound;

        // Phase 2 async: bounded retry / backoff
        int retryCount;
        Tick nextRetryTick;

        PendingWriteback()
            : linePa(0), keepAsClean(false), sourceSocket(0), hasData(false),
              queryReqId(0), queryInFlight(false),
              cachedEpoch(0), cachedOwnerNode(-1), cachedFound(false),
              retryCount(0), nextRetryTick(0)
        {
            std::memset(data, 0, sizeof(data));
        }
    };
    std::deque<PendingWriteback> _pendingWritebacks;
    void processPendingWritebacks();

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
