#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_EPRNFCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_EPRNFCONTROLLER_HH__

#include <deque>
#include <functional>
#include <iostream>
#include <map>
#include <queue>
#include <vector>

#include "mem/ruby/protocol/CHI/EpProxyOp.hh"
#include "mem/ruby/network/MessageBuffer.hh"
#include "mem/ruby/protocol/AccessPermission.hh"
#include "mem/ruby/protocol/chi/ep/EPBackend.hh"
#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"
#include "mem/ruby/slicc_interface/AbstractController.hh"
#include "mem/ruby/system/CacheRecorder.hh"
#include "mem/ruby/system/RubySystem.hh"
#include "params/EPController.hh"
#include "params/EPRNFController.hh"
#include "params/EPSNFController.hh"

#include "mem/ruby/protocol/CHI/CHIDataMsg.hh"
#include "mem/ruby/protocol/CHI/CHIRequestMsg.hh"
#include "mem/ruby/protocol/CHI/CHIResponseMsg.hh"

namespace gem5
{

namespace ruby
{

class EPController : public AbstractController
{
  public:
    PARAMS(EPController);
    EPController(const Params &p);

    void init() override;

    MessageBuffer *getMandatoryQueue() const override { return nullptr; }
    MessageBuffer *getMemReqQueue() const override { return nullptr; }
    MessageBuffer *getMemRespQueue() const override { return nullptr; }
    void initNetQueues() override;

    void print(std::ostream& out) const override;
    void wakeup() override;
    void resetStats() override;
    void regStats() override;
    void collateStats() override;

    void recordCacheTrace(int cntrl, CacheRecorder* tr) override;
    Sequencer* getCPUSequencer() const override { return nullptr; }
    DMASequencer* getDMASequencer() const override { return nullptr; }
    GPUCoalescer* getGPUCoalescer() const override { return nullptr; }

    void addSequencer(RubyPort* seq);

    bool functionalReadBuffers(PacketPtr&) override;
    bool functionalReadBuffers(PacketPtr&, WriteMask&) override;
    int functionalWriteBuffers(PacketPtr&) override;

    AccessPermission getAccessPermission(const Addr& param_addr) override;

    void functionalRead(const Addr& param_addr, Packet* param_pkt,
                        WriteMask& param_mask) override;
    int functionalWrite(const Addr& param_addr, Packet* param_pkt) override;

    int nodeId() const { return _nodeId; }

    /** EP_RNF snoop counter for test verification (M4) */
    uint64_t snoopCount() const { return _snoopCount; }
    void resetSnoopCount() { _snoopCount = 0; }

  protected:
    MessageBuffer* const reqOut;
    MessageBuffer* const snpOut;
    MessageBuffer* const rspOut;
    MessageBuffer* const datOut;
    MessageBuffer* const reqIn;
    MessageBuffer* const snpIn;
    MessageBuffer* const rspIn;
    MessageBuffer* const datIn;

    std::vector<RubyPort*> sequencers;

  public:
    const int cacheLineSize;
    const int cacheLineBits;
    const int dataChannelSize;
    const int dataMsgsPerLine;

    enum CHIChannel
    {
        CHI_REQ = 0,
        CHI_SNP = 1,
        CHI_RSP = 2,
        CHI_DAT = 3
    };

    using CHIRequestMsg = CHI::CHIRequestMsg;
    using CHIResponseMsg = CHI::CHIResponseMsg;
    using CHIDataMsg = CHI::CHIDataMsg;
    using CHIRequestMsgPtr = std::shared_ptr<CHIRequestMsg>;
    using CHIResponseMsgPtr = std::shared_ptr<CHIResponseMsg>;
    using CHIDataMsgPtr = std::shared_ptr<CHIDataMsg>;

    bool sendRequestMsg(CHIRequestMsgPtr msg) {
        return sendMessage(msg, reqOut);
    }
    bool sendSnoopMsg(CHIRequestMsgPtr msg) {
        return sendMessage(msg, snpOut);
    }
    bool sendResponseMsg(CHIResponseMsgPtr msg) {
        return sendMessage(msg, rspOut);
    }
    bool sendDataMsg(CHIDataMsgPtr msg) {
        return sendMessage(msg, datOut);
    }

  protected:
    const int _nodeId;
    uint64_t _snoopCount;

    virtual bool recvRequestMsg(const CHIRequestMsg *msg) = 0;
    virtual bool recvSnoopMsg(const CHIRequestMsg *msg) = 0;
    virtual bool recvResponseMsg(const CHIResponseMsg *msg) = 0;
    virtual bool recvDataMsg(const CHIDataMsg *msg) = 0;

  private:
    template<typename MsgType>
    bool receiveAllRdyMessages(MessageBuffer *buffer,
                        const std::function<bool(const MsgType*)> &callback)
    {
        bool pending = false;
        Tick cur_tick = curTick();
        while (buffer->isReady(cur_tick)) {
            const MsgType *msg =
                dynamic_cast<const MsgType*>(buffer->peek());
            assert(msg);
            if (callback(msg))
                buffer->dequeue(cur_tick);
            else {
                pending = true;
                break;
            }
        }
        return pending;
    }

    template<typename MessageType>
    bool sendMessage(MessageType &msg, MessageBuffer *buffer)
    {
        Tick cur_tick = curTick();
        if (buffer->areNSlotsAvailable(1, cur_tick)) {
            buffer->enqueue(msg, curTick(), cyclesToTicks(Cycles(1)),
                m_ruby_system->getRandomization(),
                m_ruby_system->getWarmupEnabled());
            return true;
        } else {
            return false;
        }
    }
};

class EPRNFController : public EPController
{
  public:
    PARAMS(EPRNFController);
    EPRNFController(const Params &p);

    void init() override;
    void wakeup() override;
    void print(std::ostream& out) const override;

    void selfTest();

    /** M4: get EPBackend for test hook access from Python */
    EPBackend* getBackend() const { return _backend; }

    // ---- M6: Delayed HN Response Management ----
    /**
     * Pending HN response context: when EP_RNF receives a snoop from HN
     * but must wait for outer txn completion, the response is queued here.
     */
    struct PendingHnResponse {
        bool valid;                         // True if there's a pending response
        uint64_t linePa;                    // Address of the snooped line
        CHI::CHIResponseType respType;      // Response type to send
        MachineID destMachine;              // Destination (HN that sent the snoop)
        Tick snoopTick;                     // Tick when snoop was received
        bool outerTxnComplete;             // True when outer txn is done

        PendingHnResponse() : valid(false), linePa(0),
            respType(CHI::CHIResponseType_SnpResp_I),
            snoopTick(0), outerTxnComplete(false) {}
    };

    /**
     * Check if there's a pending HN response for a given line.
     */
    bool hasPendingHnResponse(uint64_t linePa) const;

    /**
     * Signal that the outer transaction for a line has completed.
     * This triggers sending the delayed HN response.
     */
    void signalOuterTxnComplete(uint64_t linePa);

    /**
     * Get the pending HN response count (for test observation).
     */
    int getPendingHnResponseCount() const { return _pendingHnResponseCount; }
    void resetPendingHnResponseCount() { _pendingHnResponseCount = 0; }

    /**
     * Get count of delayed HN responses that have been sent (resolve count).
     */
    int getDelayedResponseResolvedCount() const { return _delayedResolvedCount; }
    void resetDelayedResponseResolvedCount() { _delayedResolvedCount = 0; }

    /**
     * M6: Access the outer txn pending flag for test verification.
     */
    bool isOuterTxnPending(uint64_t linePa) const;

    /**
     * M6: Mark a line as having an outer transaction in progress.
     * Used by EPBackend to signal EP_RNF about in-flight recall.
     */
    void setOuterTxnPending(uint64_t linePa, bool pending);

    /** v4: Called by EPBackend when OuterUpgradeAck(true) is received.
     *  Triggers the deferred SnpResp_I to HN-F. */
    void receiveUpgradeAck(uint64_t linePa);

    /** Drive a held SnpCleanInvalid-upgrade to completion. Idempotent: a
     *  no-op if no held upgrade exists for this line or it already completed.
     *  Called inline at first arrival and event-wise from
     *  EPBackend::onUpgradeRespArrived() when the OuterUpgradeResp arrives. */
    void completeHeldUpgrade(uint64_t linePa, bool dropRecoveryResend = false);

    /** Check whether a SnpCleanInvalid-upgrade is currently held (pending
     *  or not-yet-resolved) for this line. Used by EPBackend to decide whether
     *  to defer an incoming InvalidateReq until the held snoop is resolved. */
    bool hasHeldUpgrade(uint64_t linePa) const {
        auto it = _upgradePending.find(linePa);
        return it != _upgradePending.end() && it->second.valid &&
               !it->second.ackReceived;
    }

    /** Check whether a held upgrade for this line was rejected by the home.
     *  Used by EPBackend to handle an incoming InvalidateReq by ack'ing
     *  directly (no startCleanUnique) — the upgrade is being abandoned. */
    bool isHeldUpgradeRejected(uint64_t linePa) const {
        auto it = _upgradePending.find(linePa);
        return it != _upgradePending.end() && it->second.valid &&
               it->second.rejected;
    }

    /** Clear a held (rejected) upgrade for this line — called after the
     *  deferred InvalidateReq has been ack'd directly. */
    void clearHeldUpgrade(uint64_t linePa) {
        auto it = _upgradePending.find(linePa);
        if (it != _upgradePending.end())
            _upgradePending.erase(it);
    }

    /** Mark a rejected held upgrade for retry after its InvalidateAck has been
     *  sent (so the other requester's upgrade can drain). Resets the rejected
     *  flag and clears the old pending txn so the retry issues a fresh
     *  OuterUpgradeReq with a new reqId. Schedules a delayed wakeup. */
    void scheduleUpgradeRetryAfterRejection(uint64_t linePa);

    /** Get the HN-F destination recorded for a held snoop (to send SnpResp_I
     *  when the upgrade is abandoned). */
    MachineID getHeldUpgradeHnfDest(uint64_t linePa) const {
        auto it = _upgradePending.find(linePa);
        if (it != _upgradePending.end())
            return it->second.hnfDest;
        return MachineID();
    }

    /** Send a SnpResp_I to the given HN-F destination for this line.
     *  @param staleMark If true, set the response's `stale` flag. Used only for
     *  the EP-RNF upgrade-abandon path (TC16 dual-upgrade race loser): tells the
     *  local HN-F that this held CleanUnique must complete as STALE (the global
     *  upgrade was rejected by home). The HN-F then removes the requestor from
     *  dir_sharers and sends Comp_UC(stale=1); the L2 requestor detects stale
     *  and re-issues a fresh ReadUnique (I->M) to recall the winner's data.
     *  Default false preserves all existing (non-abandon) SnpResp_I callers
     *  byte-for-byte. */
    void sendSnpRespI(uint64_t linePa, MachineID hnfDest, bool staleMark = false);

    // ---- Q2 (deprecated): removed — RN-F should not send snoops ----
    // Snoops are HN-F's responsibility per CHI spec.  Recall/invalidation
    // must go through proper CHI Request path: EP-RNF → HN-F → HN-F handles snooping.

    // ---- v4: EP-RNF proxy operation types (scheme §4.3.2, §7.4) ----
    /** CHI operation types that EP-RNF can issue to HN-F. */
    enum class PendingChiOp { ReadShared, CleanUnique, ReadUnique };

    // ---- Q3: CHI Request-Based Coherence to HN-F ----
    /**
     * Pending CHI transaction context (§7.4).  Tracks an in-flight request
     * sent to HN-F (ReadShared for recall, CleanUnique for invalidation,
     * ReadUnique for write recall).  When the HN-F response (CompData or
     * Comp_UC) arrives, the callback is invoked and the transaction is
     * removed from the map.
     */
    struct PendingChiTxn {
        uint64_t linePa;
        uint64_t epoch;          // v4: outer epoch for this transaction
        uint64_t reqId;          // v4: outer reqId for this transaction
        PendingChiOp op;         // v4: operation type (replaces 'type')
        CHI::EpProxyOp proxyOp;       // v4: proxy op for special completion
        MachineID hnfDest;       // HN-F MachineID to send CompAck to
        int beatsExpected;       // v4: total data beats expected
        int beatsReceived;       // v4: data beats received so far
        bool needsCompAck;       // CompAck not yet successfully sent
        bool outerTxnPending;    // v4: outer (UBCC) transaction in progress
        bool readUniqueDataComplete; // P2-R8: last data beat received
        bool readUniqueCompUCSeen;   // P2-R8: Comp_UC completion token seen
        // ---- Per-PA 1-entry snoop slot (§4.3.3) ----
        bool snoopSlotValid;     // v4: queued snoop in 1-entry slot
        CHI::CHIRequestType queuedSnoopType; // v4: type of queued snoop
        bool queuedRetToSrc;     // v4: retToSrc for queued snoop
        Tick startTick;
        std::function<void(bool)> onComplete;
        // F2: Recall data capture from CHI data beats
        DataBlock recallDataBlk;
        bool recallDataValid;

        PendingChiTxn()
            : linePa(0), epoch(0), reqId(0),
              op(PendingChiOp::ReadShared),
              proxyOp(CHI::EpProxyOp_NoProxyOp),
              beatsExpected(0), beatsReceived(0),
              needsCompAck(false), outerTxnPending(false),
              readUniqueDataComplete(false), readUniqueCompUCSeen(false),
              snoopSlotValid(false),
              queuedSnoopType(CHI::CHIRequestType_null),
              queuedRetToSrc(false), startTick(0),
              recallDataBlk(64), recallDataValid(false) {}
    };

    // ---- v4: Retry queue entry (§4.3.4, §7.5) ----
    /**
     * Retry entry for deferred outbound CHI requests.
     * When a new outer request arrives while a CHI txn is in-flight,
     * the strongest op is preserved.  Stale epochs are discarded.
     *
     * Ordering: ReadUnique > CleanUnique > ReadShared
     */
    struct RetryEntry {
        uint64_t linePa;
        uint64_t epoch;
        uint64_t reqId;
        PendingChiOp strongestOp; // strongest op preserved across retries
    };

    /**
     * v4: Initiate a ReadUnique to the local HN-F for write recall (§4.3.2).
     * HN-F sends SnpUnique to invalidate the old owner, collects dirty data
     * if present, and returns CompData + Comp_UC to EP-RNF.
     * Special completion: scrub_to_I (does NOT retain ownership).
     *
     * @param linePa     Physical address in local PA view
     * @param onComplete Called when the CHI transaction completes
     */
    void startReadUnique(uint64_t linePa,
                         std::function<void(bool)> onComplete);

    /**
     * Initiate a ReadShared to the local HN-F for read recall (§4.3.2).
     * HN-F processes with full owner-tracking: sends SnpShared to
     * downgrade UD→SC and collect data.
     */
    void startReadShared(uint64_t linePa,
                         std::function<void(bool)> onComplete);

    /**
     * Initiate a CleanUnique to the local HN-F for sharer invalidation
     * (§4.3.2). HN-F sends SnpCleanInvalid to sharers, returns Comp_UC
     * as completion token.  Special completion: scrub_to_I.
     *
     * @param linePa     Physical address in local PA view
     * @param onComplete Called when the CHI transaction completes
     */
    void startCleanUnique(uint64_t linePa,
                          std::function<void(bool)> onComplete);

    // ---- v4: Helper ----
    /**
     * v4: Return the EpProxyOp for a given PendingChiOp (§4.3.2).
     * Maps PendingChiOp → EpProxyOp for special completion routing.
     * ReadShared → NoProxyOp, CleanUnique → InvalidateOnly,
     * ReadUnique → RecallUnique.
     */
    static CHI::EpProxyOp getEpProxyOp(PendingChiOp op);

  protected:
    bool recvRequestMsg(const CHIRequestMsg *msg) override;
    bool recvSnoopMsg(const CHIRequestMsg *msg) override;
    bool recvResponseMsg(const CHIResponseMsg *msg) override;
    bool recvDataMsg(const CHIDataMsg *msg) override;

    EPBackend *_backend = nullptr;

  private:
    // ---- Q3: CHI Request to HN-F ----
    /** Send a CHI request (ReadShared/CleanUnique/ReadUnique) to HN-F via reqOut.
     *  @return true if the message was enqueued successfully. */
    bool sendChiRequest(uint64_t linePa, CHI::CHIRequestType reqType,
                        CHI::EpProxyOp proxyOp = CHI::EpProxyOp_NoProxyOp);

    // v4-dual-socket: PA → socket → HN-F routing helpers (§3.3)
    int decodeHomeSocket(uint64_t linePa) const {
        int s = _addrMap.homeSocket(_nodeId, linePa);
        if (s < 0 || s >= _numSockets) return 0;
        return s;
    }
    MachineID selectHnfDestination(uint64_t linePa) const {
        return _downstreamBySocket[decodeHomeSocket(linePa)];
    }

    /** Send CompAck to HN-F via rspOut after receiving a response. */
    void sendCompAck(uint64_t linePa, MachineID dest);

    /** Per-cacheline pending CHI transaction tracking. */
    std::map<uint64_t, PendingChiTxn> _pendingChiTxns;

    /** Retry sending CompAck for pending CHI transactions
     *  whose CompAck couldn't be sent due to rspOut full. */
    void retryPendingCompAcks();

    // ---- v4: Snoop Dispatch & Queue (§4.3.3) ----
    /** Process a snoop message immediately (no in-flight CHI txn). */
    bool processSnoopImmediate(const CHIRequestMsg *msg);

    /** Handle SnpCleanInvalid: non-upgrade immediate SnpResp_I;
     *  upgrade path OuterUpgradeReq→wait→SnpResp_I (§4.3.3, §5.5). */
    bool handleSnpCleanInvalid(const CHIRequestMsg *msg);

    /** Handle SnpUnique: globalInvalidate → SnpResp_I / SnpRespData_I(_PD)
     *  (§4.3.3, §4.6.3). */
    bool handleSnpUnique(const CHIRequestMsg *msg);

    /** Handle SnpOnce: remoteFetch → SnpRespData_SC (§4.3.3). */
    bool handleSnpOnce(const CHIRequestMsg *msg);

    /** Send SnpResp_I to HN-F (common helper). */
    bool sendSnpRespI(const CHIRequestMsg *msg);

    /** v4: Send SnpResp_SC to HN-F (preserving response for SnpShared). */
    bool sendSnpRespSC(const CHIRequestMsg *msg);

    /** Send SnpRespData_SC to HN-F (SnpOnce response). */
    bool sendSnpRespDataSC(const CHIRequestMsg *msg);

    /** v4: Process the queued snoop after current CHI txn completes.
     *  Queued snoop has higher priority than deferred CHI requests. */
    void processQueuedSnoop(uint64_t linePa);

    /** v4: Complete a PendingChiTxn — invoke callback, clean up,
     *  then process queued snoop or deferred CHI requests. */
    void finishChiTxn(uint64_t linePa, bool success);

    // ---- v4: Retry Queue (§4.3.4) ----
    /**
     * v4: Enqueue or merge a retry entry with strongest-op ordering.
     * Stale epochs are discarded.  Strongest op wins across retries.
     */
    void enqueueRetry(uint64_t linePa, uint64_t epoch, uint64_t reqId,
                      PendingChiOp op);

    /** v4: Process retry queue entries after CHI txn completes. */
    void processRetryQueue();

    /** v4: Per-PA retry entries with strongest-op ordering. */
    std::map<uint64_t, RetryEntry> _retryEntries;

    // ---- v4: Upgrade Path (§5.5) ----
    /**
     * v4: Upgrade pending context.  Tracks PA that is undergoing
     * local upgrade via OuterUpgradeReq/Ack handshake.
     * SnpResp_I is deferred until OuterUpgradeAck(true) arrives.
     */
    struct UpgradePending {
        bool valid;
        uint64_t linePa;
        int homeNode;
        uint64_t epoch;
        uint64_t reqId;
        MachineID hnfDest;      // HN-F that sent SnpCleanInvalid
        bool ackReceived;       // true when OuterUpgradeAck(true) arrived
        bool rejected;          // true when home rejected: give up upgrade, but
                                // keep snoop held until the line is invalidated
                                // via a deferred InvalidateReq (direct ack, no
                                // startCleanUnique to avoid HN-F TBE collision)
        bool needsRetry;        // true when rejected + InvalidateAck sent:
                                // re-issue a fresh upgrade once the home drains
        int retryCount;         // exponential backoff: increment on each retry
                                // so the retry interval doubles each attempt
        bool dropWatchdogArmed; // true once a DROP/NO-RESP watchdog timer has
                                // been scheduled for this held (pending) upgrade
        int dropResendCount;    // number of DROP-recovery resends issued so far
                                // (bounded to avoid infinite resend storms)

        UpgradePending() : valid(false), linePa(0), homeNode(-1), epoch(0),
                           reqId(0), ackReceived(false), rejected(false),
                           needsRetry(false), retryCount(0),
                           dropWatchdogArmed(false), dropResendCount(0) {}
    };
    std::map<uint64_t, UpgradePending> _upgradePending;

    // Lines whose upgrade was rejected by the home (another upgrade
    // outstanding) and need a delayed retry. The snoop stays held; the retry
    // re-issues a fresh OuterUpgradeReq once the other upgrade drains.
    std::set<uint64_t> _upgradeRetryLines;
    void scheduleUpgradeRetry(uint64_t linePa);
    void processUpgradeRetries();

    /** Count of Cache-type controllers (for reference). */
    int _numCacheControllers;

    // v4-dual-socket: per-socket HN-F version & destination arrays (§3.3)
    int _numSockets;
    NodeAddressMap _addrMap;
    std::vector<int> _hnfVersions;               // index == socket_id
    std::vector<MachineID> _downstreamBySocket;  // index == socket_id

    // ---- Q3: Serialization of CHI requests to HN-F ----
    // Prevents multiple CHI requests being sent to HN-F in the same
    // event-processing cycle, which can cause TBE reservation exhaustion
    // and trigger `decrementReserved(): m_reserved > 0` assertion.
    // v4-dual-socket: _chiRequestInFlight remains GLOBAL — no per-socket parallelism.
    bool _chiRequestInFlight;
    // Queue of deferred CHI requests waiting for the current one to complete
    struct DeferredChiRequest {
        uint64_t linePa;
        CHI::CHIRequestType reqType;
        CHI::EpProxyOp proxyOp;  // v4: proxy op for deferred request
        Tick startTick;
    };
    std::deque<DeferredChiRequest> _deferredChiReqs;
    void processDeferredChiReqs();

    // ---- M6: Pending HN response tracking ----
    // Map from line PA to pending HN response context.
    std::map<uint64_t, PendingHnResponse> _pendingHnResponses;

    // ---- M6: Outer txn pending tracking ----
    // Map from line PA to outer-txn-in-progress flag.
    // When true, EP_RNF must not send final HN response.
    std::map<uint64_t, bool> _outerTxnPending;

    // ---- M6: Counters for test verification ----
    int _pendingHnResponseCount;
    int _delayedResolvedCount;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_EPRNFCONTROLLER_HH__
