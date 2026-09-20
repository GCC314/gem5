#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_METARNFCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_METARNFCONTROLLER_HH__

#include <array>
#include <cstdint>
#include <deque>
#include <functional>
#include <map>

#include "mem/ruby/protocol/chi/ep/EPRNFController.hh"
#include "mem/ruby/protocol/chi/ep/CoherenceMessage.hh"
#include "params/MetaRNFController.hh"

namespace gem5
{

namespace ruby
{

// Re-export the shared line status enum for convenience.
using MetaRNFLineStatus = cc::glob::MetaRNFLineStatus;

class MetaRNFController : public EPController
{
  public:
    PARAMS(MetaRNFController);
    MetaRNFController(const Params &p);
    ~MetaRNFController() override;

    static MetaRNFController* getInstance(int node_id, int socket_id = 0);

    void init() override;
    void wakeup() override;
    void print(std::ostream& out) const override;

    using MetaLine = std::array<uint8_t, 64>;
    using ReadCallback = std::function<void(bool, const MetaLine&)>;
    using WriteCallback = std::function<void(bool)>;

    // ---- Legacy 256B page operations (unchanged behavior) ----
    void issueRead(uint64_t metadataPa, ReadCallback cb);
    void issueWrite(uint64_t metadataPa, const MetaLine &line, WriteCallback cb);
    void issueDelete(uint64_t metadataPa, WriteCallback cb);

    // ---- Phase 2: 64B line operations with typed status ----
    using LineReadCallback  = std::function<void(MetaRNFLineStatus, const MetaLine&)>;
    using LineWriteCallback = std::function<void(MetaRNFLineStatus)>;

    void issueReadLine(uint64_t linePa, LineReadCallback cb);
    void issueWriteLine(uint64_t linePa, const MetaLine &line, LineWriteCallback cb);

    // ---- Multi-flight observability ----
    int activeFlightCount() const;
    int activeSlots() const { return activeFlightCount(); }
    int maxFlightSlots() const { return _maxFlights; }
    uint64_t metadataRangeStart() const { return _metadataRange.start(); }
    uint64_t metadataRangeEnd() const { return _metadataRange.end(); }

    // ---- Phase 2: bounded queue counters / limits (testable) ----
    static constexpr int kMaxLineOpsPerAddress = 8;
    static constexpr int kMaxPendingLineOps      = 128;
    static constexpr int kMaxLineFlightSlots     = 8;

    int  pendingLineOpsCount() const { return _pendingLineOps.size(); }
    int  pendingLineOpsHighwater() const { return _pendingLineOpsHighwater; }
    int  lineOpsRejected() const { return _lineOpsRejected; }
    int  lineRangeErrors() const { return _lineRangeErrors; }

  protected:
    bool recvRequestMsg(const CHIRequestMsg *msg) override;
    bool recvSnoopMsg(const CHIRequestMsg *msg) override;
    bool recvResponseMsg(const CHIResponseMsg *msg) override;
    bool recvDataMsg(const CHIDataMsg *msg) override;

  private:
    enum class OpType { Read, Write, Delete };

    enum class SlotState { Free, Allocated, Sent, Waiting, Done };

    struct FlightSlot {
        SlotState state = SlotState::Free;
        OpType op = OpType::Read;
        uint64_t pa = 0;
        DataBlock writeData{64};
        DataBlock readData{64};
        WriteMask readValid{64};
        ReadCallback readCb;
        WriteCallback writeCb;
        LineReadCallback  lineReadCb;
        LineWriteCallback lineWriteCb;
        bool isLineOp = false;
        bool waitingCompAfterDbid = false;

        void reset()
        {
            state = SlotState::Free;
            pa = 0;
            readValid.clear();
            readCb = nullptr;
            writeCb = nullptr;
            lineReadCb = nullptr;
            lineWriteCb = nullptr;
            isLineOp = false;
            waitingCompAfterDbid = false;
        }
    };

    struct QueuedOp {
        OpType op;
        uint64_t pa;
        DataBlock writeData{64};
        ReadCallback readCb;
        WriteCallback writeCb;
    };

    // Phase D7: pending write queue for slot exhaustion (legacy 256B path)
    struct PendingWrite {
        uint64_t pa;
        MetaLine data;
        WriteCallback cb;
    };
    static constexpr int kMaxPendingWrites = 64;
    std::deque<PendingWrite> _pendingWrites;
    int _pendingWritesHighwater = 0;

    // Phase 2: unified bounded pending line op queue
    struct PendingLineOp {
        uint64_t pa;
        MetaLine data;
        bool isWrite = false;   // true=write, false=read
        LineReadCallback  readCb;
        LineWriteCallback writeCb;
    };
    std::deque<PendingLineOp> _pendingLineOps;
    int _pendingLineOpsHighwater = 0;
    int _lineOpsRejected = 0;
    int _lineRangeErrors = 0;

    // Phase 2: per-address pending count for bounded wait queues
    std::map<uint64_t, int> _perAddressPendingCount;

    int findFreeSlot() const;
    void drainWaitQueue(uint64_t metadataPa);
    void drainPendingWrites();
    void drainPendingLineOps();

    bool sendReadOnce(uint64_t pa);
    bool sendWriteUnique(uint64_t pa);
    bool sendWriteData(uint64_t pa, MachineID dst, uint64_t dbid);
    bool sendCompAck(uint64_t pa, MachineID dst);
    bool inMetadataRange(uint64_t pa) const;

    // Legacy completion (unchanged)
    void completeRead(int slotIdx, bool success, const DataBlock *data);
    void completeWrite(int slotIdx, bool success);

    // Phase 2: typed line completion
    void completeReadLine(int slotIdx, MetaRNFLineStatus st, const DataBlock *data);
    void completeWriteLine(int slotIdx, MetaRNFLineStatus st);

    void completeDeferredReads();
    void completeDeferredWrites();

  private:
    AddrRange _metadataRange;
    int _hnfVersion;
    int _maxFlights;

    FlightSlot _flightSlots[8];
    std::map<uint64_t, int> _scoreboard;
    std::map<uint64_t, std::deque<QueuedOp>> _waitQueues;
    struct DeferredReadCompletion {
        int slot;
        Tick ready;
        DataBlock data;
    };
    std::deque<DeferredReadCompletion> _deferredReadCompletions;
    std::deque<std::pair<int, Tick>> _deferredWriteCompletions;

    static std::map<std::pair<int,int>, MetaRNFController*> _instances;
};

} // namespace ruby
} // namespace gem5

#endif
