#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_METARNFCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_METARNFCONTROLLER_HH__

#include <array>
#include <cstdint>
#include <functional>
#include <map>

#include "mem/ruby/protocol/chi/ep/EPRNFController.hh"
#include "params/MetaRNFController.hh"

namespace gem5
{

namespace ruby
{

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

    void issueRead(uint64_t metadataPa, ReadCallback cb);
    void issueWrite(uint64_t metadataPa, const MetaLine &line, WriteCallback cb);
    void issueDelete(uint64_t metadataPa, WriteCallback cb);

  protected:
    bool recvRequestMsg(const CHIRequestMsg *msg) override;
    bool recvSnoopMsg(const CHIRequestMsg *msg) override;
    bool recvResponseMsg(const CHIResponseMsg *msg) override;
    bool recvDataMsg(const CHIDataMsg *msg) override;

  private:
    enum class OpType { Read, Write, Delete };

    struct PendingTxn {
        OpType op;
        uint64_t pa;
        DataBlock writeData;
        ReadCallback readCb;
        WriteCallback writeCb;
        bool waitingCompAfterDbid;

        PendingTxn()
            : op(OpType::Read), pa(0), writeData(64), waitingCompAfterDbid(false)
        {}
    };

    bool sendReadOnce(uint64_t pa);
    bool sendWriteUnique(uint64_t pa);
    bool sendWriteData(uint64_t pa, MachineID dst, uint64_t dbid);
    bool sendCompAck(uint64_t pa, MachineID dst);
    bool inMetadataRange(uint64_t pa) const;
    void completeRead(uint64_t pa, bool success, const DataBlock *data);
    void completeWrite(uint64_t pa, bool success);

  private:
    AddrRange _metadataRange;
    int _hnfVersion;
    bool _requestInFlight;
    std::map<uint64_t, PendingTxn> _pending;

    static std::map<std::pair<int,int>, MetaRNFController*> _instances;
};

} // namespace ruby
} // namespace gem5

#endif
