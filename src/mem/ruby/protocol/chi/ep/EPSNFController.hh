#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__

#include <deque>
#include <map>
#include <set>

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

    // Q2: Pending write tracking — maps address → HN-F requestor.
    std::map<Addr, MachineID> _pendingWrites;

    // Q3: Retry queue for blocked grants
    struct RetryEntry {
        uint64_t linePa;
        int neededPerm;
        bool writeIntent;
        MachineID hnReq;      // HN-F requestor (for CompData routing)
        MachineID fwdReq;     // fwdRequestor (if dataToFwdReq)
        bool dataToFwdReq;
    };
    std::deque<RetryEntry> _retryQueue;

    // Q3: Deferred CompData sends (1-tick delay for TBE race fix)
    std::vector<std::shared_ptr<CHIDataMsg>> _deferredCompData;
    void processDeferredData();
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__
