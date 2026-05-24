#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__

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
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__
