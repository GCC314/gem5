/*
 * Copyright (c) 2024 UBCC Project
 *
 * EP-SNF Controller (skeleton)
 *
 * Based on CHIGenericController. Handles DSM Remote requests
 * (ReadNoSnp) from HN-F. Returns fake data for bring-up.
 */

#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__

#include "mem/ruby/protocol/chi/generic/CHIGenericController.hh"
#include "params/EPSNFController.hh"

namespace gem5
{

namespace ruby
{

class EPSNFController : public CHIGenericController
{
  public:
    PARAMS(EPSNFController);
    EPSNFController(const Params &p);

    void init() override;
    void wakeup() override;

    int getNodeId() const { return nodeId; }

  protected:
    bool recvRequestMsg(const CHIRequestMsg *msg) override;
    bool recvSnoopMsg(const CHIRequestMsg *msg) override;
    bool recvResponseMsg(const CHIResponseMsg *msg) override;
    bool recvDataMsg(const CHIDataMsg *msg) override;

  private:
    void handleReadNoSnp(const CHIRequestMsg *msg);
    void sendFakeDataResp(const CHIRequestMsg *req);

    const int nodeId;
    int readNoSnpReceived;
    int dataSentCount;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_EPSNFCONTROLLER_HH__
