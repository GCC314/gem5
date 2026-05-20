/*
 * Copyright (c) 2024 UBCC Project
 *
 * EP-RNF Sentinel Controller (skeleton)
 *
 * Based on CHIGenericController. Represents external request nodes
 * in the local HN-F directory. Receives snoop messages from HN-F
 * and returns fixed legal responses.
 */

#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_EPRNFCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_EPRNFCONTROLLER_HH__

#include "mem/ruby/protocol/chi/generic/CHIGenericController.hh"
#include "params/EPRNFController.hh"

namespace gem5
{

namespace ruby
{

class EPRNFController : public CHIGenericController
{
  public:
    PARAMS(EPRNFController);
    EPRNFController(const Params &p);

    void init() override;
    void wakeup() override;

    int getNodeId() const { return nodeId; }

  protected:
    bool recvRequestMsg(const CHIRequestMsg *msg) override;
    bool recvSnoopMsg(const CHIRequestMsg *msg) override;
    bool recvResponseMsg(const CHIResponseMsg *msg) override;
    bool recvDataMsg(const CHIDataMsg *msg) override;

  private:
    void sendSnoopResp(const CHIRequestMsg *snoop);

    const int nodeId;
    int snoopsReceived;
    int responsesSent;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_EPRNFCONTROLLER_HH__
