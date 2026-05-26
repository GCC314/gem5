#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_EPRNFCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_EPRNFCONTROLLER_HH__

#include <iostream>

#include "mem/ruby/network/MessageBuffer.hh"
#include "mem/ruby/protocol/AccessPermission.hh"
#include "mem/ruby/protocol/chi/ep/EPBackend.hh"
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

  protected:
    bool recvRequestMsg(const CHIRequestMsg *msg) override;
    bool recvSnoopMsg(const CHIRequestMsg *msg) override;
    bool recvResponseMsg(const CHIResponseMsg *msg) override;
    bool recvDataMsg(const CHIDataMsg *msg) override;

    EPBackend *_backend = nullptr;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_EPRNFCONTROLLER_HH__
