#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__

#include <cstdint>
#include <vector>

#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"
#include "params/EPBackend.hh"
#include "sim/sim_object.hh"

namespace gem5
{

namespace ruby
{

class UBCCController;

class EPBackend : public SimObject
{
  public:
    PARAMS(EPBackend);
    EPBackend(const Params &p);
    ~EPBackend();

    void init() override;
    void wakeup();

    int nodeId() const { return _nodeId; }

    bool checkAddr(uint64_t pa) const;
    bool checkDsmAddr(uint64_t pa) const;
    const NodeAddressMap& addrMap() const { return _addrMap; }

  private:
    const int _nodeId;
    NodeAddressMap _addrMap;
    UBCCController *_ubcc = nullptr;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__
