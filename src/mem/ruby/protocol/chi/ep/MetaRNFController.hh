#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_METARNFCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_METARNFCONTROLLER_HH__

#include <cstdint>
#include <functional>

#include "params/MetaRNFController.hh"
#include "sim/eventq.hh"
#include "sim/sim_object.hh"

namespace gem5
{

namespace ruby
{

struct MetaBackstoreEntry {
    int state;
    uint64_t sharersMask;
    uint64_t epoch;
};

class MetaRNFController : public SimObject
{
  public:
    PARAMS(MetaRNFController);
    MetaRNFController(const Params &p);

    void issueRead(uint64_t linePa,
                   std::function<void(bool, const MetaBackstoreEntry&)> cb);
    void issueWrite(uint64_t linePa, const MetaBackstoreEntry &entry,
                    std::function<void()> cb);
    void issueDelete(uint64_t linePa, std::function<void(bool)> cb);

  private:
    Tick _readLatency;
    Tick _writeLatency;
    Tick _deleteLatency;
};

} // namespace ruby
} // namespace gem5

#endif
