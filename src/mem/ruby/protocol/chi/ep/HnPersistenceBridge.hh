#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_HNPERSISTENCEBRIDGE_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_HNPERSISTENCEBRIDGE_HH__

#include "params/HnPersistenceBridge.hh"
#include "sim/sim_object.hh"

namespace gem5::ruby
{

class EPBackend;

class HnPersistenceBridge : public SimObject
{
  public:
    PARAMS(HnPersistenceBridge);
    HnPersistenceBridge(const Params &p);

    void registerHnPersistence(Addr linePa, Addr txnId, int sourceSocket,
                               bool replacement, bool fullLine);
  private:
    EPBackend *_backend;
};

} // namespace gem5::ruby

#endif
