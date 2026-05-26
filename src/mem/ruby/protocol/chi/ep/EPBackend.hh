#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__

#include <cstdint>
#include <string>
#include <vector>

#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"
#include "params/EPBackend.hh"
#include "sim/sim_object.hh"

namespace gem5
{

namespace ruby
{

class UBCCController;
class RubySystem;

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

    // ---- M4 Sentinel Registration Test Hooks ----
    // These are exposed to Python via gem5's Swig/SWIG bindings.

    bool installSentinelForTest(uint64_t line_pa, bool as_owner);
    bool removeSentinelForTest(uint64_t line_pa);
    std::string inspectDirEntryForTest(uint64_t line_pa);
    bool isDsmAddr(uint64_t pa) const;

    /** EP_RNF snoop counter for test verification */
    uint64_t getEpRnfSnoopCount() const;
    void resetEpRnfSnoopCount();
    void incrementEpRnfSnoopCount();

    /** Access to UBCCController for Python inspection */
    UBCCController* getUBCC() const { return _ubcc; }

  private:
    const int _nodeId;
    NodeAddressMap _addrMap;
    UBCCController *_ubcc = nullptr;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__
