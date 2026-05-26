#include "mem/ruby/protocol/chi/ep/EPBackend.hh"

#include "base/logging.hh"
#include "mem/ruby/protocol/chi/ep/UBCCController.hh"
#include "mem/ruby/system/RubySystem.hh"
#include "params/EPBackend.hh"

namespace gem5
{

namespace ruby
{

EPBackend::EPBackend(const Params &p)
  : SimObject(p),
    _nodeId(p.node_id),
    _addrMap(3, 128ULL * 1024 * 1024)
{
    // Pass RubySystem to UBCCController for SentinelHelper init
    // RubySystem is available via the params
    auto *ruby_system = p.ruby_system;
    _ubcc = new UBCCController(_nodeId, ruby_system);
}

EPBackend::~EPBackend()
{
    delete _ubcc;
}

/**
 * Forward declare the M4 self-test entry point (defined in M4SelfTest.cc).
 */
void m4SelfTest_run(EPBackend*);


void
EPBackend::init()
{
    SimObject::init();

    // ---- M4 Sentinel Registration Self-Test ----
    // Runs during instantiation; results printed to stdout.
    // Python test harness parses the output.
    // Only one node (node 0) runs the self-test to avoid duplicate output.
    if (_nodeId == 0 && _ubcc) {
        m4SelfTest_run(this);
    }
}

void
EPBackend::wakeup()
{
    if (_ubcc)
        _ubcc->wakeup();
}

bool
EPBackend::checkAddr(uint64_t pa) const
{
    if (_addrMap.isDsm(_nodeId, pa)) {
        int h = _addrMap.homeNode(_nodeId, pa);
        if (h == _nodeId) {
            return true;
        } else {
            fatal("EPBackend node_id=%d: cross-node DSM access "
                  "PA=0x%lx src=%d home_node=%d",
                  _nodeId, pa, _addrMap.srcNodeId(pa), h);
        }
    }
    fatal("EPBackend node_id=%d: forbidden non-DSM access PA=0x%lx",
          _nodeId, pa);
    return false;
}

bool
EPBackend::checkDsmAddr(uint64_t pa) const
{
    if (_addrMap.isDsm(_nodeId, pa))
        return true;

    fatal("EPBackend node_id=%d: non-DSM address on EP path PA=0x%lx",
          _nodeId, pa);
    return false;
}

// ---- M4 Sentinel Registration Test Hooks ----

bool
EPBackend::installSentinelForTest(uint64_t line_pa, bool as_owner)
{
    if (!_ubcc) {
        warn("EPBackend node_id=%d: UBCC not available\n", _nodeId);
        return false;
    }
    return _ubcc->installSentinelForTest(line_pa, as_owner);
}

bool
EPBackend::removeSentinelForTest(uint64_t line_pa)
{
    if (!_ubcc) {
        warn("EPBackend node_id=%d: UBCC not available\n", _nodeId);
        return false;
    }
    return _ubcc->removeSentinelForTest(line_pa);
}

std::string
EPBackend::inspectDirEntryForTest(uint64_t line_pa)
{
    if (!_ubcc) {
        return "{\"error\": \"UBCC not available\"}";
    }
    return _ubcc->inspectDirEntryForTest(line_pa);
}

bool
EPBackend::isDsmAddr(uint64_t pa) const
{
    if (!_ubcc)
        return false;
    return _ubcc->isDsmAddr(pa);
}

uint64_t
EPBackend::getEpRnfSnoopCount() const
{
    if (!_ubcc)
        return 0;
    return _ubcc->getEpRnfSnoopCount();
}

void
EPBackend::resetEpRnfSnoopCount()
{
    if (_ubcc)
        _ubcc->resetEpRnfSnoopCount();
}

void
EPBackend::incrementEpRnfSnoopCount()
{
    if (_ubcc)
        _ubcc->incrementEpRnfSnoopCount();
}

} // namespace ruby
} // namespace gem5
