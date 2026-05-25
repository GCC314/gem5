#include "mem/ruby/protocol/chi/ep/EPBackend.hh"

#include "base/logging.hh"
#include "mem/ruby/protocol/chi/ep/UBCCController.hh"
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
    _ubcc = new UBCCController(_nodeId);
}

EPBackend::~EPBackend()
{
    delete _ubcc;
}

void
EPBackend::init()
{
    SimObject::init();
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

} // namespace ruby
} // namespace gem5
