#include "mem/ruby/protocol/chi/ep/EPBackend.hh"

#include "mem/ruby/protocol/chi/ep/UBCCController.hh"
#include "params/EPBackend.hh"

namespace gem5
{

namespace ruby
{

EPBackend::EPBackend(const Params &p)
  : SimObject(p),
    _nodeId(p.node_id)
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

} // namespace ruby
} // namespace gem5
