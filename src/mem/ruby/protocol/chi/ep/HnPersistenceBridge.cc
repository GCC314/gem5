#include "mem/ruby/protocol/chi/ep/HnPersistenceBridge.hh"

#include "mem/ruby/protocol/chi/ep/EPBackend.hh"

namespace gem5::ruby
{

HnPersistenceBridge::HnPersistenceBridge(const Params &p)
  : SimObject(p), _backend(p.ep_backend)
{
}

void
HnPersistenceBridge::registerHnPersistence(
    Addr linePa, Addr txnId, int sourceSocket, bool replacement, bool fullLine)
{
    // Generic CHI configurations install a disabled bridge. Only the project
    // UBCC configuration binds an EPBackend and needs persistence metadata.
    if (!_backend)
        return;
    _backend->registerHnPersistence(
        linePa, txnId, sourceSocket, replacement, fullLine);
}

} // namespace gem5::ruby
