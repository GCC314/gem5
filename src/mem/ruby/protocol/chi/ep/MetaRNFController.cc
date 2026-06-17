#include "mem/ruby/protocol/chi/ep/MetaRNFController.hh"

namespace gem5
{

namespace ruby
{

MetaRNFController::MetaRNFController(const Params &p)
  : SimObject(p),
    _readLatency(p.read_latency_ticks),
    _writeLatency(p.write_latency_ticks),
    _deleteLatency(p.delete_latency_ticks)
{}

void
MetaRNFController::issueRead(
    uint64_t linePa, std::function<void(bool, const MetaBackstoreEntry&)> cb)
{
    auto *ev = new EventFunctionWrapper([cb]() {
        MetaBackstoreEntry dummy{0, 0, 0};
        cb(false, dummy);
    }, name() + ".metaRead");
    schedule(ev, curTick() + _readLatency + (linePa & 0x3));
}

void
MetaRNFController::issueWrite(
    uint64_t linePa, const MetaBackstoreEntry &entry, std::function<void()> cb)
{
    auto *ev = new EventFunctionWrapper([cb]() { cb(); }, name() + ".metaWrite");
    (void)entry;
    schedule(ev, curTick() + _writeLatency + (linePa & 0x3));
}

void
MetaRNFController::issueDelete(uint64_t linePa, std::function<void(bool)> cb)
{
    auto *ev = new EventFunctionWrapper([cb]() { cb(true); }, name() + ".metaDelete");
    schedule(ev, curTick() + _deleteLatency + (linePa & 0x3));
}

} // namespace ruby
} // namespace gem5
