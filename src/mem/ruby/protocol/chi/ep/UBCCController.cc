#include "mem/ruby/protocol/chi/ep/UBCCController.hh"

#include "base/logging.hh"
#include "sim/cur_tick.hh"

namespace gem5
{

namespace ruby
{

UBCCController::UBCCController(int node_id)
  : _nodeId(node_id)
{
}

UBCCController::~UBCCController()
{
}

void
UBCCController::wakeup()
{
    const Tick cur_tick = curTick();

    while (!_outerQueue.empty()) {
        const auto &entry = _outerQueue.front();
        if (entry.tick + entry.latency <= cur_tick) {
            _outerQueue.pop();
        } else {
            break;
        }
    }
}

} // namespace ruby
} // namespace gem5
