#include "mem/ruby/protocol/chi/ep/UBCCController.hh"

#include <sstream>

#include "base/logging.hh"
#include "mem/ruby/protocol/chi/ep/SentinelHelper.hh"
#include "mem/ruby/system/RubySystem.hh"
#include "sim/cur_tick.hh"

namespace gem5
{

namespace ruby
{

UBCCController::UBCCController(int node_id, RubySystem *ruby_system)
  : _nodeId(node_id)
{
    if (ruby_system) {
        _sentinelHelper = new SentinelHelper(ruby_system, node_id);
    }
}

UBCCController::~UBCCController()
{
    delete _sentinelHelper;
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

// ---- M4 Sentinel Registration Test Hooks ----

bool
UBCCController::installSentinelForTest(uint64_t line_pa, bool as_owner)
{
    if (!_sentinelHelper) {
        warn("UBCCController node_id=%d: SentinelHelper not available\n",
             _nodeId);
        return false;
    }

    bool ok = _sentinelHelper->installSentinelForTest(line_pa, as_owner);
    if (ok) {
        _sentinelStates[line_pa] = as_owner ? SS_OWNER : SS_SHARER;
    }
    return ok;
}

bool
UBCCController::removeSentinelForTest(uint64_t line_pa)
{
    if (!_sentinelHelper) {
        warn("UBCCController node_id=%d: SentinelHelper not available\n",
             _nodeId);
        return false;
    }

    bool ok = _sentinelHelper->removeSentinelForTest(line_pa);
    if (ok) {
        _sentinelStates.erase(line_pa);
    }
    return ok;
}

std::string
UBCCController::inspectDirEntryForTest(uint64_t line_pa)
{
    DirEntrySnapshot snap;
    if (!getDirEntrySnapshot(line_pa, snap)) {
        return "{\"error\": \"entry not found\"}";
    }

    std::ostringstream oss;
    oss << "{"
        << "\"sharerCount\":" << snap.sharerCount << ","
        << "\"ownerExists\":" << (snap.ownerExists ? "true" : "false") << ","
        << "\"ownerIsExcl\":" << (snap.ownerIsExcl ? "true" : "false") << ","
        << "\"ownerVersion\":" << snap.ownerVersion << ","
        << "\"ownerStr\":\"" << snap.ownerStr << "\","
        << "\"state\":" << snap.state << ","
        << "\"epRnfInSharers\":" << (snap.epRnfInSharers ? "true" : "false") << ","
        << "\"epRnfIsOwner\":" << (snap.epRnfIsOwner ? "true" : "false") << ","
        << "\"epRnfLookupFailed\":" << (snap.epRnfLookupFailed ? "true" : "false")
        << "}";
    return oss.str();
}

bool
UBCCController::getDirEntrySnapshot(uint64_t line_pa, DirEntrySnapshot &snap)
{
    if (!_sentinelHelper) {
        warn("UBCCController node_id=%d: SentinelHelper not available\n",
             _nodeId);
        return false;
    }
    return _sentinelHelper->inspectDirEntryForTest(line_pa, snap);
}

bool
UBCCController::isDsmAddr(uint64_t pa) const
{
    if (!_sentinelHelper)
        return false;
    return _sentinelHelper->isDsmAddr(pa);
}

uint64_t
UBCCController::getEpRnfSnoopCount() const
{
    if (!_sentinelHelper)
        return 0;
    return _sentinelHelper->getEpRnfSnoopCount();
}

void
UBCCController::resetEpRnfSnoopCount()
{
    if (_sentinelHelper)
        _sentinelHelper->resetEpRnfSnoopCount();
}

void
UBCCController::incrementEpRnfSnoopCount()
{
    if (_sentinelHelper)
        _sentinelHelper->incrementEpRnfSnoopCount();
}

} // namespace ruby
} // namespace gem5
