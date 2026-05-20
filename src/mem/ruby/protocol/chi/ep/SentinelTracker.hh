/*
 * Copyright (c) 2024 UBCC Project
 *
 * Sentinel Tracker for EP-RNF registration in HN-F directory.
 *
 * Maintains a per-node map of sentinel states for cache lines
 * that have external sharers or owners. Used by EP-RNF and
 * HN-F to coordinate global coherence.
 */

#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_SENTINEL_TRACKER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_SENTINEL_TRACKER_HH__

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "base/types.hh"

namespace gem5
{
namespace ruby
{

enum class SentinelState : uint8_t
{
    None = 0,
    ExternalSharer,   // External node has clean shared copy
    ExternalOwner,    // External node has exclusive/dirty ownership
};

struct SentinelEntry
{
    Addr addr;
    int nodeId;
    SentinelState state;
};

class SentinelTracker
{
  public:
    SentinelTracker() {}

    void insertSharer(Addr addr, int nodeId)
    {
        entries[addr] = {addr, nodeId, SentinelState::ExternalSharer};
    }

    void insertOwner(Addr addr, int nodeId)
    {
        entries[addr] = {addr, nodeId, SentinelState::ExternalOwner};
    }

    void remove(Addr addr)
    {
        entries.erase(addr);
    }

    void update(Addr addr, SentinelState newState)
    {
        auto it = entries.find(addr);
        if (it != entries.end()) {
            it->second.state = newState;
        }
    }

    SentinelState getState(Addr addr) const
    {
        auto it = entries.find(addr);
        if (it != entries.end()) {
            return it->second.state;
        }
        return SentinelState::None;
    }

    bool isPresent(Addr addr) const
    {
        return entries.find(addr) != entries.end();
    }

    bool isSharer(Addr addr) const
    {
        auto it = entries.find(addr);
        return it != entries.end() &&
               it->second.state == SentinelState::ExternalSharer;
    }

    bool isOwner(Addr addr) const
    {
        auto it = entries.find(addr);
        return it != entries.end() &&
               it->second.state == SentinelState::ExternalOwner;
    }

    int count() const { return entries.size(); }

    void clear() { entries.clear(); }

    void dump() const
    {
        for (const auto &[addr, entry] : entries) {
            printf("Sentinel: addr=0x%lx node=%d state=%d\n",
                   addr, entry.nodeId, static_cast<int>(entry.state));
        }
    }

  private:
    std::map<Addr, SentinelEntry> entries;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_SENTINEL_TRACKER_HH__
