/*
 * Copyright (c) 2024 UBCC Project
 *
 * UBCC Controller Implementation
 */

#include "mem/ruby/protocol/chi/ep/UBCCController.hh"

#include <cstdio>

#include "sim/cur_tick.hh"

namespace gem5
{
namespace ruby
{

// === OuterQueue ===

void
OuterQueue::enqueueReq(const OuterMessage &msg, Tick latency)
{
    Tick ready = curTick() + latency;
    reqQueue.push({msg, ready});
}

void
OuterQueue::enqueueResp(const OuterMessage &msg, Tick latency)
{
    Tick ready = curTick() + latency;
    respQueue.push({msg, ready});
}

bool
OuterQueue::hasPendingReq(Tick now) const
{
    return !reqQueue.empty() && reqQueue.front().readyTime <= now;
}

bool
OuterQueue::hasPendingResp(Tick now) const
{
    return !respQueue.empty() && respQueue.front().readyTime <= now;
}

OuterMessage
OuterQueue::dequeueReq(Tick now)
{
    OuterMessage msg = reqQueue.front().msg;
    reqQueue.pop();
    return msg;
}

OuterMessage
OuterQueue::dequeueResp(Tick now)
{
    OuterMessage msg = respQueue.front().msg;
    respQueue.pop();
    return msg;
}

std::vector<OuterMessage>
OuterQueue::drainReqs(Tick now)
{
    std::vector<OuterMessage> msgs;
    while (hasPendingReq(now))
        msgs.push_back(dequeueReq(now));
    return msgs;
}

std::vector<OuterMessage>
OuterQueue::drainResps(Tick now)
{
    std::vector<OuterMessage> msgs;
    while (hasPendingResp(now))
        msgs.push_back(dequeueResp(now));
    return msgs;
}

// === UBCCController ===

UBCCController::UBCCController(int _nodeId)
    : nodeId(_nodeId)
{
}

void
UBCCController::handleGlobalRequest(const OuterMessage &req)
{
    switch (req.type) {
    case OuterMsgType::GlobalReadShared:
    case OuterMsgType::GlobalReadUnique:
    {
        GlobalDirEntry &entry = getOrCreateLine(req.addr);

        if (entry.state == GlobalState::I) {
            if (req.type == OuterMsgType::GlobalReadShared) {
                entry.state = GlobalState::S;
                entry.addSharer(req.srcNode);
            } else {
                entry.state = GlobalState::M;
                entry.ownerNode = req.srcNode;
                entry.dirty = false;
            }

            OuterMessage resp;
            resp.type = (req.type == OuterMsgType::GlobalReadShared)
                        ? OuterMsgType::GrantS : OuterMsgType::GrantM;
            resp.addr = req.addr;
            resp.srcNode = nodeId;
            resp.dstNode = req.srcNode;
            resp.epoch = entry.epoch;
            resp.done = true;
            outerQueue.enqueueResp(resp, 1);
        } else {
            OuterMessage resp;
            resp.type = OuterMsgType::GlobalRetry;
            resp.addr = req.addr;
            resp.srcNode = nodeId;
            resp.dstNode = req.srcNode;
            resp.done = false;
            outerQueue.enqueueResp(resp, 1);
        }
        break;
    }
    case OuterMsgType::GlobalWriteback:
    {
        auto *entry = lookupLine(req.addr);
        if (entry && entry->ownerNode == req.srcNode) {
            entry->state = GlobalState::I;
            entry->ownerNode = -1;
            entry->dirty = false;
        }
        break;
    }
    case OuterMsgType::GlobalEvict:
    {
        auto *entry = lookupLine(req.addr);
        if (entry) {
            entry->removeSharer(req.srcNode);
            if (entry->sharerCount() == 0)
                entry->state = GlobalState::I;
        }
        break;
    }
    default:
        break;
    }
}

GlobalDirEntry*
UBCCController::lookupLine(Addr addr)
{
    auto it = directory.find(addr);
    return (it != directory.end()) ? &it->second : nullptr;
}

GlobalDirEntry&
UBCCController::getOrCreateLine(Addr addr)
{
    auto it = directory.find(addr);
    if (it == directory.end()) {
        GlobalDirEntry entry;
        entry.lineAddr = addr;
        entry.state = GlobalState::I;
        entry.ownerNode = -1;
        entry.sharerMask = 0;
        entry.dirty = false;
        entry.epoch = 0;
        directory[addr] = entry;
        return directory[addr];
    }
    return it->second;
}

void
UBCCController::updateLine(Addr addr, GlobalState state, int owner, bool dirty)
{
    auto it = directory.find(addr);
    if (it != directory.end()) {
        it->second.state = state;
        it->second.ownerNode = owner;
        it->second.dirty = dirty;
    }
}

void
UBCCController::dumpDirectory() const
{
    printf("=== UBCC Directory (node %d) ===\n", nodeId);
    for (const auto &[addr, entry] : directory) {
        printf("  0x%lx: state=%d owner=%d sharers=0x%lx dirty=%d epoch=%lu\n",
               addr, (int)entry.state, entry.ownerNode,
               entry.sharerMask, entry.dirty, entry.epoch);
    }
}

} // namespace ruby
} // namespace gem5
