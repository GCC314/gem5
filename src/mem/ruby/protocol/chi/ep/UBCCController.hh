/*
 * Copyright (c) 2024 UBCC Project
 *
 * UBCC Controller: Global Coherence Home Agent
 *
 * Lightweight C++ class (not a SimObject) that manages the global
 * MESI directory for DSM lines. Instantiated and owned by the
 * EP-RNF controller for its home node.
 */

#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__

#include <cstdint>
#include <map>
#include <queue>
#include <vector>

#include "base/types.hh"

namespace gem5
{
namespace ruby
{

enum class GlobalState : uint8_t
{
    I = 0, S = 1, E = 2, M = 3, Busy = 4
};

enum class OuterMsgType : uint8_t
{
    GlobalReadShared,
    GlobalReadUnique,
    GlobalInvalidate,
    GlobalDowngrade,
    GlobalWriteback,
    GlobalEvict,
    GlobalDataResp,
    GlobalAck,
    GlobalRetry,
    GrantS,
    GrantE,
    GrantM,
};

struct GlobalDirEntry
{
    Addr lineAddr;
    GlobalState state;
    int ownerNode;
    uint64_t sharerMask;
    bool dirty;
    uint64_t epoch;

    bool isSharer(int nodeId) const {
        return (sharerMask & (1ULL << nodeId)) != 0;
    }
    void addSharer(int nodeId) { sharerMask |= (1ULL << nodeId); }
    void removeSharer(int nodeId) { sharerMask &= ~(1ULL << nodeId); }
    int sharerCount() const { return __builtin_popcountll(sharerMask); }
};

struct OuterMessage
{
    OuterMsgType type;
    Addr addr;
    int srcNode;
    int dstNode;
    GlobalState grantState;
    uint64_t epoch;
    bool done;

    OuterMessage() : type(OuterMsgType::GlobalAck), addr(0),
        srcNode(-1), dstNode(-1), grantState(GlobalState::I),
        epoch(0), done(false) {}
};

class OuterQueue
{
  public:
    void enqueueReq(const OuterMessage &msg, Tick latency);
    void enqueueResp(const OuterMessage &msg, Tick latency);
    bool hasPendingReq(Tick now) const;
    bool hasPendingResp(Tick now) const;
    OuterMessage dequeueReq(Tick now);
    OuterMessage dequeueResp(Tick now);
    std::vector<OuterMessage> drainReqs(Tick now);
    std::vector<OuterMessage> drainResps(Tick now);

  private:
    struct TimedMsg {
        OuterMessage msg;
        Tick readyTime;
    };
    std::queue<TimedMsg> reqQueue;
    std::queue<TimedMsg> respQueue;
};

class UBCCController
{
  public:
    UBCCController(int nodeId);

    void handleGlobalRequest(const OuterMessage &req);
    GlobalDirEntry* lookupLine(Addr addr);
    GlobalDirEntry& getOrCreateLine(Addr addr);
    void updateLine(Addr addr, GlobalState state, int owner, bool dirty);
    int getDirEntryCount() const { return directory.size(); }

    OuterQueue& getQueue() { return outerQueue; }
    void dumpDirectory() const;

  private:
    int nodeId;
    std::map<Addr, GlobalDirEntry> directory;
    OuterQueue outerQueue;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__
