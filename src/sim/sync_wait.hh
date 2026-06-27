/*
 * Copyright (c) 2026
 * All rights reserved.
 *
 * Redistribution and use in source and binary forms, with or without
 * modification, are permitted provided that the following conditions are
 * met: redistributions of source code must retain the above copyright
 * notice, this list of conditions and the following disclaimer;
 * redistributions in binary form must reproduce the above copyright
 * notice, this list of conditions and the following disclaimer in the
 * documentation and/or other materials provided with the distribution;
 * neither the name of the copyright holders nor the names of its
 * contributors may be used to endorse or promote products derived from
 * this software without specific prior written permission.
 *
 * THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
 * "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
 * LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
 * A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
 * OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
 * SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
 * LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
 * DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
 * THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
 * (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
 * OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.
 */

#ifndef __SIM_SYNC_WAIT_HH__
#define __SIM_SYNC_WAIT_HH__

#include <cstdint>
#include <functional>
#include <map>
#include <set>
#include <vector>

namespace gem5
{

class ThreadContext;

class SyncWaitManager
{
  public:
    static constexpr uint32_t MAX_NODE_COUNT = 3;

    /** Callback: UBAdapter registers this to send BARRIER_REACHED via Port. */
    using BarrierSendFn = std::function<void(uint32_t mask, uint32_t nodeId)>;

  private:
    struct BarrierState
    {
        uint32_t activeThreads = 0;
        std::set<ThreadContext *> waiting;
    };

    std::map<uint32_t, BarrierState> _barriers;
    BarrierSendFn _sendFn;
    uint32_t _nodeId = 0;

  public:
    SyncWaitManager() = default;
    ~SyncWaitManager() = default;

    void setNodeId(uint32_t nid) { _nodeId = nid; }
    void setBarrierSendFn(BarrierSendFn fn) { _sendFn = std::move(fn); }

    /**
     * Called when a thread invokes Sync_Wait(mask, activeThreads).
     * activeThreads: total threads on THIS node that must arrive before
     *               BARRIER_REACHED is sent to BarrierManager.
     * If not all local threads arrived: suspend tc.
     * If all local arrived: send BARRIER_REACHED and suspend tc.
     */
    int barrierArrive(ThreadContext *tc, uint32_t mask, uint32_t activeThreads);

    /**
     * Called when BarrierManager sends BARRIER_RELEASE.
     * Wakes all waiting threads for this mask.
     */
    void barrierRelease(uint32_t mask);
};

} // namespace gem5

#endif // __SIM_SYNC_WAIT_HH__
