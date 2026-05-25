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
#include <map>
#include <set>

namespace gem5
{

class ThreadContext;

/**
 * SyncWaitManager provides a cross-node barrier primitive for SE-mode
 * multi-threaded simulations.
 *
 * Each barrier instance is identified by a node_mask. The number of
 * bits set in node_mask determines the expected number of threads that
 * must reach the barrier before all are released.
 *
 * Barrier instances are isolated by node_mask: threads using different
 * masks do not block each other.
 *
 * Barriers are reusable: after a round completes (all threads released),
 * the barrier state resets so the next round starts clean.
 *
 * Only threads that explicitly call Sync_Wait are counted; threads that
 * do not call the syscall are ignored.
 */
class SyncWaitManager
{
  private:
    /** Per-barrier-instance state. */
    struct BarrierState
    {
        /** Target count: popcount of node_mask. */
        uint32_t target;
        /** Set of ThreadContext pointers that have arrived this round. */
        std::set<ThreadContext *> arrived;
        /** True while a round is active (gathering or just released). */
        bool gathering;
    };

    /** Map from node_mask to barrier state. */
    std::map<uint32_t, BarrierState> barriers;

    /**
     * Popcount helper: count number of set bits in a 32-bit integer.
     */
    static uint32_t popcount(uint32_t v);

  public:
    SyncWaitManager() = default;
    ~SyncWaitManager() = default;

    /**
     * Called when a thread invokes the Sync_Wait syscall.
     *
     * @param tc        The calling thread context.
     * @param node_mask The barrier identifier; popcount(mask) gives the
     *                  expected number of threads.
     *
     * If not all expected threads have arrived, the calling thread is
     * suspended. When the last expected thread arrives, all waiting
     * threads are activated.
     *
     * Duplicate calls by the same thread within the same round are
     * safely ignored.
     * After a round completes and a new round begins, the barrier
     * state is automatically reset.
     */
    void barrierWait(ThreadContext *tc, uint32_t node_mask);
};

} // namespace gem5

#endif // __SIM_SYNC_WAIT_HH__
