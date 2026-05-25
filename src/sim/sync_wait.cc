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

#include "sim/sync_wait.hh"

#include <cerrno>

#include "cpu/thread_context.hh"

namespace gem5
{

uint32_t
SyncWaitManager::popcount(uint32_t v)
{
    // Use builtin popcount where available; otherwise fallback.
    v = v - ((v >> 1) & 0x55555555u);
    v = (v & 0x33333333u) + ((v >> 2) & 0x33333333u);
    v = (v + (v >> 4)) & 0x0F0F0F0Fu;
    v = v + (v >> 8);
    v = v + (v >> 16);
    return v & 0x3Fu;
}

int
SyncWaitManager::barrierWait(ThreadContext *tc, uint32_t node_mask)
{
    // ── Validation ──────────────────────────────────────────────
    // 1. node_mask must be non-zero.
    if (node_mask == 0) {
        return -EINVAL;
    }

    // 2. node_mask must not set bits beyond MAX_NODE_COUNT-1.
    //    Allowed mask bits: (1 << MAX_NODE_COUNT) - 1
    uint32_t max_valid_mask = (1u << MAX_NODE_COUNT) - 1u;
    if (node_mask & ~max_valid_mask) {
        return -EINVAL;
    }
    // ── End validation ──────────────────────────────────────────

    auto it = barriers.find(node_mask);

    // First encounter of this node_mask: create the barrier.
    if (it == barriers.end()) {
        BarrierState bs;
        bs.target = popcount(node_mask);
        bs.gathering = true;
        bs.arrived.clear();
        barriers[node_mask] = bs;
        it = barriers.find(node_mask);
    }

    BarrierState &bs = it->second;

    // If previous round completed and this is the first thread of a new
    // round, reset the barrier state.
    if (!bs.gathering) {
        bs.arrived.clear();
        bs.gathering = true;
    }

    // Duplicate call by the same thread within the same round: ignore.
    if (bs.arrived.find(tc) != bs.arrived.end()) {
        return 0;
    }

    // Register this thread.
    bs.arrived.insert(tc);

    if (bs.arrived.size() < bs.target) {
        // Not enough threads yet: suspend and wait.
        tc->suspend();
    } else {
        // All expected threads have arrived: wake everyone.
        // The current thread (last arriver) is not suspended.
        for (ThreadContext *t : bs.arrived) {
            if (t != tc) {
                t->activate();
            }
        }
        // Mark round as complete so next round will auto-reset.
        bs.gathering = false;
    }

    return 0;
}

} // namespace gem5
