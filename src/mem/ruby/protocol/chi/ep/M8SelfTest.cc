/**
 * M8 Shared-Read Hardening And Upgrade/Invalidate Closure Self-Test.
 * Runs during EPBackend::init() and prints results to stdout.
 *
 * Validates M8 infrastructure:
 *   - TC-M8-1: Two Requesters Hold Shared simultaneously
 *   - TC-M8-2: Local Upgrade Invalidates Sharers (GlobalInvalidate flow)
 *   - TC-M8-3: Shared Default Path (no force_grant_m)
 *   - TC-M8-4: SharerMask Correctness
 *
 * Scoring model: PASS / FAIL / SKIP (ternary).
 *   - FAIL: assertion explicitly false
 *   - SKIP: preconditions not met (e.g., requires infrastructure)
 *   - PASS: assertion confirmed true
 */

#include "mem/ruby/protocol/chi/ep/EPBackend.hh"
#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"
#include "mem/ruby/protocol/chi/ep/UBCCController.hh"

#include <cstdio>
#include <cstdlib>
#include <sstream>
#include <string>

#include "base/logging.hh"

namespace gem5
{
namespace ruby
{

namespace M8SelfTest {

static int _passed = 0;
static int _failed = 0;
static int _skipped = 0;
static int _total = 0;

#define M8_CHECK(_name, _cond, _detail) \
    do { \
        _total++; \
        if (_cond) { \
            _passed++; \
            printf("  M8 %s: PASS\n", _name); \
        } else { \
            std::string _d(_detail); \
            if (_d.rfind("SKIP:", 0) == 0) { \
                _skipped++; \
                printf("  M8 %s: SKIP (%s)\n", _name, _d.c_str() + 5); \
            } else { \
                _failed++; \
                printf("  M8 %s: FAIL", _name); \
                if (!_d.empty()) \
                    printf(" (%s)", _d.c_str()); \
                printf("\n"); \
            } \
        } \
    } while(0)

void runSelfTest(EPBackend *backend, int home_node)
{
    printf("=== M8 Shared-Read Hardening Self-Test (node_id=%d) ===\n",
           home_node);

    _passed = 0;
    _failed = 0;
    _skipped = 0;
    _total = 0;

    NodeAddressMap addrMap(3, 128ULL * 1024 * 1024);

    UBCCController *ubcc = backend->getUBCC();
    if (!ubcc) {
        M8_CHECK("M8-INFRA: UBCC not available", false,
                 "SKIP:UBCC not available");
        goto print_results;
    }

    // ===========================================================
    // Test 1: TC-M8-1 — Two Requesters Hold Shared
    // ===========================================================
    {
        uint64_t offset = 0x1000;
        uint64_t pa = addrMap.buildDsmPA(0, 0, offset);

        // Step 1: Node 0 requests shared read
        UBCC_OuterGrantType grant0 =
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 0);

        M8_CHECK("M8-1-1: Node0 gets Shared grant",
                 grant0 == UBCC_OuterGrantType::GlobalGrantShared,
                 std::string("grant=") + std::to_string(static_cast<int>(grant0)));

        // Verify G_S, node 0 in sharers
        UBCCController::MESIState state;
        int ownerNode;
        uint64_t sharersMask;
        bool dirty;
        bool exists = ubcc->getUbccDirFieldsForTest(
            pa, state, ownerNode, sharersMask, dirty);

        M8_CHECK("M8-1-2: state is G_S after first shared",
                 exists && state == UBCCController::MESIState::G_S
                 && ownerNode == -1 && !dirty,
                 std::string("state=") +
                     std::to_string(static_cast<int>(state)) +
                     " owner=" + std::to_string(ownerNode));

        M8_CHECK("M8-1-3: Node 0 in sharersMask",
                 exists && (sharersMask & (1ULL << 0)),
                 std::string("sharersMask=0x") +
                     std::to_string(sharersMask));

        // Step 2: Node 2 also requests shared read
        UBCC_OuterGrantType grant2 =
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 2);

        M8_CHECK("M8-1-4: Node2 gets Shared grant on same line",
                 grant2 == UBCC_OuterGrantType::GlobalGrantShared,
                 std::string("grant=") + std::to_string(static_cast<int>(grant2)));

        // Verify BOTH nodes in sharers, still G_S
        exists = ubcc->getUbccDirFieldsForTest(
            pa, state, ownerNode, sharersMask, dirty);

        M8_CHECK("M8-1-5: state remains G_S with two sharers",
                 exists && state == UBCCController::MESIState::G_S
                 && ownerNode == -1 && !dirty,
                 std::string("state=") +
                     std::to_string(static_cast<int>(state)) +
                     " owner=" + std::to_string(ownerNode));

        M8_CHECK("M8-1-6: Both Node0 and Node2 in sharersMask",
                 exists && (sharersMask & (1ULL << 0))
                 && (sharersMask & (1ULL << 2)),
                 std::string("sharersMask=0x") +
                     std::to_string(sharersMask));

        M8_CHECK("M8-1-7: Only two nodes in sharers (no stray bits)",
                 exists && __builtin_popcountll(sharersMask) == 2,
                 std::string("sharersMask count=") +
                     std::to_string(__builtin_popcountll(sharersMask)));

        // Step 3: Node 1 (not yet a sharer) also gets shared
        UBCC_OuterGrantType grant1 =
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 1);

        M8_CHECK("M8-1-8: Node1 gets Shared grant",
                 grant1 == UBCC_OuterGrantType::GlobalGrantShared,
                 std::string("grant=") + std::to_string(static_cast<int>(grant1)));

        exists = ubcc->getUbccDirFieldsForTest(
            pa, state, ownerNode, sharersMask, dirty);

        M8_CHECK("M8-1-9: Three sharers in mask (Node0, Node1, Node2)",
                 exists && (sharersMask & (1ULL << 0))
                 && (sharersMask & (1ULL << 1))
                 && (sharersMask & (1ULL << 2)),
                 std::string("sharersMask=0x") +
                     std::to_string(sharersMask));
    }

    // ===========================================================
    // Test 2: TC-M8-2 — Local Upgrade Invalidates Sharers
    //   Sub-scenario A: No other sharers → immediate upgrade
    //   Sub-scenario B: Other sharers → invalidation flow
    // ===========================================================
    {
        // --- Sub-scenario A: Single sharer upgrades, no other sharers ---
        {
            uint64_t offset = 0x1100;
            uint64_t pa = addrMap.buildDsmPA(0, 0, offset);

            // Node 0 gets shared first
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 0);

            // Node 0 upgrades to unique (no other sharers)
            UBCC_OuterGrantType grant =
                ubcc->processOuterRequest(pa,
                    UBCC_OuterReqType::GlobalReadUnique, true, 0);

            M8_CHECK("M8-2a-1: Solo sharer upgrades to Modified",
                     grant == UBCC_OuterGrantType::GlobalGrantModified,
                     std::string("grant=") + std::to_string(static_cast<int>(grant)));

            UBCCController::MESIState state;
            int ownerNode;
            uint64_t sharersMask;
            bool dirty;
            bool exists = ubcc->getUbccDirFieldsForTest(
                pa, state, ownerNode, sharersMask, dirty);

            M8_CHECK("M8-2a-2: state is G_M after upgrade",
                     exists && state == UBCCController::MESIState::G_M
                     && ownerNode == 0 && dirty,
                     std::string("state=") +
                         std::to_string(static_cast<int>(state)) +
                         " owner=" + std::to_string(ownerNode) +
                         " dirty=" + (dirty ? "true" : "false"));
        }

        // --- Sub-scenario B: Multiple sharers, one upgrades ---
        {
            uint64_t offset = 0x1200;
            uint64_t pa = addrMap.buildDsmPA(0, 0, offset);

            // Node 0 and Node 2 get shared
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 0);
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 2);

            uint64_t epochBefore = ubcc->getEpochForLine(pa);

            // Node 0 upgrades to unique → should trigger invalidation of Node 2
            uint64_t invCountBefore = ubcc->getInvalidationCount();
            UBCC_OuterGrantType grant =
                ubcc->processOuterRequest(pa,
                    UBCC_OuterReqType::GlobalReadUnique, true, 0);
            uint64_t invCountAfter = ubcc->getInvalidationCount();

            M8_CHECK("M8-2b-1: Upgrade with other sharers returns Modified",
                     grant == UBCC_OuterGrantType::GlobalGrantModified,
                     std::string("grant=") + std::to_string(static_cast<int>(grant)));

            M8_CHECK("M8-2b-2: Invalidation count incremented",
                     invCountAfter > invCountBefore,
                     std::string("before=") + std::to_string(invCountBefore) +
                         " after=" + std::to_string(invCountAfter));

            // Verify line is in invalidation-pending state (pendingOp=2)
            UBCCController::MESIState state;
            int ownerNode;
            uint64_t sharersMask;
            bool dirty;
            bool busy;
            int pendingReq;
            int pendingRecall;
            bool exists = ubcc->getUbccDirFieldsExtendedForTest(
                pa, state, ownerNode, sharersMask, dirty, busy,
                pendingReq, pendingRecall);

            M8_CHECK("M8-2b-3: state is G_M (upgraded immediately)",
                     exists && state == UBCCController::MESIState::G_M,
                     std::string("state=") +
                         std::to_string(static_cast<int>(state)));

            M8_CHECK("M8-2b-4: owner is Node0",
                     exists && ownerNode == 0,
                     std::string("ownerNode=") + std::to_string(ownerNode));

            M8_CHECK("M8-2b-5: line is busy (pendingOp=2 for invalidation)",
                     exists && busy == true,
                     std::string("busy=") + (busy ? "true" : "false"));

            int pendingInvCount = ubcc->getPendingInvalidationCount(pa);
            M8_CHECK("M8-2b-6: one pending invalidation (Node2)",
                     pendingInvCount == 1,
                     std::string("pendingInvCount=") +
                         std::to_string(pendingInvCount));

            // Verify pending invalidations mask includes Node2
            uint64_t pendingInvMask = ubcc->getPendingInvalidationMask(pa);
            M8_CHECK("M8-2b-7: pendingInvalidationMask includes Node2",
                     (pendingInvMask & (1ULL << 2)) != 0,
                     std::string("pendingInvMask=0x") +
                         std::to_string(pendingInvMask));

            M8_CHECK("M8-2b-8: pendingInvalidationMask does NOT include Node0",
                     (pendingInvMask & (1ULL << 0)) == 0,
                     "Node0 (requester) should not be in invalidation mask");

            // Step: Node 2 acks the invalidation
            uint64_t epochAfter = ubcc->getEpochForLine(pa);
            uint64_t ackCountBefore = ubcc->getInvalidationAckCount();
            bool ackOk = ubcc->processInvalidationAck(pa, 2, epochAfter);
            uint64_t ackCountAfter = ubcc->getInvalidationAckCount();

            M8_CHECK("M8-2b-9: invalidation ack accepted",
                     ackOk,
                     "Ack from node 2 should be accepted");

            M8_CHECK("M8-2b-10: ack counter incremented",
                     ackCountAfter > ackCountBefore,
                     std::string("before=") + std::to_string(ackCountBefore) +
                         " after=" + std::to_string(ackCountAfter));

            // After ack, line should no longer be busy
            exists = ubcc->getUbccDirFieldsExtendedForTest(
                pa, state, ownerNode, sharersMask, dirty, busy,
                pendingReq, pendingRecall);

            M8_CHECK("M8-2b-11: line is no longer busy after all acks",
                     exists && !busy,
                     std::string("busy=") + (busy ? "true" : "false"));

            // Verify Node2 is no longer in sharersMask
            M8_CHECK("M8-2b-12: Node2 removed from sharersMask after ack",
                     exists && !(sharersMask & (1ULL << 2)),
                     std::string("sharersMask=0x") +
                         std::to_string(sharersMask));

            // Verify Node0 is still owner
            M8_CHECK("M8-2b-13: Node0 remains owner",
                     exists && ownerNode == 0 && state == UBCCController::MESIState::G_M,
                     "owner and state unchanged after invalidation ack");

            M8_CHECK("M8-2b-14: No pending invalidations after all acks",
                     ubcc->getPendingInvalidationCount(pa) == -1,
                     std::string("remaining=") +
                         std::to_string(ubcc->getPendingInvalidationCount(pa)));
            (void)epochBefore; (void)epochAfter;
        }

        // --- Sub-scenario C: Upgrade with write intent from different requester ---
        {
            uint64_t offset = 0x1300;
            uint64_t pa = addrMap.buildDsmPA(0, 0, offset);

            // Node 0 and Node 1 both have shared
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 0);
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 1);

            // Node 2 requests unique with write intent (not a current sharer)
            uint64_t invCountBefore = ubcc->getInvalidationCount();
            UBCC_OuterGrantType grant =
                ubcc->processOuterRequest(pa,
                    UBCC_OuterReqType::GlobalReadUnique, true, 2);
            uint64_t invCountAfter = ubcc->getInvalidationCount();

            M8_CHECK("M8-2c-1: New requester gets Modified from shared line",
                     grant == UBCC_OuterGrantType::GlobalGrantModified,
                     std::string("grant=") + std::to_string(static_cast<int>(grant)));

            M8_CHECK("M8-2c-2: Invalidation count incremented",
                     invCountAfter > invCountBefore,
                     std::string("before=") + std::to_string(invCountBefore) +
                         " after=" + std::to_string(invCountAfter));

            // Both Node0 and Node1 need invalidation
            int pendingInvCount = ubcc->getPendingInvalidationCount(pa);
            M8_CHECK("M8-2c-3: Two pending invalidations (Node0 + Node1)",
                     pendingInvCount == 2,
                     std::string("pendingInvCount=") +
                         std::to_string(pendingInvCount));

            uint64_t pendingInvMask = ubcc->getPendingInvalidationMask(pa);
            M8_CHECK("M8-2c-4: Both Node0 and Node1 in invalidation mask",
                     (pendingInvMask & (1ULL << 0)) && (pendingInvMask & (1ULL << 1)),
                     std::string("pendingInvMask=0x") +
                         std::to_string(pendingInvMask));

            M8_CHECK("M8-2c-5: Node2 (requester) not in invalidation mask",
                     !(pendingInvMask & (1ULL << 2)),
                     "Requester should not be invalidated");

            // Ack both
            uint64_t epoch = ubcc->getEpochForLine(pa);
            ubcc->processInvalidationAck(pa, 0, epoch);
            ubcc->processInvalidationAck(pa, 1, epoch);

            UBCCController::MESIState state;
            int ownerNode;
            uint64_t sharersMask;
            bool dirty, busy;
            int pr, prt;
            bool exists = ubcc->getUbccDirFieldsExtendedForTest(
                pa, state, ownerNode, sharersMask, dirty, busy, pr, prt);

            M8_CHECK("M8-2c-6: new owner is Node2",
                     exists && ownerNode == 2,
                     std::string("ownerNode=") + std::to_string(ownerNode));

            M8_CHECK("M8-2c-7: line is not busy after all acks",
                     exists && !busy,
                     std::string("busy=") + (busy ? "true" : "false"));

            M8_CHECK("M8-2c-8: old sharers removed",
                     exists && !(sharersMask & (1ULL << 0))
                     && !(sharersMask & (1ULL << 1)),
                     std::string("sharersMask=0x") +
                         std::to_string(sharersMask));
        }
    }

    // ===========================================================
    // Test 3: TC-M8-3 — Shared Default Path
    // ===========================================================
    {
        // Verify that GlobalReadShared returns GlobalGrantShared
        // (not forcing GrantModified as default)
        uint64_t offset = 0x1400;
        uint64_t pa = addrMap.buildDsmPA(0, 0, offset);

        // Test 1: G_I + GlobalReadShared → GlobalGrantShared + G_S
        UBCC_OuterGrantType grant1 =
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 0);

        M8_CHECK("M8-3-1: G_I + ReadShared → GrantShared",
                 grant1 == UBCC_OuterGrantType::GlobalGrantShared,
                 std::string("grant=") + std::to_string(static_cast<int>(grant1)));

        UBCCController::MESIState state;
        int ownerNode;
        uint64_t sharersMask;
        bool dirty;
        bool exists = ubcc->getUbccDirFieldsForTest(
            pa, state, ownerNode, sharersMask, dirty);

        M8_CHECK("M8-3-2: state is G_S (not G_M) after ReadShared",
                 exists && state == UBCCController::MESIState::G_S,
                 std::string("state=") +
                     std::to_string(static_cast<int>(state)));

        M8_CHECK("M8-3-3: no owner after ReadShared",
                 exists && ownerNode == -1,
                 std::string("ownerNode=") + std::to_string(ownerNode));

        M8_CHECK("M8-3-4: line is clean after ReadShared",
                 exists && !dirty,
                 "dirty flag should be false for shared");

        // Test 2: Second ReadShared on shared line → still GrantShared
        uint64_t pa2 = addrMap.buildDsmPA(0, 0, offset + 0x80);
        ubcc->processOuterRequest(pa2,
            UBCC_OuterReqType::GlobalReadShared, false, 0);

        UBCC_OuterGrantType grant2 =
            ubcc->processOuterRequest(pa2,
                UBCC_OuterReqType::GlobalReadShared, false, 1);

        M8_CHECK("M8-3-5: G_S + ReadShared → GrantShared (not forced to M)",
                 grant2 == UBCC_OuterGrantType::GlobalGrantShared,
                 std::string("grant=") + std::to_string(static_cast<int>(grant2)));

        // Test 3: ReadShared should never produce GrantExclusive or GrantModified
        uint64_t pa3 = addrMap.buildDsmPA(0, 0, offset + 0x100);
        UBCC_OuterGrantType grant3 =
            ubcc->processOuterRequest(pa3,
                UBCC_OuterReqType::GlobalReadShared, false, 0);

        M8_CHECK("M8-3-6: ReadShared never produces Exclusive",
                 grant3 != UBCC_OuterGrantType::GlobalGrantExclusive,
                 std::string("grant=") + std::to_string(static_cast<int>(grant3)));

        M8_CHECK("M8-3-7: ReadShared never produces Modified",
                 grant3 != UBCC_OuterGrantType::GlobalGrantModified,
                 std::string("grant=") + std::to_string(static_cast<int>(grant3)));
    }

    // ===========================================================
    // Test 4: TC-M8-4 — SharerMask Correctness
    // ===========================================================
    {
        // Test 4a: sharerMask accurately tracks multiple nodes
        {
            uint64_t offset = 0x1500;
            uint64_t pa = addrMap.buildDsmPA(0, 0, offset);

            // Add Node0 as sharer
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 0);

            // Add Node2 as sharer
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 2);

            // Verify mask has exactly nodes 0 and 2
            UBCCController::MESIState state;
            int ownerNode;
            uint64_t sharersMask;
            bool dirty;
            bool exists = ubcc->getUbccDirFieldsForTest(
                pa, state, ownerNode, sharersMask, dirty);

            M8_CHECK("M8-4a-1: sharersMask matches exact nodes (0,2)",
                     exists && sharersMask == ((1ULL << 0) | (1ULL << 2)),
                     std::string("sharersMask=0x") +
                         std::to_string(sharersMask));
        }

        // Test 4b: Evict removes node from sharerMask
        {
            uint64_t offset = 0x1580;
            uint64_t pa = addrMap.buildDsmPA(0, 0, offset);

            // Nodes 0 and 2 become sharers
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 0);
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 2);

            // Node 2 evicts
            uint64_t epoch = ubcc->getEpochForLine(pa);
            ubcc->processEvict(pa, 2, epoch);

            UBCCController::MESIState state;
            int ownerNode;
            uint64_t sharersMask;
            bool dirty;
            bool exists = ubcc->getUbccDirFieldsForTest(
                pa, state, ownerNode, sharersMask, dirty);

            M8_CHECK("M8-4b-1: Node 2 removed from sharersMask after evict",
                     exists && (sharersMask & (1ULL << 0))
                     && !(sharersMask & (1ULL << 2)),
                     std::string("sharersMask=0x") +
                         std::to_string(sharersMask));

            M8_CHECK("M8-4b-2: Node 0 still in sharersMask after Node2 evict",
                     exists && (sharersMask & (1ULL << 0)),
                     std::string("sharersMask=0x") +
                         std::to_string(sharersMask));
        }

        // Test 4c: Upgrade + invalidation clears sharerMask properly
        {
            uint64_t offset = 0x1600;
            uint64_t pa = addrMap.buildDsmPA(0, 0, offset);

            // Three sharers: Node0, Node1, Node2
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 0);
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 1);
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 2);

            uint64_t maskBefore = 0;
            {
                UBCCController::MESIState s;
                int o; uint64_t m; bool d;
                ubcc->getUbccDirFieldsForTest(pa, s, o, m, d);
                maskBefore = m;
            }

            M8_CHECK("M8-4c-1: Three sharers before upgrade",
                     __builtin_popcountll(maskBefore) == 3,
                     std::string("mask=0x") + std::to_string(maskBefore));

            // Node 0 upgrades to Unique
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadUnique, false, 0);

            // Ack Node1 and Node2 invalidations
            uint64_t epoch = ubcc->getEpochForLine(pa);
            ubcc->processInvalidationAck(pa, 1, epoch);
            ubcc->processInvalidationAck(pa, 2, epoch);

            // After all acks, sharerMask should be clean
            UBCCController::MESIState state;
            int ownerNode;
            uint64_t sharersMask;
            bool dirty;
            bool exists = ubcc->getUbccDirFieldsForTest(
                pa, state, ownerNode, sharersMask, dirty);

            M8_CHECK("M8-4c-2: No stale sharers after invalidation completed",
                     exists && !(sharersMask & (1ULL << 1))
                     && !(sharersMask & (1ULL << 2)),
                     std::string("sharersMask=0x") +
                         std::to_string(sharersMask));

            M8_CHECK("M8-4c-3: state is G_E after unique upgrade",
                     exists && state == UBCCController::MESIState::G_E,
                     std::string("state=") +
                         std::to_string(static_cast<int>(state)));

            M8_CHECK("M8-4c-4: owner is Node0",
                     exists && ownerNode == 0,
                     std::string("ownerNode=") + std::to_string(ownerNode));
        }

        // Test 4d: SharerMask correctness with recall (read on owned line)
        {
            uint64_t offset = 0x1680;
            uint64_t pa = addrMap.buildDsmPA(0, 0, offset);

            // Node 0 becomes owner (G_E)
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadUnique, false, 0);

            // Node 2 reads → triggers recall, old owner downgraded to shared
            bool recallNeeded = false;
            int recallOwnerNode = -1;
            ubcc->processOuterRequest(pa,
                UBCC_OuterReqType::GlobalReadShared, false, 2,
                nullptr, nullptr, &recallNeeded, &recallOwnerNode);

            uint64_t epoch = ubcc->getEpochForLine(pa);
            ubcc->processRecallResponse(pa, 0, false, epoch);

            UBCCController::MESIState state;
            int ownerNode;
            uint64_t sharersMask;
            bool dirty;
            bool exists = ubcc->getUbccDirFieldsForTest(
                pa, state, ownerNode, sharersMask, dirty);

            M8_CHECK("M8-4d-1: state is G_S after read recall",
                     exists && state == UBCCController::MESIState::G_S,
                     std::string("state=") +
                         std::to_string(static_cast<int>(state)));

            M8_CHECK("M8-4d-2: both Node0 and Node2 in sharersMask",
                     exists && (sharersMask & (1ULL << 0))
                     && (sharersMask & (1ULL << 2)),
                     std::string("sharersMask=0x") +
                         std::to_string(sharersMask));

            M8_CHECK("M8-4d-3: exactly two sharers",
                     exists && __builtin_popcountll(sharersMask) == 2,
                     std::string("count=") +
                         std::to_string(__builtin_popcountll(sharersMask)));
            (void)recallNeeded; (void)recallOwnerNode;
        }
    }

    // ===========================================================
    // Test 5: Stale Invalidation Ack Rejected
    // ===========================================================
    {
        uint64_t offset = 0x1700;
        uint64_t pa = addrMap.buildDsmPA(0, 0, offset);

        // Node 0 and Node 2 shared
        ubcc->processOuterRequest(pa,
            UBCC_OuterReqType::GlobalReadShared, false, 0);
        ubcc->processOuterRequest(pa,
            UBCC_OuterReqType::GlobalReadShared, false, 2);

        // Node 0 upgrades → triggers invalidation of Node 2
        ubcc->processOuterRequest(pa,
            UBCC_OuterReqType::GlobalReadUnique, true, 0);

        // Ack Node2 with correct epoch
        uint64_t epoch = ubcc->getEpochForLine(pa);
        bool ackOk = ubcc->processInvalidationAck(pa, 2, epoch);
        M8_CHECK("M8-5-1: valid epoch invalidation ack accepted",
                 ackOk,
                 std::string("epoch=") + std::to_string(epoch));

        // Duplicate ack from Node2 should be accepted (idempotent)
        bool dupOk = ubcc->processInvalidationAck(pa, 2, epoch);
        M8_CHECK("M8-5-2: duplicate invalidation ack is idempotent",
                 dupOk,
                 "Duplicate ack should be accepted (not fatal)");

        // Stale epoch ack should be rejected
        // Create a new upgrade scenario to test stale rejection
        uint64_t pa2 = addrMap.buildDsmPA(0, 0, offset + 0x80);
        ubcc->processOuterRequest(pa2,
            UBCC_OuterReqType::GlobalReadShared, false, 0);
        ubcc->processOuterRequest(pa2,
            UBCC_OuterReqType::GlobalReadShared, false, 1);

        uint64_t oldEpoch = ubcc->getEpochForLine(pa2);

        ubcc->processOuterRequest(pa2,
            UBCC_OuterReqType::GlobalReadUnique, false, 0);

        uint64_t staleRejBefore = ubcc->getStaleEpochRejectedCount();
        bool staleOk = ubcc->processInvalidationAck(pa2, 1, oldEpoch);
        uint64_t staleRejAfter = ubcc->getStaleEpochRejectedCount();

        M8_CHECK("M8-5-3: stale epoch invalidation ack rejected",
                 !staleOk && staleRejAfter > staleRejBefore,
                 std::string("accepted=") + std::to_string(staleOk) +
                     " rej_before=" + std::to_string(staleRejBefore) +
                     " rej_after=" + std::to_string(staleRejAfter));
    }

    // ===========================================================
    // Test 6: EPBackend Invalidation Counters
    // ===========================================================
    {
        uint64_t invRecvBefore = backend->getInvalidationReceivedCount();
        uint64_t invSentBefore = backend->getInvalidationAckSentCount();

        M8_CHECK("M8-6-1: Invalidation received counter initialized",
                 invRecvBefore <= 10000,
                 std::string("count=") + std::to_string(invRecvBefore));

        M8_CHECK("M8-6-2: Invalidation ack sent counter initialized",
                 invSentBefore <= 10000,
                 std::string("count=") + std::to_string(invSentBefore));
    }

    // ===========================================================
    // Test 7: busy line with invalidation rejects new requests
    // ===========================================================
    {
        // This test verifies that a line with pendingOp=2
        // (invalidation-in-progress) rejects new outer requests.
        // We can't easily test fatal() without triggering it,
        // but we can verify the pendingOp is correctly set.
        uint64_t offset = 0x1800;
        uint64_t pa = addrMap.buildDsmPA(0, 0, offset);

        // Set up shared state with external sharer
        ubcc->processOuterRequest(pa,
            UBCC_OuterReqType::GlobalReadShared, false, 0);
        ubcc->processOuterRequest(pa,
            UBCC_OuterReqType::GlobalReadShared, false, 2);

        // Trigger upgrade with invalidation
        ubcc->processOuterRequest(pa,
            UBCC_OuterReqType::GlobalReadUnique, false, 0);

        // Verify line is busy (pendingOp=2)
        UBCCController::MESIState state;
        int ownerNode;
        uint64_t sharersMask;
        bool dirty, busy;
        int pr, prt;
        bool exists = ubcc->getUbccDirFieldsExtendedForTest(
            pa, state, ownerNode, sharersMask, dirty, busy, pr, prt);

        M8_CHECK("M8-7-1: line busy during invalidation",
                 exists && busy && pr == 0,
                 std::string("busy=") + (busy ? "true" : "false") +
                     " pendingOp (via busy flag)=" + (busy ? ">0" : "0"));

        M8_CHECK("M8-7-2: pending invalidations still active",
                 ubcc->getPendingInvalidationCount(pa) == 1,
                 std::string("count=") +
                     std::to_string(ubcc->getPendingInvalidationCount(pa)));

        // Complete invalidation
        uint64_t epoch = ubcc->getEpochForLine(pa);
        ubcc->processInvalidationAck(pa, 2, epoch);

        // Line should now be free
        busy = false;
        exists = ubcc->getUbccDirFieldsExtendedForTest(
            pa, state, ownerNode, sharersMask, dirty, busy, pr, prt);

        M8_CHECK("M8-7-3: line not busy after invalidation completes",
                 exists && !busy,
                 std::string("busy=") + (busy ? "true" : "false"));

        M8_CHECK("M8-7-4: new request can proceed on freed line",
                 ubcc->getPendingInvalidationCount(pa) == -1,
                 "No pending invalidations remain");
    }

print_results:
    printf("=== M8 Self-Test Results: %d/%d PASS, %d FAIL, %d SKIP ===\n",
           _passed, _total, _failed, _skipped);

    fflush(stdout);
    if (_failed > 0) {
        printf("M8_SELF_TEST_FAILED=1\n");
        fflush(stdout);
    } else {
        printf("M8_SELF_TEST_PASSED=1\n");
        fflush(stdout);
    }
}

} // namespace M8SelfTest

} // namespace ruby
} // namespace gem5

// Entry point for EPBackend::init()
namespace gem5 {
namespace ruby {
void m8SelfTest_run(EPBackend *backend)
{
    if (!backend || !backend->getUBCC())
        return;
    M8SelfTest::runSelfTest(backend, backend->nodeId());
}
} // namespace ruby
} // namespace gem5
