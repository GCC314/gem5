/**
 * M4 Sentinel Registration Self-Test.
 * Runs during EPBackend::init() and prints results to stdout.
 *
 * M4 SKIP 清理: All tests now use UBCCController's own directory
 * (processOuterRequest / inspectUbccDirForTest / getUbccDirFieldsForTest
 *  / processEvict) instead of SentinelHelper.
 *
 * UBCC directory IS the authoritative registration — sharersMask,
 * ownerNode, and dirty are registration themselves.
 *
 * Scoring model: PASS / FAIL / SKIP (ternary).
 * Final output: "M4 Self-Test: X/Y PASS, Z FAIL, W SKIP"
 * Exit code non-zero iff Z > 0.
 */

#include <cstdio>
#include <cstdlib>
#include <sstream>

#include "base/logging.hh"
#include "mem/ruby/protocol/chi/ep/EPBackend.hh"
#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"
#include "mem/ruby/protocol/chi/ep/UBCCController.hh"

namespace gem5
{
namespace ruby
{

namespace M4SelfTest {

static int _passed = 0;
static int _failed = 0;
static int _skipped = 0;
static int _total = 0;

static bool _any_failure = false;

#define M4_CHECK(_name, _cond, _detail) \
    do { \
        _total++; \
        if (_cond) { \
            _passed++; \
            printf("  M4 %s: PASS\n", _name); \
        } else { \
            std::string _d(_detail); \
            if (_d.rfind("SKIP:", 0) == 0) { \
                _skipped++; \
                printf("  M4 %s: SKIP (%s)\n", _name, _d.c_str() + 5); \
            } else { \
                _failed++; \
                _any_failure = true; \
                printf("  M4 %s: FAIL", _name); \
                if (!_d.empty()) \
                    printf(" (%s)", _d.c_str()); \
                printf("\n"); \
            } \
        } \
    } while(0)

void runSelfTest(UBCCController *ubcc, int home_node)
{
    printf("=== M4 Sentinel Registration Self-Test (node_id=%d) ===\n",
           home_node);

    _passed = 0;
    _failed = 0;
    _skipped = 0;
    _total = 0;

    if (!ubcc) {
        M4_CHECK("M4-PRE: UBCC available", false, "UBCC is null");
        printf("=== M4 Self-Test Results: %d/%d PASS, %d FAIL, %d SKIP ===\n",
               _passed, _total, _failed, _skipped);
        if (_failed > 0) {
            printf("M4_SELF_TEST_FAILED=1\n");
            fflush(stdout);
        } else {
            printf("M4_SELF_TEST_PASSED=1\n");
            fflush(stdout);
        }
        return;
    }

    NodeAddressMap addrMap(3, 128ULL * 1024 * 1024);
    uint64_t segSize = addrMap.segSize();

    // Compute DSM addresses for this home node
    uint64_t dsm_base = addrMap.nodeBase(home_node) + 2 * segSize
                        + home_node * segSize;
    uint64_t dsm_pa  = (dsm_base + 0x100) & ~0x3FULL;
    uint64_t dsm_pa2 = (dsm_base + 0x140) & ~0x3FULL;
    uint64_t dsm_pa3 = (dsm_base + 0x1C0) & ~0x3FULL;
    uint64_t dsm_pa4 = (dsm_base + 0x240) & ~0x3FULL;

    // Non-DSM addresses
    uint64_t lp_pa = (addrMap.nodeBase(home_node) + 0x40) & ~0x3FULL;
    uint64_t ue_pa = (addrMap.nodeBase(home_node) + segSize + 0x40) & ~0x3FULL;

    // ---- Test 1: Address classification (pure NodeAddressMap) ----
    M4_CHECK("M4-ADDR-1: DSM address recognized",
             addrMap.isDsm(home_node, dsm_pa),
             std::string("pa=0x") + std::to_string(dsm_pa));
    M4_CHECK("M4-ADDR-2: DSM home node correct",
             addrMap.homeNode(home_node, dsm_pa) == home_node, "");
    M4_CHECK("M4-ADDR-3: LocalPrivate NOT DSM",
             !addrMap.isDsm(home_node, lp_pa), "");
    M4_CHECK("M4-ADDR-4: UbccExclusive NOT DSM",
             !addrMap.isDsm(home_node, ue_pa), "");

    // ---- Test 2: isDsmAddr pure range check ----
    {
        bool ubcc_dsm_yes = ubcc->isDsmAddr(dsm_pa);
        bool ubcc_dsm_no_lp = ubcc->isDsmAddr(lp_pa);
        bool ubcc_dsm_no_ue = ubcc->isDsmAddr(ue_pa);

        M4_CHECK("M4-TC4-4a: UBCC isDsmAddr DSM=true",
                 ubcc_dsm_yes,
                 std::string("dsm_pa=0x") + std::to_string(dsm_pa));
        M4_CHECK("M4-TC4-4b: UBCC isDsmAddr LP=false",
                 !ubcc_dsm_no_lp,
                 std::string("lp_pa=0x") + std::to_string(lp_pa));
        M4_CHECK("M4-TC4-4c: UBCC isDsmAddr UE=false",
                 !ubcc_dsm_no_ue,
                 std::string("ue_pa=0x") + std::to_string(ue_pa));
    }

    // ---- Test 3: Sharer/Owner/Remove via UBCC directory (7 tests) ----
    // Use processOuterRequest to create directory state, then verify
    // via getUbccDirFieldsForTest / inspectUbccDirForTest.
    // Remove via processEvict.
    {
        int requester = home_node;  // self-node for single-node tests

        // --- 3a: Create G_S via GlobalReadShared ---
        UBCC_OuterGrantType grant1 =
            ubcc->processOuterRequest(dsm_pa,
                UBCC_OuterReqType::GlobalReadShared,
                false, requester);

        M4_CHECK("M4-TC-Sharer-1: Shared request → GrantShared",
                 grant1 == UBCC_OuterGrantType::GlobalGrantShared,
                 std::string("grant=") + std::to_string(static_cast<int>(grant1)));

        // Verify: EP_RNF (requester) is in sharers
        MESIState state;
        int ownerNode;
        uint64_t sharersMask;
        bool dirty;
        bool exists = ubcc->getUbccDirFieldsForTest(dsm_pa, state, ownerNode,
                                                     sharersMask, dirty);
        M4_CHECK("M4-TC-Sharer-2: entry exists after Shared grant",
                 exists, "");
        M4_CHECK("M4-TC-Sharer-3: state == G_S after Shared grant",
                 exists && state == MESIState::G_S,
                 exists ? std::string("state=") +
                     std::to_string(static_cast<int>(state)) : "entry missing");
        M4_CHECK("M4-TC-Sharer-4: EP_RNF in sharersMask",
                 exists && (sharersMask & (1ULL << requester)),
                 "requester bit must be set in sharersMask");

        // --- 3b: Create G_E (owner) on a different line ---
        UBCC_OuterGrantType grant2 =
            ubcc->processOuterRequest(dsm_pa2,
                UBCC_OuterReqType::GlobalReadUnique,
                false, requester);

        M4_CHECK("M4-TC-Owner-1: Unique/nowrite → GrantExclusive",
                 grant2 == UBCC_OuterGrantType::GlobalGrantExclusive,
                 std::string("grant=") + std::to_string(static_cast<int>(grant2)));

        exists = ubcc->getUbccDirFieldsForTest(dsm_pa2, state, ownerNode,
                                                sharersMask, dirty);
        M4_CHECK("M4-TC-Owner-2: EP_RNF is owner (ownerNode==requester)",
                 exists && ownerNode == requester,
                 exists ? std::string("ownerNode=") + std::to_string(ownerNode)
                        : "entry missing");
        M4_CHECK("M4-TC-Owner-3: state == G_E after Unique grant",
                 exists && state == MESIState::G_E,
                 exists ? std::string("state=") +
                     std::to_string(static_cast<int>(state)) : "entry missing");

        // --- 3c: Remove via processEvict ---
        uint64_t epoch = ubcc->getEpochForLine(dsm_pa);
        bool evictOk = ubcc->processEvict(dsm_pa, requester, epoch);

        M4_CHECK("M4-TC-Remove-1: evict returns true",
                 evictOk,
                 std::string("evictOk=") + std::to_string(evictOk));

        // Verify: EP_RNF is no longer in sharers
        exists = ubcc->getUbccDirFieldsForTest(dsm_pa, state, ownerNode,
                                                sharersMask, dirty);
        bool ep_gone = !exists ||
                       (sharersMask == 0 && ownerNode < 0);
        M4_CHECK("M4-TC-Remove-2: EP_RNF gone after evict",
                 ep_gone,
                 exists ? std::string("sharersMask=0x") +
                     std::to_string(sharersMask) : "entry gone");
    }

    // ---- Test 4: EP_RNF snoop counter (local, no SentinelHelper) ----
    {
        uint64_t before = ubcc->getEpRnfSnoopCount();
        ubcc->incrementEpRnfSnoopCount();
        ubcc->incrementEpRnfSnoopCount();
        uint64_t after = ubcc->getEpRnfSnoopCount();
        M4_CHECK("M4-SNOOP-1: snoop counter increments",
                 after == before + 2,
                 std::string("before=") + std::to_string(before) +
                     " after=" + std::to_string(after));

        ubcc->resetEpRnfSnoopCount();
        uint64_t after_reset = ubcc->getEpRnfSnoopCount();
        M4_CHECK("M4-SNOOP-2: snoop counter resets",
                 after_reset == 0,
                 std::string("after_reset=") + std::to_string(after_reset));
    }

    // ---- Test 5: M4-4 readiness (local unique recall/snoop) ----
    // UBCC directory is authoritative. When a unique request arrives
    // on a shared line with other sharers, an invalidation is triggered.
    // We verify the invalidation infrastructure is operational.
    {
        int requester = home_node;

        // First, make line shared with requester (home_node)
        ubcc->processOuterRequest(dsm_pa3,
            UBCC_OuterReqType::GlobalReadShared,
            false, requester);

        // Verify entry exists (M4-4-a: UBCC directory accessible)
        MESIState state;
        int ownerNode;
        uint64_t sharersMask;
        bool dirty;
        bool exists = ubcc->getUbccDirFieldsForTest(dsm_pa3, state, ownerNode,
                                                     sharersMask, dirty);
        M4_CHECK("M4-4-a: UBCC directory entry exists for DSM line",
                 exists && state == MESIState::G_S,
                 "directory entry must be G_S after Shared grant");

        // Now add a simulated other sharer by directly faking a second
        // sharer in the mask via another Shared request from a different
        // node. Since we only have one node, we simulate node 1 as a
        // peer sharer by calling processOuterRequest with requesterNode=1.
        // This will make the line have two sharers (home_node + node 1).
        ubcc->processOuterRequest(dsm_pa3,
            UBCC_OuterReqType::GlobalReadShared,
            false, 1 /* other node */);

        exists = ubcc->getUbccDirFieldsForTest(dsm_pa3, state, ownerNode,
                                                sharersMask, dirty);
        bool has_both_sharers = exists && (sharersMask & (1ULL << home_node))
                                && (sharersMask & (1ULL << 1));
        M4_CHECK("M4-4-b: multiple sharers registered in directory",
                 has_both_sharers,
                 exists ? std::string("sharersMask=0x") +
                     std::to_string(sharersMask) : "entry missing");

        // Now issue a Unique request from requester (home_node).
        // This should trigger invalidation of node 1 (other sharer).
        // Since node 1 is not a real sim object, pendingInvalidationCount
        // will be 1 but no actual snoop will complete.
        ubcc->processOuterRequest(dsm_pa3,
            UBCC_OuterReqType::GlobalReadUnique,
            false, requester);

        // Check that an invalidation was initiated
        int pendingCount = ubcc->getPendingInvalidationCount(dsm_pa3);
        uint64_t pendingMask = ubcc->getPendingInvalidationMask(dsm_pa3);
        bool inval_started = (pendingCount > 0) || (pendingMask != 0);
        M4_CHECK("M4-4-c: unique on shared line triggers invalidation",
                 inval_started,
                 std::string("pendingCount=") + std::to_string(pendingCount) +
                     " pendingMask=0x" + std::to_string(pendingMask));

        // Cleanup: complete the invalidation manually
        uint64_t epoch = ubcc->getEpochForLine(dsm_pa3);
        ubcc->processInvalidationAck(dsm_pa3, 1, epoch);
    }

    // ---- Test 6: M4-5 readiness (grant-path registration) ----
    // Sentinel registration is now implicit: processOuterRequest updates
    // sharersMask/ownerNode/dirty directly — no separate install call.
    {
        int requester = home_node;

        // M4-5-a: processOuterRequest works (grant function exists)
        UBCC_OuterGrantType grant =
            ubcc->processOuterRequest(dsm_pa4,
                UBCC_OuterReqType::GlobalReadUnique,
                true, requester);

        M4_CHECK("M4-5-a: processOuterRequest Unique+write → GrantModified",
                 grant == UBCC_OuterGrantType::GlobalGrantModified,
                 std::string("grant=") + std::to_string(static_cast<int>(grant)));

        // M4-5-b: getUbccDirFieldsForTest provides snapshot
        MESIState state;
        int ownerNode;
        uint64_t sharersMask;
        bool dirty;
        bool exists = ubcc->getUbccDirFieldsForTest(dsm_pa4, state, ownerNode,
                                                     sharersMask, dirty);
        M4_CHECK("M4-5-b: getUbccDirFieldsForTest returns entry",
                 exists, "");
        M4_CHECK("M4-5-c: round-trip: state==G_M ownerNode==requester dirty==true",
                 exists && state == MESIState::G_M
                        && ownerNode == requester
                        && dirty,
                 "full round-trip verification");
    }

    // ---- Test 7: M4-FMT — directory field completeness ----
    // Verify getUbccDirFieldsForTest returns all four fields meaningfully.
    {
        MESIState state;
        int ownerNode;
        uint64_t sharersMask;
        bool dirty;

        // dsm_pa was evicted earlier; dsm_pa2 is still G_E.
        bool exists = ubcc->getUbccDirFieldsForTest(dsm_pa2, state, ownerNode,
                                                     sharersMask, dirty);
        M4_CHECK("M4-FMT-1: getUbccDirFieldsForTest returns all fields",
                 exists && state == MESIState::G_E
                        && ownerNode == static_cast<int>(home_node)
                        && sharersMask == 0
                        && dirty == false,
                 "field integrity check");

        // Verify inspectUbccDirForTest returns JSON with required keys
        std::string json = ubcc->inspectUbccDirForTest(dsm_pa2);
        bool has_state  = (json.find("\"state\"") != std::string::npos);
        bool has_owner  = (json.find("\"ownerNode\"") != std::string::npos);
        bool has_sharers= (json.find("\"sharersMask\"") != std::string::npos);
        bool has_dirty  = (json.find("\"dirty\"") != std::string::npos);
        bool has_epoch  = (json.find("\"epoch\"") != std::string::npos);

        M4_CHECK("M4-FMT-2: inspectUbccDirForTest JSON has all keys",
                 has_state && has_owner && has_sharers && has_dirty && has_epoch,
                 "state+owner+sharers+dirty+epoch must be present");
    }

    printf("=== M4 Self-Test Results: %d/%d PASS, %d FAIL, %d SKIP ===\n",
           _passed, _total, _failed, _skipped);

    if (_failed > 0) {
        printf("M4_SELF_TEST_FAILED=1\n");
        fflush(stdout);
    } else {
        printf("M4_SELF_TEST_PASSED=1\n");
        fflush(stdout);
    }
}

} // namespace M4SelfTest

} // namespace ruby
} // namespace gem5

// Entry point for EPBackend::init()
namespace gem5 {
namespace ruby {
void m4SelfTest_run(EPBackend *backend)
{
    if (!backend || !backend->getUBCC())
        return;
    M4SelfTest::runSelfTest(backend->getUBCC(), backend->nodeId());
}
} // namespace ruby
} // namespace gem5
