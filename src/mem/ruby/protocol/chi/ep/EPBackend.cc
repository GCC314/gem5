#include "mem/ruby/protocol/chi/ep/EPBackend.hh"

#include <sstream>

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "mem/ruby/protocol/chi/ep/UBCCController.hh"
#include "mem/ruby/system/RubySystem.hh"
#include "params/EPBackend.hh"

namespace gem5
{

namespace ruby
{

EPBackend::EPBackend(const Params &p)
  : SimObject(p),
    _nodeId(p.node_id),
    _addrMap(3, 128ULL * 1024 * 1024),
    _lastSideband{false, 0, 0, false, -1, -1, -1}
{
    // Pass RubySystem to UBCCController for SentinelHelper init
    // RubySystem is available via the params
    auto *ruby_system = p.ruby_system;
    _ubcc = new UBCCController(_nodeId, ruby_system);
}

EPBackend::~EPBackend()
{
    delete _ubcc;
}

/**
 * Forward declare the M4 self-test entry point (defined in M4SelfTest.cc).
 */
void m4SelfTest_run(EPBackend*);

/**
 * Forward declare the M5 self-test entry point (defined in M5SelfTest.cc).
 */
void m5SelfTest_run(EPBackend*);


void
EPBackend::init()
{
    SimObject::init();

    // ---- M4 Sentinel Registration Self-Test ----
    // Runs during instantiation; results printed to stdout.
    // Python test harness parses the output.
    // Only one node (node 0) runs the self-tests to avoid duplicate output.
    if (_nodeId == 0 && _ubcc) {
        m4SelfTest_run(this);
        m5SelfTest_run(this);
    }
}

void
EPBackend::wakeup()
{
    if (_ubcc)
        _ubcc->wakeup();
}

bool
EPBackend::checkAddr(uint64_t pa) const
{
    if (_addrMap.isDsm(_nodeId, pa)) {
        int h = _addrMap.homeNode(_nodeId, pa);
        if (h == _nodeId) {
            return true;
        } else {
            fatal("EPBackend node_id=%d: cross-node DSM access "
                  "PA=0x%lx src=%d home_node=%d",
                  _nodeId, pa, _addrMap.srcNodeId(pa), h);
        }
    }
    fatal("EPBackend node_id=%d: forbidden non-DSM access PA=0x%lx",
          _nodeId, pa);
    return false;
}

bool
EPBackend::checkDsmAddr(uint64_t pa) const
{
    if (_addrMap.isDsm(_nodeId, pa))
        return true;

    fatal("EPBackend node_id=%d: non-DSM address on EP path PA=0x%lx",
          _nodeId, pa);
    return false;
}

// ---- M4 Sentinel Registration Test Hooks ----

bool
EPBackend::installSentinelForTest(uint64_t line_pa, bool as_owner)
{
    if (!_ubcc) {
        warn("EPBackend node_id=%d: UBCC not available\n", _nodeId);
        return false;
    }
    return _ubcc->installSentinelForTest(line_pa, as_owner);
}

bool
EPBackend::removeSentinelForTest(uint64_t line_pa)
{
    if (!_ubcc) {
        warn("EPBackend node_id=%d: UBCC not available\n", _nodeId);
        return false;
    }
    return _ubcc->removeSentinelForTest(line_pa);
}

std::string
EPBackend::inspectDirEntryForTest(uint64_t line_pa)
{
    if (!_ubcc) {
        return "{\"error\": \"UBCC not available\"}";
    }
    return _ubcc->inspectDirEntryForTest(line_pa);
}

bool
EPBackend::isDsmAddr(uint64_t pa) const
{
    if (!_ubcc)
        return false;
    return _ubcc->isDsmAddr(pa);
}

uint64_t
EPBackend::getEpRnfSnoopCount() const
{
    if (!_ubcc)
        return 0;
    return _ubcc->getEpRnfSnoopCount();
}

void
EPBackend::resetEpRnfSnoopCount()
{
    if (_ubcc)
        _ubcc->resetEpRnfSnoopCount();
}

void
EPBackend::incrementEpRnfSnoopCount()
{
    if (_ubcc)
        _ubcc->incrementEpRnfSnoopCount();
}

// ---- M5: Remote Miss Request Dispatch ----

int
EPBackend::handleRemoteMiss(uint64_t line_pa, int neededPerm, bool writeIntent,
                             int& outHomeNode)
{
    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: handleRemoteMiss PA=0x%lx "
            "neededPerm=%d writeIntent=%d\n",
            _nodeId, line_pa, neededPerm, writeIntent);

    // Validate: neededPerm must be 0 (Shared) or 1 (Unique)
    if (neededPerm != 0 && neededPerm != 1) {
        fatal("EPBackend node_id=%d: invalid neededPerm=%d "
              "(must be 0 or 1) for PA=0x%lx\n",
              _nodeId, neededPerm, line_pa);
    }

    // Validate: Shared + true is illegal
    if (neededPerm == 0 && writeIntent) {
        fatal("EPBackend node_id=%d: illegal sideband combination "
              "Shared+writeIntent=true for PA=0x%lx\n",
              _nodeId, line_pa);
    }

    // Validate DSM address
    if (!_addrMap.isDsm(_nodeId, line_pa)) {
        fatal("EPBackend node_id=%d: non-DSM address on remote miss path "
              "PA=0x%lx\n", _nodeId, line_pa);
    }

    int homeNode = _addrMap.homeNode(_nodeId, line_pa);
    if (homeNode < 0 || homeNode == _nodeId) {
        fatal("EPBackend node_id=%d: invalid home node %d for PA=0x%lx\n",
              _nodeId, homeNode, line_pa);
    }
    outHomeNode = homeNode;

    // Translate PA from requester's view to home node's view.
    uint64_t offset = _addrMap.dsmOffset(line_pa);
    uint64_t homePa = _addrMap.buildDsmPA(homeNode, homeNode, offset);

    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: translating PA 0x%lx -> home PA 0x%lx "
            "homeNode=%d offset=0x%lx\n",
            _nodeId, line_pa, homePa, homeNode, offset);

    // Map sideband to outer request type
    OuterReqType reqType;
    if (neededPerm == 0) {
        // Shared + false → GlobalReadShared
        reqType = OuterReqType::GlobalReadShared;
    } else {
        // Unique + false/true → GlobalReadUnique
        reqType = OuterReqType::GlobalReadUnique;
    }

    // Create requester bookkeeping entry (uses requester's PA view)
    _epochCounter++;
    RequesterLineEntry entry;
    entry.lineAddr = line_pa;
    entry.state = RequesterLineState::R_WAIT_GRANT;
    entry.pendingReq = reqType;
    entry.epoch = _epochCounter;
    entry.writeIntent = writeIntent;
    entry.homeNode = homeNode;
    _requesterLines[line_pa] = entry;

    // Dispatch to home node's UBCC via cross-node registry.
    // Use home PA so the home UBCC sees the line in its own address space.
    UBCCController *homeUbcc = UBCCController::getInstance(homeNode);
    if (!homeUbcc) {
        // Fallback: use our own UBCC (for same-home-node or
        // bootstrap scenarios). This is a temporary M5 simplification.
        DPRINTF(RubyCHIGeneric,
                "EPBackend node_id=%d: home UBCC for node %d not found, "
                "falling back to local UBCC\n",
                _nodeId, homeNode);
        homeUbcc = _ubcc;
    }
    if (!homeUbcc) {
        fatal("EPBackend node_id=%d: no UBCC available for home node %d "
              "PA=0x%lx\n", _nodeId, homeNode, line_pa);
    }

    // ---- M5 Phase 2: Outer Message Envelope ----
    // Build a proper outer request envelope for dispatch, logging,
    // and future network migration.
    OuterReqEnvelope reqEnv;
    reqEnv.linePa = homePa;
    reqEnv.reqType = reqType;
    reqEnv.writeIntent = writeIntent;
    reqEnv.srcNode = _nodeId;
    reqEnv.epoch = entry.epoch;
    _lastReqEnv = reqEnv;

    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: outer request envelope "
            "linePa=0x%lx reqType=%d writeIntent=%d srcNode=%d epoch=%lu\n",
            _nodeId, reqEnv.linePa, static_cast<int>(reqEnv.reqType),
            reqEnv.writeIntent, reqEnv.srcNode, reqEnv.epoch);

    // Convert outer request type to UBCC's internal enum
    UBCC_OuterReqType ubccReq =
        (reqType == OuterReqType::GlobalReadShared)
            ? UBCC_OuterReqType::GlobalReadShared
            : UBCC_OuterReqType::GlobalReadUnique;

    // Send to home UBCC using home PA view and requesterNode
    // M5 Phase 2: capture grant/sentinel visible ticks for envelope
    Tick grantVisibleTick = 0;
    Tick sentinelVisibleTick = 0;
    UBCC_OuterGrantType ubccGrant =
        homeUbcc->processOuterRequest(homePa, ubccReq, writeIntent, _nodeId,
                                      &grantVisibleTick, &sentinelVisibleTick);

    // ---- M5 Phase 2: Outer Grant Envelope ----
    // Capture grant decision into structured envelope
    OuterGrantEnvelope grantEnv;
    grantEnv.linePa = homePa;
    grantEnv.homeNode = homeNode;
    grantEnv.epoch = entry.epoch;
    grantEnv.grantVisibleTick = grantVisibleTick;
    grantEnv.sentinelVisibleTick = sentinelVisibleTick;

    // Self-test assertion: sentinelVisibleTick <= grantVisibleTick
    if (sentinelVisibleTick > grantVisibleTick) {
        fatal("EPBackend node_id=%d: tick ordering violation "
              "PA=0x%lx sentinelVisibleTick=%lu > grantVisibleTick=%lu\n",
              _nodeId, line_pa, sentinelVisibleTick, grantVisibleTick);
    }

    // Convert UBCC grant back to EPBackend's OuterGrantType
    OuterGrantType grant;
    switch (ubccGrant) {
        case UBCC_OuterGrantType::GlobalGrantShared:
            grant = OuterGrantType::GlobalGrantShared;
            grantEnv.grantType = OuterGrantType::GlobalGrantShared;
            break;
        case UBCC_OuterGrantType::GlobalGrantExclusive:
            grant = OuterGrantType::GlobalGrantExclusive;
            grantEnv.grantType = OuterGrantType::GlobalGrantExclusive;
            break;
        case UBCC_OuterGrantType::GlobalGrantModified:
            grant = OuterGrantType::GlobalGrantModified;
            grantEnv.grantType = OuterGrantType::GlobalGrantModified;
            break;
        default:
            fatal("EPBackend node_id=%d: unknown UBCC grant %d\n",
                  _nodeId, static_cast<int>(ubccGrant));
    }

    _lastGrantEnv = grantEnv;

    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: outer grant envelope "
            "linePa=0x%lx grantType=%d homeNode=%d epoch=%lu\n",
            _nodeId, grantEnv.linePa, static_cast<int>(grantEnv.grantType),
            grantEnv.homeNode, grantEnv.epoch);

    // Handle grant result and update bookkeeping
    OuterGrantType result = handleGrant(line_pa, grant, homeNode);
    return static_cast<int>(result);

    fatal("EPBackend node_id=%d: no UBCC available for remote miss "
          "PA=0x%lx\n", _nodeId, line_pa);
    return -1;
}

OuterGrantType
EPBackend::handleGrant(uint64_t line_pa, OuterGrantType grant, int homeNode)
{
    DPRINTF(RubyCHIGeneric,
            "EPBackend node_id=%d: handleGrant PA=0x%lx grant=%d homeNode=%d\n",
            _nodeId, line_pa, static_cast<int>(grant), homeNode);

    auto it = _requesterLines.find(line_pa);
    if (it == _requesterLines.end()) {
        fatal("EPBackend node_id=%d: grant for unknown line PA=0x%lx\n",
              _nodeId, line_pa);
    }

    // Update requester bookkeeping based on grant
    switch (grant) {
        case OuterGrantType::GlobalGrantShared:
            it->second.state = RequesterLineState::R_S;
            DPRINTF(RubyCHIGeneric,
                    "EPBackend node_id=%d: line 0x%lx -> R_S\n",
                    _nodeId, line_pa);
            break;
        case OuterGrantType::GlobalGrantExclusive:
            it->second.state = RequesterLineState::R_E;
            DPRINTF(RubyCHIGeneric,
                    "EPBackend node_id=%d: line 0x%lx -> R_E (GrantExclusive)\n",
                    _nodeId, line_pa);
            break;
        case OuterGrantType::GlobalGrantModified:
            it->second.state = RequesterLineState::R_M;
            DPRINTF(RubyCHIGeneric,
                    "EPBackend node_id=%d: line 0x%lx -> R_M (GrantModified)\n",
                    _nodeId, line_pa);
            break;
        default:
            fatal("EPBackend node_id=%d: unknown grant type %d\n",
                  _nodeId, static_cast<int>(grant));
    }

    return grant;
}

// ---- M5 Inspection API ----

RequesterLineSnapshot
EPBackend::inspectRequesterState(uint64_t line_pa) const
{
    RequesterLineSnapshot snap;
    snap.valid = false;

    auto it = _requesterLines.find(line_pa);
    if (it != _requesterLines.end()) {
        snap.valid = true;
        snap.lineAddr = it->second.lineAddr;
        snap.state = static_cast<int>(it->second.state);
        snap.pendingReq = static_cast<int>(it->second.pendingReq);
        snap.writeIntent = it->second.writeIntent;
        snap.epoch = it->second.epoch;
        snap.homeNode = it->second.homeNode;
    }

    return snap;
}

void
EPBackend::recordSideband(uint64_t line_pa, int neededPerm, bool writeIntent,
                           int outerReqType, int grantResult, int homeNode)
{
    _lastSideband.valid = true;
    _lastSideband.lineAddr = line_pa;
    _lastSideband.neededPerm = neededPerm;
    _lastSideband.writeIntent = writeIntent;
    _lastSideband.outerReqType = outerReqType;
    _lastSideband.grantResult = grantResult;
    _lastSideband.homeNode = homeNode;
}

SidebandSnapshot
EPBackend::inspectLastSideband() const
{
    return _lastSideband;
}

void
EPBackend::clearSidebandSnapshot()
{
    _lastSideband.valid = false;
    _lastSideband.lineAddr = 0;
    _lastSideband.neededPerm = 0;
    _lastSideband.writeIntent = false;
    _lastSideband.outerReqType = -1;
    _lastSideband.grantResult = -1;
    _lastSideband.homeNode = -1;
}

// ---- M5 Phase 2: Diagnose Expected Grant ----

std::string
EPBackend::diagnoseExpectedGrant(int neededPerm, bool writeIntent) const
{
    if (neededPerm == 0) {
        // Shared → always GrantShared
        if (writeIntent) {
            return "ILLEGAL: Shared+writeIntent=true is invalid";
        }
        return "Shared";
    } else {
        // Unique
        if (writeIntent) {
            return "Modified";
        } else {
            return "Exclusive";
        }
    }
}

} // namespace ruby
} // namespace gem5
