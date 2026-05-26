/**
 * SentinelHelper implementation.
 *
 * Accesses the HN directory through a proper public accessor
 * (AbstractController::getDirectoryPtr()), which is overridden
 * in generated Cache_Controller to return m_directory_ptr.
 *
 * No #define private public hack needed — the SLICC code generator
 * was modified (StateMachine.py) to emit the override for all
 * controllers with a 'directory' config parameter.
 */

#include "mem/ruby/protocol/chi/ep/SentinelHelper.hh"

#include <cstring>
#include <sstream>
#include <typeinfo>

#include "base/logging.hh"
#include "debug/RubyEP.hh"
#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"
#include "mem/ruby/protocol/CHI/Cache_Controller.hh"
#include "mem/ruby/protocol/CHI/Cache_DirEntry.hh"
#include "mem/ruby/protocol/CHI/Cache_State.hh"
#include "mem/ruby/protocol/PerfectCacheMemory.hh"
#include "mem/ruby/protocol/NetDest.hh"
#include "mem/ruby/protocol/MachineID.hh"
#include "mem/ruby/slicc_interface/AbstractController.hh"
#include "mem/ruby/system/RubySystem.hh"

namespace gem5
{
namespace ruby
{

namespace {

using namespace CHI;

/**
 * Iterate over ALL controllers using RubySystem's public m_abstract_controls.
 */
void forEachController(RubySystem *rs,
                       std::function<void(AbstractController*)> fn)
{
    for (const auto &type_map : rs->m_abstract_controls) {
        for (const auto &pair : type_map) {
            fn(pair.second);
        }
    }
}

/**
 * Determine whether a controller is the HN for a given node_id.
 * Uses the getDirectoryPtr() accessor to identify HN controllers
 * (only HNs have a non-null directory pointer), then verifies
 * node_id via address range matching.
 */
bool isHnForNode(AbstractController *ctrl, int targetNodeId)
{
    if (ctrl->getMachineID().type != MachineType_Cache)
        return false;

    // Only HN controllers have a non-null directory pointer
    if (ctrl->getDirectoryPtr() == nullptr)
        return false;

    // Use address range to identify the node.
    // The HN's addr_ranges include [nodeBase + 0, nodeBase + 2*segSize)
    // for the local node. We check the first range to get the node_id.
    const auto &ranges = ctrl->getAddrRanges();
    if (ranges.empty())
        return false;

    // Build a reference NodeAddressMap and check which node the
    // first range start belongs to.
    uint64_t range_start = ranges.front().start();
    int node_from_range = static_cast<int>(range_start >> NodeAddressMap::NODE_ADDR_SHIFT);
    return node_from_range == targetNodeId;
}

/**
 * Find the EP_RNF controller's MachineID by scanning all controllers
 * and using RTTI to identify the EPRNFController class.
 *
 * Returns false if RTTI fails to discover EP_RNF. Caller must handle
 * this failure — silent fallback to node_id is NOT allowed.
 */
bool findEpRnfMachineID(RubySystem *rs, int node_id, MachineID &out)
{
    out.type = MachineType_Cache;
    out.num = 0;

    for (const auto &type_map : rs->m_abstract_controls) {
        for (const auto &pair : type_map) {
            AbstractController *ctrl = pair.second;
            if (!ctrl) continue;
            if (ctrl->getMachineID().type != MachineType_Cache) continue;

            const char *clsName = typeid(*ctrl).name();
            if (strstr(clsName, "EPRNFController") != nullptr) {
                // Verify node_id by checking address range
                const auto &ranges = ctrl->getAddrRanges();
                if (!ranges.empty()) {
                    int node_from_range =
                        static_cast<int>(ranges.front().start() >> NodeAddressMap::NODE_ADDR_SHIFT);
                    if (node_from_range == node_id) {
                        out = ctrl->getMachineID();
                        DPRINTF(RubyEP,
                                "SentinelHelper: found EP_RNF node %d "
                                "machineID type=%d num=%d\n",
                                node_id, (int)out.type, out.num);
                        return true;
                    }
                }
            }
        }
    }

    // RTTI failed: do NOT silently fall back to node_id.
    // Caller must handle this as a failure.
    DPRINTF(RubyEP, "SentinelHelper: EP_RNF for node %d not found via RTTI\n",
            node_id);
    return false;
}

std::string machineIDStr(const MachineID &mid)
{
    std::ostringstream oss;
    oss << "type=" << static_cast<int>(mid.type) << " num=" << mid.num;
    return oss.str();
}

void fillSnapshot(Cache_DirEntry *entry, DirEntrySnapshot &snap)
{
    snap.sharerCount = entry->m_sharers.count();
    snap.ownerExists = entry->m_ownerExists;
    snap.ownerIsExcl = entry->m_ownerIsExcl;
    if (entry->m_ownerExists) {
        snap.ownerVersion = static_cast<int>(entry->m_owner.num);
        snap.ownerType = static_cast<int>(entry->m_owner.type);
        snap.ownerStr = machineIDStr(entry->m_owner);
    } else {
        snap.ownerVersion = -1;
        snap.ownerType = -1;
        snap.ownerStr = "none";
    }
    snap.state = static_cast<int>(entry->m_state);
}

} // anonymous namespace

SentinelHelper::SentinelHelper(RubySystem *ruby_system, int node_id)
    : _ruby_system(ruby_system),
      _nodeId(node_id),
      _epRnfSnoopCount(0)
{
    fatal_if(!_ruby_system, "SentinelHelper: null RubySystem");
}

SentinelHelper::~SentinelHelper()
{
}

AbstractController*
SentinelHelper::_findHnController()
{
    AbstractController *found = nullptr;
    forEachController(_ruby_system, [&](AbstractController *ctrl) {
        if (!found && isHnForNode(ctrl, _nodeId)) {
            found = ctrl;
        }
    });
    if (!found) {
        // Fallback: find ANY Cache controller with a directory pointer
        forEachController(_ruby_system, [&](AbstractController *ctrl) {
            if (!found &&
                ctrl->getMachineID().type == MachineType_Cache &&
                ctrl->getDirectoryPtr() != nullptr) {
                found = ctrl;
            }
        });
    }
    return found;
}

bool
SentinelHelper::isDsmAddr(uint64_t pa) const
{
    NodeAddressMap addrMap(3, 128ULL * 1024 * 1024);
    return addrMap.isDsm(_nodeId, pa) && addrMap.homeNode(_nodeId, pa) == _nodeId;
}

bool
SentinelHelper::installSentinelForTest(uint64_t line_pa, bool as_owner)
{
    if (!isDsmAddr(line_pa)) {
        warn("SentinelHelper: non-DSM address 0x%lx, rejected\n", line_pa);
        return false;
    }

    AbstractController *hnAbs = _findHnController();
    if (!hnAbs) {
        warn("SentinelHelper: no HN controller for node %d\n", _nodeId);
        return false;
    }

    // Use the proper accessor instead of #define private public
    void *dirPtrVoid = hnAbs->getDirectoryPtr();
    if (!dirPtrVoid) {
        warn("SentinelHelper: HN controller %s has no directory\n",
             hnAbs->name().c_str());
        return false;
    }

    auto *dir = static_cast<PerfectCacheMemory<Cache_DirEntry>*>(dirPtrVoid);

    MachineID ep_rnf_id;
    bool found_ep = findEpRnfMachineID(_ruby_system, _nodeId, ep_rnf_id);
    if (!found_ep) {
        warn("SentinelHelper: EP_RNF not found for node %d via RTTI "
             "— sentinel install cannot proceed\n", _nodeId);
        return false;
    }

    DPRINTF(RubyEP, "SentinelHelper: EP_RNF machineID type=%d num=%d\n",
            (int)ep_rnf_id.type, ep_rnf_id.num);

    if (!dir->isTagPresent(line_pa)) {
        dir->allocate(line_pa);
        DPRINTF(RubyEP, "SentinelHelper: allocated new dir entry for 0x%lx\n",
                line_pa);
    }

    Cache_DirEntry *entry = dir->lookup(line_pa);
    if (!entry) {
        warn("SentinelHelper: could not lookup directory entry\n");
        return false;
    }

    DPRINTF(RubyEP, "SentinelHelper: before install - sharers=%d ownerExists=%d "
            "owner=(%d,%d)\n",
            entry->m_sharers.count(), entry->m_ownerExists,
            (int)entry->m_owner.num, (int)entry->m_owner.type);

    // If installing as owner, check invariant: no local dirty owner
    if (as_owner && entry->m_ownerExists && entry->m_ownerIsExcl) {
        if (entry->m_owner.num != ep_rnf_id.num ||
            entry->m_owner.type != ep_rnf_id.type) {
            warn("SentinelHelper: local dirty owner exists "
                 "PA=0x%lx (owner type=%d num=%d), cannot install EP_RNF owner\n",
                 line_pa, (int)entry->m_owner.type, (int)entry->m_owner.num);
            return false;
        }
    }

    // Remove any existing EP_RNF entry first
    entry->m_sharers.remove(ep_rnf_id);

    if (as_owner) {
        entry->m_owner = ep_rnf_id;
        entry->m_ownerExists = true;
        entry->m_ownerIsExcl = true;
    }

    entry->m_sharers.add(ep_rnf_id);

    DPRINTF(RubyEP,
            "SentinelHelper: installed EP_RNF sentinel "
            "PA=0x%lx as_owner=%d sharers=%d\n",
            line_pa, as_owner, entry->m_sharers.count());

    return true;
}

bool
SentinelHelper::removeSentinelForTest(uint64_t line_pa)
{
    AbstractController *hnAbs = _findHnController();
    if (!hnAbs) {
        warn("SentinelHelper: no HN controller for node %d\n", _nodeId);
        return false;
    }

    void *dirPtrVoid = hnAbs->getDirectoryPtr();
    if (!dirPtrVoid)
        return true; // nothing to remove

    auto *dir = static_cast<PerfectCacheMemory<Cache_DirEntry>*>(dirPtrVoid);

    if (!dir->isTagPresent(line_pa)) {
        return true; // nothing to remove
    }

    Cache_DirEntry *entry = dir->lookup(line_pa);
    if (!entry)
        return true;

    MachineID ep_rnf_id;
    bool found_ep = findEpRnfMachineID(_ruby_system, _nodeId, ep_rnf_id);
    if (!found_ep) {
        warn("SentinelHelper: EP_RNF not found for node %d via RTTI "
             "— sentinel remove cannot proceed (not safe to remove "
             "with unknown MachineID)\n", _nodeId);
        return false;
    }

    entry->m_sharers.remove(ep_rnf_id);

    if (entry->m_ownerExists &&
        entry->m_owner.num == ep_rnf_id.num &&
        entry->m_owner.type == ep_rnf_id.type) {
        entry->m_ownerExists = false;
        entry->m_ownerIsExcl = false;
    }

    DPRINTF(RubyEP,
            "SentinelHelper: removed EP_RNF sentinel "
            "PA=0x%lx remaining_sharers=%d\n",
            line_pa, entry->m_sharers.count());

    return true;
}

bool
SentinelHelper::inspectDirEntryForTest(uint64_t line_pa, DirEntrySnapshot &snap)
{
    snap = DirEntrySnapshot();
    snap.epRnfInSharers = false;
    snap.epRnfIsOwner = false;

    AbstractController *hnAbs = _findHnController();
    if (!hnAbs) {
        warn("SentinelHelper: no HN controller for inspection\n");
        return false;
    }

    void *dirPtrVoid = hnAbs->getDirectoryPtr();
    if (!dirPtrVoid)
        return false;

    auto *dir = static_cast<PerfectCacheMemory<Cache_DirEntry>*>(dirPtrVoid);

    if (!dir->isTagPresent(line_pa)) {
        return false;
    }

    Cache_DirEntry *entry = dir->lookup(line_pa);
    if (!entry)
        return false;

    fillSnapshot(entry, snap);

    MachineID ep_rnf_id;
    bool found_ep = findEpRnfMachineID(_ruby_system, _nodeId, ep_rnf_id);
    if (!found_ep) {
        // EP_RNF identity not discoverable — directory inspection
        // succeeds but EP_RNF-specific fields cannot be trusted.
        // Mark them as false and return true so the raw directory
        // snapshot is still usable.
        warn("SentinelHelper: EP_RNF not found for node %d via RTTI "
             "— EP_RNF-specific fields set to false\n", _nodeId);
        snap.epRnfInSharers = false;
        snap.epRnfIsOwner = false;
        snap.epRnfLookupFailed = true;
        return true;
    }

    snap.epRnfInSharers = entry->m_sharers.isElement(ep_rnf_id);

    if (entry->m_ownerExists &&
        entry->m_owner.num == ep_rnf_id.num &&
        entry->m_owner.type == ep_rnf_id.type) {
        snap.epRnfIsOwner = true;
    }

    snap.epRnfLookupFailed = false;

    return true;
}

} // namespace ruby
} // namespace gem5
