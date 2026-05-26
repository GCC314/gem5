#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_SENTINELHELPER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_SENTINELHELPER_HH__

/**
 * TEST-ONLY helper for accessing the HN-F directory from external code.
 *
 * Accesses HN directory via `getDirectoryPtr()` virtual accessor
 * (SLICC generated, see AbstractController.hh and StateMachine.py).
 * No '#define private public' trick needed — the SLICC code generator
 * emits the override for all controllers with a 'directory' config
 * parameter.
 *
 * This header is ONLY included from SentinelHelper.cc and is never
 * exposed to production protocol paths.
 */

#include <cstdint>
#include <string>
#include <map>

namespace gem5
{
namespace ruby
{

class RubySystem;
class AbstractController;

struct DirEntrySnapshot {
    int sharerCount;
    bool ownerExists;
    bool ownerIsExcl;
    int ownerVersion;
    int ownerType;  // MachineType enum value
    std::string ownerStr;
    int state;  // Cache_State enum value
    bool epRnfInSharers;
    bool epRnfIsOwner;
    bool epRnfLookupFailed; // true if findEpRnfMachineID() failed
};

class SentinelHelper
{
  public:
    SentinelHelper(RubySystem *ruby_system, int node_id);
    ~SentinelHelper();

    int nodeId() const { return _nodeId; }

    /**
     * Install EP_RNF as a sentinel in the HN-F directory.
     *
     * @param line_pa  Physical address of the cache line (must be DSM).
     * @param as_owner If true, install as S_OWNER; otherwise S_SHARER.
     * @return true on success, false on failure (non-DSM, etc.)
     */
    bool installSentinelForTest(uint64_t line_pa, bool as_owner);

    /**
     * Remove EP_RNF sentinel from the HN-F directory.
     *
     * @param line_pa Physical address of the cache line.
     * @return true on success.
     */
    bool removeSentinelForTest(uint64_t line_pa);

    /**
     * Inspect the HN-F directory entry for a given line.
     *
     * @param line_pa Physical address of the cache line.
     * @param snap   Output: filled with directory state snapshot.
     * @return true if entry exists and was read.
     */
    bool inspectDirEntryForTest(uint64_t line_pa, DirEntrySnapshot &snap);

    /**
     * Check whether a PA is in the DSM range for this node.
     */
    bool isDsmAddr(uint64_t pa) const;

    /**
     * Get number of snoops received by EP_RNF since last reset.
     */
    uint64_t getEpRnfSnoopCount() const { return _epRnfSnoopCount; }
    void resetEpRnfSnoopCount() { _epRnfSnoopCount = 0; }
    void incrementEpRnfSnoopCount() { _epRnfSnoopCount++; }

  private:
    RubySystem *_ruby_system;
    int _nodeId;
    uint64_t _epRnfSnoopCount;

    // Internal: find the HN Cache_Controller for this node
    AbstractController* _findHnController();
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_SENTINELHELPER_HH__
