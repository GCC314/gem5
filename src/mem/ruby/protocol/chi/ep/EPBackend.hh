#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__

#include <cstdint>
#include <map>
#include <string>
#include <vector>

#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"
#include "params/EPBackend.hh"
#include "sim/sim_object.hh"

namespace gem5
{

namespace ruby
{

class UBCCController;
class RubySystem;

// ---- M5 Outer Protocol Types ----
// Outer protocol request types sent from EP_SNF (requester) to home UBCC.
enum class OuterReqType {
    GlobalReadShared,   // Requester needs shared read permission
    GlobalReadUnique    // Requester needs unique/exclusive permission
};

// Outer protocol grant types sent from home UBCC to requester EPBackend.
enum class OuterGrantType {
    GlobalGrantShared,     // Shared read granted
    GlobalGrantExclusive,  // Clean exclusive owner granted
    GlobalGrantModified    // Dirty modified owner granted
};

// Requester-side per-line bookkeeping state.
// Tracks the global permission held by this requester for a remote DSM line.
enum class RequesterLineState {
    R_I,            // No global permission
    R_WAIT_GRANT,   // Remote miss issued, waiting for home grant
    R_S,            // Shared read permission held
    R_E,            // Clean exclusive owner permission (GrantExclusive)
    R_M             // Dirty modified owner permission (GrantModified)
};

// Per-line requester bookkeeping entry.
struct RequesterLineEntry {
    uint64_t lineAddr;
    RequesterLineState state;
    OuterReqType pendingReq;
    uint64_t epoch;
    bool writeIntent;
};

// ---- M5 Inspection API return types ----
struct RequesterLineSnapshot {
    bool valid;
    uint64_t lineAddr;
    int state;      // RequesterLineState cast to int
    int pendingReq; // OuterReqType cast to int
    bool writeIntent;
    uint64_t epoch;
};

// Snapshot of the last UBCC sideband observed by EP_SNF on a recvRequestMsg.
// Used for Python test inspection of HN→EP_SNF sideband field values.
struct SidebandSnapshot {
    bool valid;         // true if a sideband has been observed
    uint64_t lineAddr;  // PA of the request that carried the sideband
    int neededPerm;     // 0=Shared, 1=Unique
    bool writeIntent;   // write intent flag
    int outerReqType;   // mapped outer request: 0=GlobalReadShared, 1=GlobalReadUnique
    int grantResult;    // grant result code (OuterGrantType cast to int, -1=none)
    int homeNode;       // home node determined from PA routing (-1=none)
};

class EPBackend : public SimObject
{
  public:
    PARAMS(EPBackend);
    EPBackend(const Params &p);
    ~EPBackend();

    void init() override;
    void wakeup();

    int nodeId() const { return _nodeId; }

    bool checkAddr(uint64_t pa) const;
    bool checkDsmAddr(uint64_t pa) const;
    const NodeAddressMap& addrMap() const { return _addrMap; }

    // ---- M5: Remote Miss Request Dispatch ----
    // Called by EP_SNF when it receives a ReadNoSnp for a remote DSM line.
    // Returns the home_node for this request (-1 if invalid).
    int handleRemoteMiss(uint64_t line_pa, int neededPerm, bool writeIntent,
                         int& outHomeNode);

    // Called after home UBCC makes a grant decision.
    // Returns the OuterGrantType that was granted.
    OuterGrantType handleGrant(uint64_t line_pa, OuterGrantType grant,
                               int homeNode);

    // ---- M4 Sentinel Registration Test Hooks ----
    // These are exposed to Python via gem5's Swig/SWIG bindings.

    bool installSentinelForTest(uint64_t line_pa, bool as_owner);
    bool removeSentinelForTest(uint64_t line_pa);
    std::string inspectDirEntryForTest(uint64_t line_pa);
    bool isDsmAddr(uint64_t pa) const;

    /** EP_RNF snoop counter for test verification */
    uint64_t getEpRnfSnoopCount() const;
    void resetEpRnfSnoopCount();
    void incrementEpRnfSnoopCount();

    /** Access to UBCCController for Python inspection */
    UBCCController* getUBCC() const { return _ubcc; }

    // ---- M5 Inspection API ----
    /** Inspect requester-side bookkeeping for a given line. */
    RequesterLineSnapshot inspectRequesterState(uint64_t line_pa) const;

    /**
     * Record the sideband values from a recvRequestMsg for later
     * inspection by Python test harness.
     * Called by EPSNFController::recvRequestMsg().
     */
    void recordSideband(uint64_t line_pa, int neededPerm, bool writeIntent,
                        int outerReqType, int grantResult, int homeNode);

    /**
     * Retrieve the last recorded sideband snapshot.
     * Returns a snapshot with valid=false if no sideband has been recorded.
     */
    SidebandSnapshot inspectLastSideband() const;

    /** Clear the last sideband snapshot (for test reset). */
    void clearSidebandSnapshot();

  private:
    const int _nodeId;
    NodeAddressMap _addrMap;
    UBCCController *_ubcc = nullptr;

    // ---- M5: Requester-Side Bookkeeping ----
    // Per-line entries tracking global permissions for remote DSM lines.
    std::map<uint64_t, RequesterLineEntry> _requesterLines;
    uint64_t _epochCounter = 0;

    // ---- M5: Sideband Inspection ----
    // Last sideband values recorded by recvRequestMsg.
    // Only for test/observation hooks; not used in protocol decisions.
    SidebandSnapshot _lastSideband;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_EPBACKEND_HH__
