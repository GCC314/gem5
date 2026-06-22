#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_BACKSTOREORGANIZATION_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_BACKSTOREORGANIZATION_HH__

#include <functional>
#include <vector>

#include "mem/ruby/protocol/chi/ep/BackstoreTypes.hh"

namespace gem5
{

namespace ruby
{

/**
 * Abstract interface for DRAM backstore page organization strategies.
 *
 * Multiple implementations (Schema A, Schema C) allow performance
 * comparison and ablation study.  The interface separates layout
 * policy from the ResidentDir / MetaRNF / EPBackend mechanics.
 *
 * All methods are const so the caller (EPBackend / UBCCController)
 * can make protocol decisions based on layout without side effects.
 * State modifications (e.g., inserting into a page, updating header)
 * happen through the returned UpdatePlan and the caller's RMW.
 */
class BackstoreOrganization
{
  public:
    virtual ~BackstoreOrganization() = default;

    /** Return the 16-group group index for a PA. */
    virtual uint32_t groupForPa(uint64_t pa) const = 0;

    /**
     * Return the list of page physical addresses (metadata PA) that
     * could contain the given PA during a lookup.  The caller must
     * read each page in turn until found or list exhausted.
     */
    virtual std::vector<uint64_t> candidatePagesForLookup(
        uint64_t pa, const GroupIndex& idx) const = 0;

    /**
     * Search a fully-loaded page for a matching entry.
     * Returns true and fills out on success.
     */
    virtual bool lookupInPage(uint64_t pa, const BackstorePage& page,
                              BackstoreEntry& out) const = 0;

    /**
     * Result of planning an upsert or delete operation.
     */
    struct UpdatePlan {
        uint64_t target_page_pa;    // metadata page PA to RMW
        bool needs_new_page;        // true if target_page_pa is fresh (no read needed)
        bool needs_read_before;     // true if the page must be read before modifying
        int entry_slot_offset;      // byte offset within page.entries[] for this entry
        bool is_tombstone;          // if delete: true = overwrite as tombstone

        UpdatePlan()
            : target_page_pa(0), needs_new_page(false),
              needs_read_before(true), entry_slot_offset(-1),
              is_tombstone(false) {}
    };

    /**
     * Plan an upsert (insert or update) into the DRAM backstore.
     * Determines which page to target and where within it.
     */
    virtual UpdatePlan planUpsert(uint64_t pa, const BackstoreEntry& entry,
                                  const GroupIndex& idx) const = 0;

    /**
     * Plan a tombstone delete for a PA.
     */
    virtual UpdatePlan planDelete(uint64_t pa,
                                  const GroupIndex& idx) const = 0;

    /**
     * Return all page PAs in scan order for reconstruct.
     */
    virtual std::vector<uint64_t> scanGroupPages(const GroupIndex& idx) const = 0;

    /**
     * Apply an upsert into a page (modify page locally, then caller writes
     * the page to DRAM via MetaRNF).  Handles new entry append or overwrite.
     *
     * @param page   The page to modify (must be read first if not new).
     * @param pa     PA of the entry.
     * @param entry  The expanded entry value.
     * @param plan   The plan returned by planUpsert().
     */
    virtual void applyUpsert(BackstorePage& page, uint64_t pa,
                             const BackstoreEntry& entry,
                             const UpdatePlan& plan) const = 0;

    /**
     * Apply a tombstone delete into a page.  Sets the deleted flag
     * on the matching entry.
     *
     * @return true if the entry was found and tombstone'd, false if not found.
     */
    virtual bool applyDelete(BackstorePage& page, uint64_t pa,
                             const UpdatePlan& plan) const = 0;

    /**
     * After writing a page to DRAM, update the GroupIndex metadata
     * to reflect the change (e.g., new page pointer, count updates).
     */
    virtual void updateIndexAfterWrite(GroupIndex& idx,
                                       const UpdatePlan& plan,
                                       uint64_t new_page_pa = 0) const = 0;

    /**
     * Return a human-readable name for logging / ablation reports.
     */
    virtual const char* name() const = 0;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_BACKSTOREORGANIZATION_HH__
