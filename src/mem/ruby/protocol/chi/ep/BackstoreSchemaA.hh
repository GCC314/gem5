#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_BACKSTORESCHEMAA_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_BACKSTORESCHEMAA_HH__

#include "mem/ruby/protocol/chi/ep/BackstoreOrganization.hh"

namespace gem5
{

namespace ruby
{

/**
 * Schema A: Append-friendly segment organization.
 *
 * Per group:
 *   page_directory[0] = head page (oldest)
 *   page_directory[1] = tail page (newest, where appends land)
 *   page_directory[2..3] = unused (reserved for future overflow)
 *
 * Lookup walks the chain head→tail linearly.
 * Insert appends to tail (fast, no search needed for write path).
 * Delete walks the chain and sets tombstone bit.
 */
class BackstoreSchemaA : public BackstoreOrganization
{
  public:
    BackstoreSchemaA() = default;
    ~BackstoreSchemaA() override = default;

    uint32_t groupForPa(uint64_t pa) const override;

    std::vector<uint64_t> candidatePagesForLookup(
        uint64_t pa, const GroupIndex& idx) const override;

    bool lookupInPage(uint64_t pa, const BackstorePage& page,
                      BackstoreEntry& out) const override;

    UpdatePlan planUpsert(uint64_t pa, const BackstoreEntry& entry,
                          const GroupIndex& idx) const override;

    UpdatePlan planDelete(uint64_t pa,
                          const GroupIndex& idx) const override;

    std::vector<uint64_t> scanGroupPages(const GroupIndex& idx) const override;

    void applyUpsert(BackstorePage& page, uint64_t pa,
                     const BackstoreEntry& entry,
                     const UpdatePlan& plan) const override;

    bool applyDelete(BackstorePage& page, uint64_t pa,
                     const UpdatePlan& plan) const override;

    void updateIndexAfterWrite(GroupIndex& idx,
                               const UpdatePlan& plan,
                               uint64_t new_page_pa = 0) const override;

    const char* name() const override { return "SchemaA"; }

  private:
    static constexpr size_t kHeadIdx = 0;
    static constexpr size_t kTailIdx = 1;

    static uint64_t murmurFinalizer(uint64_t h);

    /** Find a free slot (offset) in the page's entry area. */
    int findFreeSlot(const BackstorePage& page) const;

    /** Walk page for an entry matching pa. Returns entry byte offset or -1. */
    int findEntryOffset(const BackstorePage& page, uint64_t pa) const;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_BACKSTORESCHEMAA_HH__
