#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_BACKSTORESCHEMAC_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_BACKSTORESCHEMAC_HH__

#include "mem/ruby/protocol/chi/ep/BackstoreOrganization.hh"

namespace gem5
{

namespace ruby
{

/**
 * Schema C: Bucketized page organization.
 *
 * Per group: page_directory[0..3] = bucket heads (4 buckets).
 * PA is hashed within the group to select which bucket.
 * Each bucket is a linked list of pages (via next_page_ptr).
 *
 * Lookup: bucket → walk chain → linear scan within page.
 * Insert: bucket → walk to tail page → append; if full, allocate new.
 * Delete: bucket → walk chain → set tombstone.
 */
class BackstoreSchemaC : public BackstoreOrganization
{
  public:
    BackstoreSchemaC() = default;
    ~BackstoreSchemaC() override = default;

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

    const char* name() const override { return "SchemaC"; }

  private:
    static constexpr int kNumBuckets = 4;

    int bucketForPa(uint64_t pa) const;

    int findFreeSlot(const BackstorePage& page) const;
    int findEntryOffset(const BackstorePage& page, uint64_t pa) const;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_BACKSTORESCHEMAC_HH__
