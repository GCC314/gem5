#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_RESIDENTDIR_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_RESIDENTDIR_HH__

#include <cstddef>
#include <cstdint>
#include <vector>

#include "mem/ruby/protocol/chi/ep/BackstoreTypes.hh"

namespace gem5
{

namespace ruby
{

struct UBCCDirEntry {
    uint64_t lineAddr;
    UBCCMESIState state;
    uint64_t sharersMask;
    uint64_t epoch;
    bool residentDirty;

    UBCCDirEntry()
        : lineAddr(0), state(UBCCMESIState::G_I), sharersMask(0),
          epoch(0), residentDirty(false)
    {}

    bool isEmpty() const { return state == UBCCMESIState::G_I && !residentDirty; }
    bool isTombstone() const { return state == UBCCMESIState::G_I && residentDirty; }
    bool isExclusive() const
    {
        return state == UBCCMESIState::G_E || state == UBCCMESIState::G_M;
    }

    static int ownerFromSharers(const UBCCDirEntry &e);
    static bool protoDirty(const UBCCDirEntry &e);
    static bool canonicalOneHotRequired(const UBCCDirEntry &e);
};

class ResidentDir
{
  public:
    static constexpr size_t SramBytes = 512 * 1024;
    static constexpr size_t EntryBytes = 7;
    static constexpr size_t DefaultBloomBytes = 60 * 1024;
    static constexpr size_t DefaultIndexBytes = 4 * 1024;
    static constexpr int    BloomHashes = 4;
    static constexpr int    BloomGroups = 16;

    explicit ResidentDir(size_t bf_bytes = DefaultBloomBytes,
                         size_t force_entries = 0);

    bool lookup(uint64_t pa, UBCCDirEntry& out) const;
    bool lookupWithSlot(uint64_t pa, UBCCDirEntry& out, size_t& slot) const;
    bool insert(uint64_t pa, const UBCCDirEntry& in);
    void update(uint64_t pa, const UBCCDirEntry& in);
    bool remove(uint64_t pa);
    bool forceRemove(uint64_t pa);
    void clear();

    // ---- Plain Bloom Filter (grouped) ----
    bool bloomMayContain(uint64_t pa) const;
    void bloomInsert(uint64_t pa);
    void bloomRemove(uint64_t pa);
    void bloomClear();

    // ---- Group Index ----
    const GroupIndex& groupIndex(int g) const { return _groupIndex[g]; }
    GroupIndex& groupIndex(int g) { return _groupIndex[g]; }
    int groupForPa(uint64_t pa) const;

    // ---- Reconstruction ----
    bool shouldReconstructGroup(int g) const;
    void reconstructGroup(int g);

    // ---- Diagnostics ----
    double estimateFPR(int group = -1) const;

    uint8_t control(size_t slot) const;
    void setFillPending(uint64_t pa, bool v);
    void setWbPending(uint64_t pa, bool v);
    void setPinned(uint64_t pa, bool v);
    bool fillPending(uint64_t pa) const;
    bool wbPending(uint64_t pa) const;
    bool pinned(uint64_t pa) const;
    void touch(uint64_t pa);

    bool hasFreeSlot() const;
    bool pickVictim(uint64_t avoidPa, uint64_t &victimPa, UBCCDirEntry &victim) const;

    size_t capacity() const { return _capacity; }
    size_t count() const { return _count; }

  private:
    size_t hashLine(uint64_t pa) const;
    bool findSlot(uint64_t pa, size_t& slot) const;

    uint64_t loadPacked56(size_t slot) const;
    void storePacked56(size_t slot, uint64_t packed56);
    void encodeEntry(uint64_t pa, const UBCCDirEntry& in, uint64_t& out56) const;
    void decodeEntry(uint64_t pa, uint64_t packed56, UBCCDirEntry& out) const;

    static int decodeOwner(uint8_t owner_code);
    static uint8_t encodeOwner(int owner_node);
    static uint64_t splitmix64(uint64_t x);

    // ---- Bloom Filter helpers ----
    size_t bloomByteOffset(uint64_t pa, int hash_idx, int group) const;
    size_t bloomBitIndex(size_t byteOff, int bitSub) const;
    bool bloomBitTest(uint64_t pa, int hash_idx) const;
    void bloomBitSet(uint64_t pa, int hash_idx);
    int bloomGroup(uint64_t pa) const;
    size_t bloomGroupBytes() const { return _bloomBytes / BloomGroups; }

    void validateCanonical(const UBCCDirEntry& in, uint64_t pa) const;

    // Scan resident entries belonging to a group and insert into shadow BF.
    void scanResidentForGroup(int g, std::vector<uint8_t>& shadowBF) const;

  private:
    uint8_t _buf[SramBytes];
    size_t _capacity;
    size_t _count;
    size_t _bfOffset;
    size_t _bloomBytes;
    size_t _bloomBitCount;
    uint64_t _lruTick;

    std::vector<uint64_t> _keys;
    std::vector<uint8_t> _used;
    std::vector<uint8_t> _dist;
    std::vector<uint8_t> _ctrl;
    std::vector<uint8_t> _bloomBits;

    GroupIndex _groupIndex[BloomGroups];

    static constexpr uint32_t kReconstructPeriod = 1024;
    static constexpr double kReconstructStaleThreshold = 0.25;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_RESIDENTDIR_HH__
