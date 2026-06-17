#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_RESIDENTDIR_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_RESIDENTDIR_HH__

#include <cstddef>
#include <cstdint>
#include <vector>

namespace gem5
{

namespace ruby
{

enum class UBCCMESIState : uint8_t {
    G_I = 0,
    G_S = 1,
    G_E = 2,
    G_M = 3,
};

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
    static constexpr size_t DefaultBloomBytes = 64 * 1024;
    static constexpr int BloomHashes = 3;
    static constexpr int CounterBits = 4;

    explicit ResidentDir(size_t bf_bytes = DefaultBloomBytes,
                         size_t force_entries = 0);

    bool lookup(uint64_t pa, UBCCDirEntry& out) const;
    bool lookupWithSlot(uint64_t pa, UBCCDirEntry& out, size_t& slot) const;
    bool insert(uint64_t pa, const UBCCDirEntry& in);
    void update(uint64_t pa, const UBCCDirEntry& in);
    bool remove(uint64_t pa);
    bool forceRemove(uint64_t pa);
    void clear();

    bool bloomMayContain(uint64_t pa) const;
    void bloomInsert(uint64_t pa);
    void bloomRemove(uint64_t pa);
    void bloomClear();

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
    size_t bloomCounterIndex(uint64_t pa, int hash_idx) const;
    uint8_t bloomCounterRead(size_t idx) const;
    void bloomCounterWrite(size_t idx, uint8_t v);
    void validateCanonical(const UBCCDirEntry& in, uint64_t pa) const;

  private:
    uint8_t _buf[SramBytes];
    size_t _capacity;
    size_t _count;
    size_t _bfOffset;
    size_t _bloomBytes;
    size_t _bloomCounterCount;
    uint64_t _lruTick;

    std::vector<uint64_t> _keys;
    std::vector<uint8_t> _used;
    std::vector<uint8_t> _dist;
    std::vector<uint8_t> _ctrl;
    std::vector<uint8_t> _bloomCounters;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_RESIDENTDIR_HH__
