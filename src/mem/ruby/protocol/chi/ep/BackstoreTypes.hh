#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_BACKSTORETYPES_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_BACKSTORETYPES_HH__

#include <cstdint>
#include <cstring>

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

static constexpr size_t BackstorePageSize = 256;
static constexpr size_t BackstorePageHeaderSize = 24;
static constexpr size_t BackstorePageEntryArea = BackstorePageSize - BackstorePageHeaderSize;
static constexpr size_t CompactEntryBytes = 12;
static constexpr size_t MaxEntriesPerPage = BackstorePageEntryArea / CompactEntryBytes;

static constexpr size_t BackstoreNumGroups = 16;
static constexpr size_t GroupIndexSize = 256;

struct BackstoreEntry {
    uint64_t pa;
    UBCCMESIState state;
    uint64_t sharersMask;
    uint64_t epoch;
    bool deleted;

    BackstoreEntry()
        : pa(0), state(UBCCMESIState::G_I), sharersMask(0),
          epoch(0), deleted(false)
    {}
};

struct GroupIndex {
    uint64_t page_directory[4];
    uint32_t live_count;
    uint32_t dirty_count;
    uint32_t stale_delete_count;
    uint32_t insert_count;
    uint8_t  existence_bf[32];
    uint8_t  padding[176];

    GroupIndex()
        : page_directory{0, 0, 0, 0},
          live_count(0), dirty_count(0),
          stale_delete_count(0), insert_count(0)
    {
        std::memset(existence_bf, 0, sizeof(existence_bf));
        std::memset(padding, 0, sizeof(padding));
    }

    bool hasActivePages() const { return page_directory[0] != 0; }

    /**
     * Find the first existing page_directory pointer for a free slot,
     * or request new page if all 4 are used (last is always replaceable
     * if the schema supports it). Returns index into page_directory[]
     * where to write, or -1 if full and cannot grow.
     */
    int firstFreeDirSlot() const
    {
        for (int i = 0; i < 4; ++i)
            if (page_directory[i] == 0) return i;
        return -1;
    }
};

struct BackstorePageHeader {
    uint64_t page_id;       //  8B
    uint64_t next_page_ptr; //  8B
    uint16_t entry_count;   //  2B
    uint16_t free_offset;   //  2B
    uint8_t  padding[4];    //  4B → 24B total
};

struct BackstorePage {
    BackstorePageHeader hdr;
    uint8_t entries[BackstorePageEntryArea];

    BackstorePage()
    {
        clear();
    }

    void clear()
    {
        hdr.page_id = 0;
        hdr.entry_count = 0;
        hdr.free_offset = 0;
        hdr.next_page_ptr = 0;
        std::memset(hdr.padding, 0, sizeof(hdr.padding));
        std::memset(entries, 0, sizeof(entries));
    }

    bool isFull() const
    {
        return hdr.entry_count >= MaxEntriesPerPage ||
               hdr.free_offset >= BackstorePageEntryArea;
    }

    /**
     * Load from a raw 256B byte buffer (e.g., after MetaRNF read).
     */
    void loadFrom(const uint8_t buf[BackstorePageSize])
    {
        std::memcpy(&hdr, buf, sizeof(hdr));
        std::memcpy(entries, buf + sizeof(hdr), sizeof(entries));
    }

    /**
     * Store to a raw 256B byte buffer (e.g., before MetaRNF write).
     */
    void storeTo(uint8_t buf[BackstorePageSize]) const
    {
        std::memcpy(buf, &hdr, sizeof(hdr));
        std::memcpy(buf + sizeof(hdr), entries, sizeof(entries));
    }

    /**
     * Get a pointer to the compact entry at the given slot offset (byte offset
     * into entries[] array). Returns nullptr if offset is out of bounds.
     */
    uint8_t* entryAt(int byteOffset)
    {
        if (byteOffset < 0 || byteOffset + CompactEntryBytes > (int)BackstorePageEntryArea)
            return nullptr;
        return &entries[byteOffset];
    }

    const uint8_t* entryAt(int byteOffset) const
    {
        if (byteOffset < 0 || byteOffset + CompactEntryBytes > (int)BackstorePageEntryArea)
            return nullptr;
        return &entries[byteOffset];
    }
};

static_assert(sizeof(BackstorePage) == BackstorePageSize,
              "BackstorePage must be exactly 256B");
static_assert(sizeof(GroupIndex) == GroupIndexSize,
              "GroupIndex must be exactly 256B");

namespace CompactCodec
{

namespace
{

constexpr uint64_t kMask44 = (1ULL << 44) - 1;
constexpr uint64_t kMask2  = (1ULL << 2) - 1;
constexpr uint64_t kMask10 = (1ULL << 10) - 1;
constexpr uint64_t kMask24 = (1ULL << 24) - 1;
constexpr uint64_t kMask4  = (1ULL << 4) - 1;

} // anonymous namespace

/**
 * Pack a BackstoreEntry into a 12-byte compact wire format.
 *
 *   Byte 0-5:   pa[43:0] | state[1:0]                         (46b: 44+2)
 *   Byte 5-7:   sharers[9:0] | epoch[13:0]                    (24b: 10+14)
 *   Byte 7-11:  epoch[23:14] | flags[3:0] | 4 bits reserved   (14b: 10+4+4)
 *
 *   Total: 46+24+14 = 84 bits → 10.5 bytes → 12 bytes padded.
 *
 * Compact encoding layed out as three 32-bit words LE:
 *
 *   w0[31:0]  = pa[31:0]
 *   w1[31:0]  = pa[43:32] | state[1:0]     | sharers[9:0]   | epoch[7:0]
 *              =  12        | 2               | 10             | 8 = 32b
 *   w2[31:0]  = epoch[23:8] | flags[3:0]    | reserved[3:0]
 *              =  16         | 4              | 4 = 24b (8b slack)
 */
inline void pack(const BackstoreEntry& in, uint8_t out[CompactEntryBytes])
{
    uint64_t pa = in.pa & kMask44;
    uint32_t state = static_cast<uint32_t>(in.state) & 0x3;
    uint32_t sharers = static_cast<uint32_t>(in.sharersMask & kMask10);
    uint32_t epoch = static_cast<uint32_t>(in.epoch & kMask24);
    uint32_t flags = (in.deleted ? 0x1 : 0x0);

    uint32_t w0 = static_cast<uint32_t>(pa & 0xFFFFFFFFULL);
    uint32_t w1 = static_cast<uint32_t>(((pa >> 32) & 0xFFF) << 20)
                | (state << 18)
                | (sharers << 8)
                | ((epoch & 0xFF) << 0);
    uint32_t w2 = static_cast<uint32_t>(((epoch >> 8) & 0xFFFF) << 8)
                | (flags << 4);

    std::memcpy(out + 0, &w0, 4);
    std::memcpy(out + 4, &w1, 4);
    std::memcpy(out + 8, &w2, 4);
}

/**
 * Unpack a 12-byte compact wire format into a BackstoreEntry.
 * @param pa  Expected PA (used to set out.pa).
 */
inline void unpack(uint64_t expectedPa, const uint8_t in[CompactEntryBytes],
                   BackstoreEntry& out)
{
    uint32_t w0 = 0, w1 = 0, w2 = 0;
    std::memcpy(&w0, in + 0, 4);
    std::memcpy(&w1, in + 4, 4);
    std::memcpy(&w2, in + 8, 4);

    uint64_t pa_lo = (w0 & 0xFFFFFFFFULL);
    uint64_t pa_hi = ((w1 >> 20) & 0xFFFULL);
    uint64_t storedPa = pa_lo | (pa_hi << 32);
    (void)storedPa; // reserved for integrity check

    uint32_t state    = (w1 >> 18) & 0x3;
    uint32_t sharers  = (w1 >> 8)  & 0x3FF;
    uint32_t epoch_lo = (w1 >> 0)  & 0xFF;
    uint32_t epoch_hi = (w2 >> 8)  & 0xFFFF;
    uint32_t flags    = (w2 >> 4)  & 0xF;

    out.pa = expectedPa;
    out.state = static_cast<UBCCMESIState>(state);
    out.sharersMask = sharers;
    out.epoch = epoch_lo | (static_cast<uint64_t>(epoch_hi) << 8);
    out.deleted = (flags & 0x1) != 0;
}

} // namespace CompactCodec

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_BACKSTORETYPES_HH__
