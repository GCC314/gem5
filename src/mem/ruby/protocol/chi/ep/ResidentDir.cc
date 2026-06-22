#include "mem/ruby/protocol/chi/ep/ResidentDir.hh"

#include <algorithm>
#include <cmath>
#include <cstring>

#include "base/logging.hh"

namespace gem5
{

namespace ruby
{

namespace
{

constexpr uint64_t kMask2 = (1ULL << 2) - 1;
constexpr uint64_t kMask16 = (1ULL << 16) - 1;
constexpr uint64_t kMask24 = (1ULL << 24) - 1;
constexpr uint64_t kMask56 = (1ULL << 56) - 1;
constexpr uint8_t kCtrlFillPending = 1u << 0;
constexpr uint8_t kCtrlWbPending = 1u << 1;
constexpr uint8_t kCtrlPinned = 1u << 2;
constexpr uint8_t kCtrlLruShift = 3;
constexpr uint8_t kCtrlLruMask = 0x1f;

} // anonymous namespace

int
UBCCDirEntry::ownerFromSharers(const UBCCDirEntry &e)
{
    if (!e.isExclusive()) {
        return -1;
    }
    if (__builtin_popcountll(e.sharersMask) != 1) {
        return -1;
    }
    return __builtin_ctzll(e.sharersMask);
}

bool
UBCCDirEntry::protoDirty(const UBCCDirEntry &e)
{
    return e.state == UBCCMESIState::G_M;
}

bool
UBCCDirEntry::canonicalOneHotRequired(const UBCCDirEntry &e)
{
    return e.state == UBCCMESIState::G_E || e.state == UBCCMESIState::G_M;
}

ResidentDir::ResidentDir(size_t bf_bytes, size_t force_entries)
    : _buf{0}, _capacity(0), _count(0), _bfOffset(0),
      _bloomBytes(0), _bloomBitCount(0), _lruTick(0)
{
    const size_t capped_bf = std::min(bf_bytes, SramBytes - DefaultIndexBytes);
    _bloomBytes = capped_bf;
    const size_t totalOverhead = _bloomBytes + DefaultIndexBytes;
    _bfOffset = SramBytes - totalOverhead;
    _capacity = _bfOffset / EntryBytes;

    if (force_entries > 0 && force_entries < _capacity) {
        _capacity = force_entries;
    }

    _keys.assign(_capacity, 0);
    _used.assign(_capacity, 0);
    _dist.assign(_capacity, 0);
    _ctrl.assign(_capacity, 0);

    _bloomBitCount = _bloomBytes * 8;
    _bloomBits.assign(_bloomBytes, 0);

    for (int g = 0; g < BloomGroups; ++g)
        _groupIndex[g] = GroupIndex();

    clear();
}

size_t
ResidentDir::hashLine(uint64_t pa) const
{
    if (_capacity == 0) {
        return 0;
    }
    return ((pa >> 6) % _capacity);
}

uint64_t
ResidentDir::loadPacked56(size_t slot) const
{
    const size_t off = slot * EntryBytes;
    uint64_t val = 0;
    for (size_t i = 0; i < EntryBytes; ++i) {
        val |= (static_cast<uint64_t>(_buf[off + i]) << (8 * i));
    }
    return val & kMask56;
}

void
ResidentDir::storePacked56(size_t slot, uint64_t packed56)
{
    const size_t off = slot * EntryBytes;
    const uint64_t val = packed56 & kMask56;
    for (size_t i = 0; i < EntryBytes; ++i) {
        _buf[off + i] = static_cast<uint8_t>((val >> (8 * i)) & 0xff);
    }
}

uint8_t ResidentDir::encodeOwner(int owner_node) { return static_cast<uint8_t>(owner_node); }
int ResidentDir::decodeOwner(uint8_t owner_code) { return static_cast<int>(owner_code); }

uint64_t
ResidentDir::splitmix64(uint64_t x)
{
    x += 0x9e3779b97f4a7c15ULL;
    x = (x ^ (x >> 30)) * 0xbf58476d1ce4e5b9ULL;
    x = (x ^ (x >> 27)) * 0x94d049bb133111ebULL;
    return x ^ (x >> 31);
}

int
ResidentDir::bloomGroup(uint64_t pa) const
{
    return static_cast<int>(splitmix64(pa >> 6) % BloomGroups);
}

int
ResidentDir::groupForPa(uint64_t pa) const
{
    return bloomGroup(pa);
}

size_t
ResidentDir::bloomByteOffset(uint64_t pa, int hash_idx, int group) const
{
    static constexpr uint64_t seeds[BloomHashes] = {
        0x243f6a8885a308d3ULL,
        0x13198a2e03707344ULL,
        0xa4093822299f31d0ULL,
        0xc6e00bf33da88fc2ULL,
    };

    size_t groupBytes = bloomGroupBytes();
    if (groupBytes == 0) return 0;

    uint64_t h = splitmix64(pa ^ seeds[hash_idx % BloomHashes]);
    size_t off = (h % (groupBytes * 8)) / 8;
    return static_cast<size_t>(group * groupBytes) + off;
}

size_t
ResidentDir::bloomBitIndex(size_t byteOff, int bitSub) const
{
    return byteOff * 8 + static_cast<size_t>(bitSub % 8);
}

bool
ResidentDir::bloomBitTest(uint64_t pa, int hash_idx) const
{
    if (_bloomBitCount == 0) return false;

    int group = bloomGroup(pa);
    size_t byteOff = bloomByteOffset(pa, hash_idx, group);
    if (byteOff >= _bloomBits.size()) return false;

    uint64_t h = splitmix64(pa ^ static_cast<uint64_t>(hash_idx * 0x9e3779b9ULL));
    int bitSub = static_cast<int>(h % 8);

    size_t bitIdx = bloomBitIndex(byteOff, bitSub);
    if (bitIdx >= _bloomBitCount) return false;

    return (_bloomBits[bitIdx / 8] >> (bitIdx % 8)) & 1u;
}

void
ResidentDir::bloomBitSet(uint64_t pa, int hash_idx)
{
    if (_bloomBitCount == 0) return;

    int group = bloomGroup(pa);
    size_t byteOff = bloomByteOffset(pa, hash_idx, group);
    if (byteOff >= _bloomBits.size()) return;

    uint64_t h = splitmix64(pa ^ static_cast<uint64_t>(hash_idx * 0x9e3779b9ULL));
    int bitSub = static_cast<int>(h % 8);

    size_t bitIdx = bloomBitIndex(byteOff, bitSub);
    if (bitIdx >= _bloomBitCount) return;

    _bloomBits[bitIdx / 8] |= static_cast<uint8_t>(1u << (bitIdx % 8));
}

void
ResidentDir::validateCanonical(const UBCCDirEntry& in, uint64_t pa) const
{
    if (UBCCDirEntry::canonicalOneHotRequired(in)) {
        panic_if(__builtin_popcountll(in.sharersMask) != 1,
                 "ResidentDir invalid exclusive entry PA=0x%lx sharers=0x%lx",
                 pa, in.sharersMask);
    }
    if (in.state == UBCCMESIState::G_S) {
        panic_if(in.sharersMask == 0,
                 "ResidentDir invalid shared entry PA=0x%lx sharers=0", pa);
    }
    if (in.state == UBCCMESIState::G_I) {
        panic_if(in.sharersMask != 0,
                 "ResidentDir invalid G_I entry PA=0x%lx sharers=0x%lx", pa,
                 in.sharersMask);
    }
}

void
ResidentDir::encodeEntry(uint64_t pa, const UBCCDirEntry& in, uint64_t& out56) const
{
    validateCanonical(in, pa);
    const uint64_t mesi = static_cast<uint64_t>(in.state) & kMask2;
    const uint64_t dirty = in.residentDirty ? 1ULL : 0ULL;
    const uint64_t sharers = in.sharersMask & kMask16;
    const uint64_t epoch = in.epoch & kMask24;

    // [1:0] MESI, [2] residentDirty, [18:3] sharers16, [42:19] epoch24.
    out56 = (mesi)
          | (dirty << 2)
          | (sharers << 3)
          | (epoch << 19);

    (void)pa;
}

void
ResidentDir::decodeEntry(uint64_t pa, uint64_t packed56, UBCCDirEntry& out) const
{
    out.lineAddr = pa;
    out.state = static_cast<UBCCMESIState>(packed56 & kMask2);
    out.residentDirty = ((packed56 >> 2) & 0x1) != 0;
    out.sharersMask = (packed56 >> 3) & kMask16;
    out.epoch = (packed56 >> 19) & kMask24;
}

bool
ResidentDir::findSlot(uint64_t pa, size_t& slot) const
{
    if (_capacity == 0) {
        return false;
    }

    size_t idx = hashLine(pa);
    uint8_t probe_dist = 0;

    while (_used[idx]) {
        if (_keys[idx] == pa) {
            slot = idx;
            return true;
        }

        // Robin Hood early stop.
        if (_dist[idx] < probe_dist) {
            return false;
        }

        idx = (idx + 1) % _capacity;
        ++probe_dist;
        if (probe_dist >= _capacity) {
            return false;
        }
    }

    return false;
}

bool
ResidentDir::lookup(uint64_t pa, UBCCDirEntry& out) const
{
    size_t slot = 0;
    if (!findSlot(pa, slot)) {
        return false;
    }
    decodeEntry(pa, loadPacked56(slot), out);
    return true;
}

bool
ResidentDir::lookupWithSlot(uint64_t pa, UBCCDirEntry& out, size_t& slot) const
{
    if (!findSlot(pa, slot)) {
        return false;
    }
    decodeEntry(pa, loadPacked56(slot), out);
    return true;
}

bool
ResidentDir::insert(uint64_t pa, const UBCCDirEntry& in)
{
    validateCanonical(in, pa);
    if (_capacity == 0 || _count >= _capacity) {
        return false;
    }

    size_t existed = 0;
    if (findSlot(pa, existed)) {
        return false;
    }

    size_t idx = hashLine(pa);
    uint8_t probe_dist = 0;
    uint64_t cur_key = pa;
    uint64_t cur_packed = 0;
    encodeEntry(pa, in, cur_packed);

    while (_used[idx]) {
        if (_dist[idx] < probe_dist) {
            std::swap(cur_key, _keys[idx]);
            std::swap(probe_dist, _dist[idx]);

            uint64_t slot_packed = loadPacked56(idx);
            storePacked56(idx, cur_packed);
            cur_packed = slot_packed;
        }

        idx = (idx + 1) % _capacity;
        ++probe_dist;
        if (probe_dist >= _capacity) {
            return false;
        }
    }

    _used[idx] = 1;
    _keys[idx] = cur_key;
    _dist[idx] = probe_dist;
    storePacked56(idx, cur_packed);
    _ctrl[idx] = 0;
    ++_count;
    return true;
}

void
ResidentDir::update(uint64_t pa, const UBCCDirEntry& in)
{
    validateCanonical(in, pa);
    size_t slot = 0;
    if (!findSlot(pa, slot)) {
        (void)insert(pa, in);
        return;
    }

    uint64_t packed = 0;
    encodeEntry(pa, in, packed);
    storePacked56(slot, packed);
}

bool
ResidentDir::remove(uint64_t pa)
{
    size_t slot = 0;
    if (!findSlot(pa, slot)) {
        return false;
    }

    size_t cur = slot;
    size_t next = (cur + 1) % _capacity;

    while (_used[next] && _dist[next] > 0) {
        _keys[cur] = _keys[next];
        _used[cur] = 1;
        _dist[cur] = _dist[next] - 1;
        storePacked56(cur, loadPacked56(next));

        cur = next;
        next = (next + 1) % _capacity;
    }

    _used[cur] = 0;
    _keys[cur] = 0;
    _dist[cur] = 0;
    _ctrl[cur] = 0;
    storePacked56(cur, 0);
    --_count;
    return true;
}

bool
ResidentDir::forceRemove(uint64_t pa)
{
    return remove(pa);
}

// ---- Plain Bloom Filter (grouped) ----

bool
ResidentDir::bloomMayContain(uint64_t pa) const
{
    if (_bloomBitCount == 0)
        return false;

    for (int i = 0; i < BloomHashes; ++i) {
        if (!bloomBitTest(pa, i))
            return false;
    }
    return true;
}

void
ResidentDir::bloomInsert(uint64_t pa)
{
    if (_bloomBitCount == 0)
        return;

    int g = bloomGroup(pa);
    for (int i = 0; i < BloomHashes; ++i)
        bloomBitSet(pa, i);

    _groupIndex[g].insert_count++;
}

void
ResidentDir::bloomRemove(uint64_t pa)
{
    int g = bloomGroup(pa);
    _groupIndex[g].stale_delete_count++;
}

void
ResidentDir::bloomClear()
{
    std::fill(_bloomBits.begin(), _bloomBits.end(), 0);
    for (int g = 0; g < BloomGroups; ++g) {
        _groupIndex[g] = GroupIndex();
    }
}

// ---- Reconstruction ----

bool
ResidentDir::shouldReconstructGroup(int g) const
{
    if (g < 0 || g >= BloomGroups) return false;

    const GroupIndex& gi = _groupIndex[g];

    if (gi.insert_count > 0 && gi.insert_count % kReconstructPeriod == 0)
        return true;

    if (gi.live_count > 0 &&
        static_cast<double>(gi.stale_delete_count) / gi.live_count > kReconstructStaleThreshold)
        return true;

    return false;
}

void
ResidentDir::scanResidentForGroup(int g, std::vector<uint8_t>& shadowBF) const
{
    size_t groupBytes = bloomGroupBytes();
    for (size_t i = 0; i < _capacity; ++i) {
        if (!_used[i])
            continue;

        uint64_t pa = _keys[i];
        if (bloomGroup(pa) != g)
            continue;

        uint64_t packed = loadPacked56(i);
        UBCCMESIState st = static_cast<UBCCMESIState>(packed & kMask2);
        bool dirty = ((packed >> 2) & 0x1) != 0;
        if (st == UBCCMESIState::G_I && !dirty)
            continue;

        for (int h = 0; h < BloomHashes; ++h) {
            size_t byteOff = bloomByteOffset(pa, h, g);
            if (byteOff >= groupBytes) continue;

            uint64_t hval = splitmix64(pa ^ static_cast<uint64_t>(h * 0x9e3779b9ULL));
            int bitSub = static_cast<int>(hval % 8);
            size_t bitIdx = bloomBitIndex(byteOff, bitSub);
            if (bitIdx / 8 >= shadowBF.size()) continue;

            shadowBF[bitIdx / 8] |= static_cast<uint8_t>(1u << (bitIdx % 8));
        }
    }
}

void
ResidentDir::reconstructGroup(int g)
{
    if (g < 0 || g >= BloomGroups)
        return;

    size_t groupBytes = bloomGroupBytes();
    if (groupBytes == 0) return;

    std::vector<uint8_t> shadowBF(groupBytes, 0);

    scanResidentForGroup(g, shadowBF);

    size_t groupStart = static_cast<size_t>(g) * groupBytes;
    std::memcpy(&_bloomBits[groupStart], shadowBF.data(), groupBytes);

    _groupIndex[g].stale_delete_count = 0;
}

// ---- Diagnostics ----

double
ResidentDir::estimateFPR(int group) const
{
    size_t n = 0;
    if (group < 0 || group >= BloomGroups) {
        for (int g = 0; g < BloomGroups; ++g)
            n += _groupIndex[g].live_count;
    } else {
        n = _groupIndex[group].live_count;
    }

    if (n == 0) return 0.0;

    size_t m;
    if (group < 0 || group >= BloomGroups) {
        m = _bloomBitCount;
    } else {
        m = bloomGroupBytes() * 8;
    }

    int k = BloomHashes;
    double nm = static_cast<double>(n) / static_cast<double>(m);
    return std::pow(1.0 - std::exp(-static_cast<double>(k) * nm), static_cast<double>(k));
}

// ---- Control Flags (unchanged) ----

uint8_t
ResidentDir::control(size_t slot) const
{
    if (slot >= _ctrl.size()) {
        return 0;
    }
    return _ctrl[slot];
}

void
ResidentDir::setFillPending(uint64_t pa, bool v)
{
    size_t slot = 0;
    if (!findSlot(pa, slot)) {
        return;
    }
    if (v) _ctrl[slot] |= kCtrlFillPending;
    else _ctrl[slot] &= ~kCtrlFillPending;
}

void
ResidentDir::setWbPending(uint64_t pa, bool v)
{
    size_t slot = 0;
    if (!findSlot(pa, slot)) {
        return;
    }
    if (v) _ctrl[slot] |= kCtrlWbPending;
    else _ctrl[slot] &= ~kCtrlWbPending;
}

void
ResidentDir::setPinned(uint64_t pa, bool v)
{
    size_t slot = 0;
    if (!findSlot(pa, slot)) {
        return;
    }
    if (v) _ctrl[slot] |= kCtrlPinned;
    else _ctrl[slot] &= ~kCtrlPinned;
}

bool
ResidentDir::fillPending(uint64_t pa) const
{
    size_t slot = 0;
    if (!findSlot(pa, slot)) {
        return false;
    }
    return (_ctrl[slot] & kCtrlFillPending) != 0;
}

bool
ResidentDir::wbPending(uint64_t pa) const
{
    size_t slot = 0;
    if (!findSlot(pa, slot)) {
        return false;
    }
    return (_ctrl[slot] & kCtrlWbPending) != 0;
}

bool
ResidentDir::pinned(uint64_t pa) const
{
    size_t slot = 0;
    if (!findSlot(pa, slot)) {
        return false;
    }
    return (_ctrl[slot] & kCtrlPinned) != 0;
}

void
ResidentDir::touch(uint64_t pa)
{
    size_t slot = 0;
    if (!findSlot(pa, slot)) {
        return;
    }
    _lruTick++;
    uint8_t age = static_cast<uint8_t>(_lruTick & kCtrlLruMask);
    _ctrl[slot] = static_cast<uint8_t>((_ctrl[slot] & 0x7) | (age << kCtrlLruShift));
}

bool
ResidentDir::hasFreeSlot() const
{
    return _count < _capacity;
}

bool
ResidentDir::pickVictim(uint64_t avoidPa, uint64_t &victimPa, UBCCDirEntry &victim) const
{
    if (_count == 0) {
        return false;
    }

    bool found = false;
    uint8_t bestAge = 0xff;
    size_t bestSlot = 0;
    for (size_t i = 0; i < _capacity; ++i) {
        if (!_used[i]) {
            continue;
        }
        if (_keys[i] == avoidPa) {
            continue;
        }
        if ((_ctrl[i] & kCtrlPinned) != 0) {
            continue;
        }
        uint8_t age = static_cast<uint8_t>((_ctrl[i] >> kCtrlLruShift) & kCtrlLruMask);
        if (!found || age < bestAge) {
            found = true;
            bestAge = age;
            bestSlot = i;
        }
    }

    if (!found) {
        return false;
    }
    victimPa = _keys[bestSlot];
    decodeEntry(victimPa, loadPacked56(bestSlot), victim);
    return true;
}

void
ResidentDir::clear()
{
    _count = 0;
    _lruTick = 0;
    std::memset(_buf, 0, sizeof(_buf));
    std::fill(_keys.begin(), _keys.end(), 0);
    std::fill(_used.begin(), _used.end(), 0);
    std::fill(_dist.begin(), _dist.end(), 0);
    std::fill(_ctrl.begin(), _ctrl.end(), 0);
    bloomClear();
}

} // namespace ruby
} // namespace gem5
