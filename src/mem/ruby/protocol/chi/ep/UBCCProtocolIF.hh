#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBCCPROTOCOLIF_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBCCPROTOCOLIF_HH__

#include <cstdint>
#include <functional>

namespace gem5 { namespace ruby {

struct DataBlock;

enum class UBCC_OuterReqType {
    GlobalReadShared,
    GlobalReadUnique,
    GlobalWriteback,
    GlobalEvict,
    GlobalInvalidate
};

enum class UBCC_OuterGrantType {
    GlobalGrantShared,
    GlobalGrantExclusive,
    GlobalGrantModified
};

enum class GrantDataSource {
    HomeMemory,
    RecallBuffer,
    NoData
};

/**
 * Abstract host interface for UBCCController backstore I/O.
 * When running inside gem5: EPBackend implements this.
 * When running standalone: ubio implements this via Port send/recv.
 */
class UbioHostIf {
public:
    virtual ~UbioHostIf() = default;
    virtual void hostIssueBackstoreRead(uint64_t pa) = 0;
    virtual void hostIssueBackstoreWrite(uint64_t pa) = 0;
    virtual void hostIssueBackstoreDelete(uint64_t pa) = 0;
};

}} // namespace gem5::ruby

#endif
