#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__

#include <cstdint>
#include <map>
#include <queue>

namespace gem5
{

namespace ruby
{

class UBCCController
{
  public:
    UBCCController(int node_id);
    ~UBCCController();

    int nodeId() const { return _nodeId; }

    void wakeup();

  private:
    const int _nodeId;

    struct OuterEntry {
        uint64_t addr;
        uint64_t state;
        uint64_t tick;
    };
    std::map<uint64_t, OuterEntry> _metadata;

    struct OuterQueueEntry {
        uint64_t addr;
        uint64_t tick;
        int latency;
    };
    std::queue<OuterQueueEntry> _outerQueue;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_UBCCCONTROLLER_HH__
