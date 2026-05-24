#ifndef __MEM_RUBY_PROTOCOL_CHI_EP_NODEADDRESSMAP_HH__
#define __MEM_RUBY_PROTOCOL_CHI_EP_NODEADDRESSMAP_HH__

#include <cstdint>

namespace gem5
{

namespace ruby
{

class NodeAddressMap
{
  public:
    NodeAddressMap(int num_nodes, uint64_t seg_size);

    bool isDsm(uint64_t pa) const {
        return pa >= _dsmBase && pa < _dsmEnd;
    }

    int homeNode(uint64_t pa) const {
        if (!isDsm(pa)) return -1;
        return static_cast<int>((pa - _dsmBase) / _segSize);
    }

    bool isDsmLocal(int node_id, uint64_t pa) const {
        return isDsm(pa) && homeNode(pa) == node_id;
    }

    bool isDsmRemote(int node_id, uint64_t pa) const {
        return isDsm(pa) && homeNode(pa) != node_id;
    }

    uint64_t dsmBase() const { return _dsmBase; }
    uint64_t dsmEnd() const { return _dsmEnd; }
    uint64_t segSize() const { return _segSize; }
    int numNodes() const { return _numNodes; }

  private:
    int _numNodes;
    uint64_t _segSize;
    uint64_t _dsmBase;
    uint64_t _dsmEnd;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_NODEADDRESSMAP_HH__
