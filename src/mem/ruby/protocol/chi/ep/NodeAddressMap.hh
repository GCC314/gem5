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
    static constexpr int NODE_ADDR_SHIFT = 40;
    static constexpr int MAX_NODES = 3;

    NodeAddressMap(int num_nodes, uint64_t seg_size);

    uint64_t nodeBase(int node_id) const {
        return static_cast<uint64_t>(node_id) << NODE_ADDR_SHIFT;
    }

    bool isDsm(int node_id, uint64_t pa) const {
        uint64_t base = nodeBase(node_id);
        uint64_t dsm_start = base + 2 * _segSize;
        uint64_t dsm_end = dsm_start + _numNodes * _segSize;
        return pa >= dsm_start && pa < dsm_end;
    }

    int homeNode(int node_id, uint64_t pa) const {
        if (!isDsm(node_id, pa)) return -1;
        uint64_t base = nodeBase(node_id);
        return static_cast<int>((pa - base - 2 * _segSize) / _segSize);
    }

    bool isDsmLocal(int node_id, uint64_t pa) const {
        return isDsm(node_id, pa) && homeNode(node_id, pa) == node_id;
    }

    bool isDsmRemote(int node_id, uint64_t pa) const {
        return isDsm(node_id, pa) && homeNode(node_id, pa) != node_id;
    }

    int srcNodeId(uint64_t pa) const {
        return static_cast<int>(pa >> NODE_ADDR_SHIFT);
    }

    uint64_t dsmOffset(uint64_t pa) const {
        return pa & (_segSize - 1);
    }

    uint64_t buildDsmPA(int tgt_node, int home_node, uint64_t offset) const {
        return nodeBase(tgt_node) + 2 * _segSize
               + home_node * _segSize + offset;
    }

    uint64_t segSize() const { return _segSize; }
    int numNodes() const { return _numNodes; }

  private:
    int _numNodes;
    uint64_t _segSize;
};

} // namespace ruby
} // namespace gem5

#endif // __MEM_RUBY_PROTOCOL_CHI_EP_NODEADDRESSMAP_HH__
