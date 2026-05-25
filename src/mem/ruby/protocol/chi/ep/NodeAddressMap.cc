#include "mem/ruby/protocol/chi/ep/NodeAddressMap.hh"

namespace gem5
{

namespace ruby
{

NodeAddressMap::NodeAddressMap(int num_nodes, uint64_t seg_size)
  : _numNodes(num_nodes),
    _segSize(seg_size)
{
}

} // namespace ruby
} // namespace gem5
