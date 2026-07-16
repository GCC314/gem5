# UBAdapter SimObject

from m5.SimObject import SimObject
from m5.params import *


class UBAdapter(SimObject):
    type = "UBAdapter"
    cxx_header = "mem/ruby/protocol/chi/ep/UBAdapter.hh"
    cxx_class = "gem5::ruby::UBAdapter"

    node_id = Param.Int(0, "Node ID for this adapter")
    socket_id = Param.Int(0, "Socket ID for this adapter")
    num_nodes = Param.Int(3, "Total number of nodes in the system")
    num_sockets = Param.Int(1, "Number of sockets per node")
    # -1 = build all nodes in this process (legacy single-process mode);
    # otherwise this process owns exactly one node and only that node's Port binds.
    local_node = Param.Int(-1, "Node this gem5 process owns (-1 = all)")
    wait_cap = Param.UInt64(2000000, "Wait cap for response polling (cycles)")
