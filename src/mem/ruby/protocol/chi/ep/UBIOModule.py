# UBIOModule SimObject

from m5.SimObject import SimObject
from m5.params import *


class UBIOModule(SimObject):
    type = "UBIOModule"
    cxx_header = "mem/ruby/protocol/chi/ep/UBIOModule.hh"
    cxx_class = "gem5::ruby::UBIOModule"

    node_id = Param.Int(0, "Node ID for this router")
    socket_id = Param.Int(0, "Socket ID for this router")
    ub_msg_latency = Param.Latency("0ns", "Per-hop message queue latency")

    # Debug-only fault injection: comma-separated fault rules.
    # Format: "name:type:src:dst:pa:action[:delayTicks[:matchCount]]"
    #   type = msg type name (e.g., "ClearReq"); "*" = any
    #   src  = source node (-1 = any)
    #   dst  = dest node (-1 = any)
    #   pa   = line PA in hex (0 = any)
    #   action = drop | delay | dup
    #   delayTicks = ticks to delay (only for delay action)
    #   matchCount = max times to fire (0 = infinite)
    # Example: "tc47_drop_clear:ClearReq:-1:-1:0:drop"
    # Rules are applied by the Python config helper configure_fault_rules().
    fault_rules = VectorParam.String([], "Debug fault injection rules")
