# UBRouter SimObject

from m5.SimObject import SimObject
from m5.params import *


class UBRouter(SimObject):
    type = "UBRouter"
    cxx_header = "mem/ruby/protocol/chi/ep/UBRouter.hh"
    cxx_class = "gem5::ruby::UBRouter"

    node_id = Param.Int(0, "Node ID for this router")
    ub_msg_latency = Param.Latency("0ns", "Per-hop message queue latency")
