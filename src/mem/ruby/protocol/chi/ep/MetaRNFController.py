from m5.SimObject import SimObject
from m5.params import *


class MetaRNFController(SimObject):
    type = "MetaRNFController"
    cxx_header = "mem/ruby/protocol/chi/ep/MetaRNFController.hh"
    cxx_class = "gem5::ruby::MetaRNFController"

    read_latency_ticks = Param.UInt64(8000, "metadata read latency")
    write_latency_ticks = Param.UInt64(7500, "metadata write latency")
    delete_latency_ticks = Param.UInt64(7500, "metadata delete latency")
