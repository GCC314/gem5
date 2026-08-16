from .EPController import EPController
from m5.params import *


class MetaRNFController(EPController):
    type = "MetaRNFController"
    cxx_header = "mem/ruby/protocol/chi/ep/MetaRNFController.hh"
    cxx_class = "gem5::ruby::MetaRNFController"

    socket_id = Param.Int(0, "Socket ID for this controller")
    metadata_private_range = Param.AddrRange(
        AddrRange(0, size="16MiB"),
        "private metadata DRAM range"
    )
    flight_slots = Param.Int(8, "Maximum concurrent metadata page requests")
