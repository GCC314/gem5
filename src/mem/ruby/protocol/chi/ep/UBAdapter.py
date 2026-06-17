# UBAdapter SimObject

from m5.SimObject import SimObject
from m5.params import *


class UBAdapter(SimObject):
    type = "UBAdapter"
    cxx_header = "mem/ruby/protocol/chi/ep/UBAdapter.hh"
    cxx_class = "gem5::ruby::UBAdapter"

    node_id = Param.Int(0, "Node ID for this adapter")
    router = Param.UBRouter(NULL, "Local UBRouter for message dispatch")
