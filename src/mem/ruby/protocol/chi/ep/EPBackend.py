# EPBackend SimObject

from m5.SimObject import SimObject
from m5.params import *


class EPBackend(SimObject):
    type = "EPBackend"
    cxx_header = "mem/ruby/protocol/chi/ep/EPBackend.hh"
    cxx_class = "gem5::ruby::EPBackend"

    node_id = Param.Int(0, "Node ID for this backend")
