# EPRNFController SimObject params

from m5.params import *

from .EPController import EPController


class EPRNFController(EPController):
    type = "EPRNFController"
    cxx_header = "mem/ruby/protocol/chi/ep/EPRNFController.hh"
    cxx_class = "gem5::ruby::EPRNFController"
    abstract = False

    addr_ranges = VectorParam.AddrRange([], "Address ranges served")
    ep_backend = Param.EPBackend(NULL, "EPBackend for this endpoint")
