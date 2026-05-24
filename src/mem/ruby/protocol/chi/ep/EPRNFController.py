# EPRNFController SimObject params

from m5.params import *

from .EPController import EPController


class EPRNFController(EPController):
    type = "EPRNFController"
    cxx_header = "mem/ruby/protocol/chi/ep/EPRNFController.hh"
    cxx_class = "gem5::ruby::EPRNFController"
    abstract = False

    addr_range = Param.AddrRange(AddrRange(0, size="1MiB"),
                                 "Address range served")
