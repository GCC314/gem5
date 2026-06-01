# EPSNFController SimObject params

from m5.params import *

from .EPController import EPController


class EPSNFController(EPController):
    type = "EPSNFController"
    cxx_header = "mem/ruby/protocol/chi/ep/EPSNFController.hh"
    cxx_class = "gem5::ruby::EPSNFController"
    abstract = False

    ep_backend = Param.EPBackend(NULL, "EPBackend for this endpoint")
