# EPSNFController SimObject params

from m5.params import *

from .EPController import EPController


class EPSNFController(EPController):
    type = "EPSNFController"
    cxx_header = "mem/ruby/protocol/chi/ep/EPSNFController.hh"
    cxx_class = "gem5::ruby::EPSNFController"
    abstract = False

    ep_backend = Param.EPBackend(NULL, "EPBackend for this endpoint")
    socket_id = Param.Int(0, "v4-dual-socket: socket index")
    retry_cycles = Param.UInt64(20000, "Retry cycle count after BUSY grant")
    delta_noc_cycles = Param.UInt64(0, "Cross-socket NoC extra latency in cycles")
