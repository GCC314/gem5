# EPRNFController SimObject params

from m5.params import *

from .EPController import EPController


class EPRNFController(EPController):
    type = "EPRNFController"
    cxx_header = "mem/ruby/protocol/chi/ep/EPRNFController.hh"
    cxx_class = "gem5::ruby::EPRNFController"
    abstract = False

    ep_backend = Param.EPBackend(NULL, "EPBackend for this endpoint")
    compack_retry_cycles = Param.UInt64(100000, "CompAck retry interval in cycles")
    wakeup_retry_cycles = Param.UInt64(1000000, "Wakeup retry interval in cycles")
    upgrade_retry_min_cycles = Param.UInt64(10000, "Held-upgrade retry backoff min cycles")
    upgrade_retry_max_cycles = Param.UInt64(400000, "Held-upgrade retry backoff max cycles")
