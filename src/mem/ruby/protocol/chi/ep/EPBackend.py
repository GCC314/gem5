# EPBackend SimObject

from m5.SimObject import SimObject
from m5.params import *


class EPBackend(SimObject):
    type = "EPBackend"
    cxx_header = "mem/ruby/protocol/chi/ep/EPBackend.hh"
    cxx_class = "gem5::ruby::EPBackend"

    node_id = Param.Int(0, "Node ID for this backend")
    ruby_system = Param.RubySystem("")
    ub_adapter = Param.UBAdapter(NULL, "UBAdapter for message-path UBCC access")
    ubcc_epoch_bits = Param.UInt32(64, "UBCC committed epoch width in bits")
    meta_rnf = Param.MetaRNFController(NULL, "metadata async service stub")
    ubcc_bf_bytes = Param.UInt32(64 * 1024, "resident dir counting bloom bytes")
    ubcc_force_resident_entries = Param.UInt32(
        0, "force resident entries for tests (0=auto)"
    )
