# EPBackend SimObject

from m5.SimObject import SimObject
from m5.params import *


class EPBackend(SimObject):
    type = "EPBackend"
    cxx_header = "mem/ruby/protocol/chi/ep/EPBackend.hh"
    cxx_class = "gem5::ruby::EPBackend"

    node_id = Param.Int(0, "Node ID for this backend")
    ruby_system = Param.RubySystem("")
    ub_adapter = Param.UBAdapter(NULL, "UBAdapter for message-path UBCC access (legacy, use ub_adapters)")
    ub_adapters = VectorParam.UBAdapter([], "per-socket UBAdapters (index == socket_id); "
                                            "becomes a child so each adapter's init() runs and "
                                            "binds its own ubio Port")
    num_sockets = Param.Int(1, "Number of sockets per node")
    num_nodes = Param.Int(3, "Total number of nodes in the system")
    ubcc_epoch_bits = Param.UInt32(64, "UBCC committed epoch width in bits")
    meta_rnf = Param.MetaRNFController(NULL, "metadata async service stub")
    ubcc_bf_bytes = Param.UInt32(64 * 1024, "resident dir counting bloom bytes")
    ubcc_force_resident_entries = Param.UInt32(
        0, "force resident entries for tests (0=auto)"
    )
    metadata_private_base = Param.Addr(0, "metadata private DRAM base")
    metadata_private_size = Param.MemorySize("128MiB", "metadata private DRAM size")
    silent_upgrade = Param.Bool(False, "silent upgrade: complete write upgrade locally when requester holds E/M")
    direct_fwd = Param.Bool(False, "enable direct-forward (owner->requester data bypass)")
