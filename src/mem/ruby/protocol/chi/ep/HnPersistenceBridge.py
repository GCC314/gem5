from m5.params import NULL, Param
from m5.SimObject import SimObject


class HnPersistenceBridge(SimObject):
    type = "HnPersistenceBridge"
    cxx_header = "mem/ruby/protocol/chi/ep/HnPersistenceBridge.hh"
    cxx_class = "gem5::ruby::HnPersistenceBridge"

    ep_backend = Param.EPBackend(NULL, "Owning project EPBackend")
