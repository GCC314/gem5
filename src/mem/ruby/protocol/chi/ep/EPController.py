# Copyright (c) 2024 UBCC Project

from m5.params import *

from m5.objects.CHIGeneric import CHIGenericController as CHIGenericParent


class EPRNFController(CHIGenericParent):
    type = "EPRNFController"
    cxx_header = "mem/ruby/protocol/chi/ep/EPRNFController.hh"
    cxx_class = "gem5::ruby::EPRNFController"

    node_id = Param.Int(0, "Node ID of this EP-RNF controller")


class EPSNFController(CHIGenericParent):
    type = "EPSNFController"
    cxx_header = "mem/ruby/protocol/chi/ep/EPSNFController.hh"
    cxx_class = "gem5::ruby::EPSNFController"

    node_id = Param.Int(0, "Node ID of this EP-SNF controller")
