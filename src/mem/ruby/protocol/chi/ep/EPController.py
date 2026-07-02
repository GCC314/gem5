# EP Controller base SimObject (intermediate abstract base)
# Does NOT import CHIGenericController to avoid build-time dependency issues.
# Inlines all CHIGenericController params directly.

from m5.objects.Controller import RubyController
from m5.objects.MessageBuffer import MessageBuffer
from m5.params import *


class EPController(RubyController):
    type = "EPController"
    cxx_header = "mem/ruby/protocol/chi/ep/EPRNFController.hh"
    cxx_class = "gem5::ruby::EPController"
    abstract = True

    node_id = Param.Int(0, "Node ID for this endpoint")
    num_nodes = Param.Int(3, "Total number of nodes in the system")

    data_channel_size = Param.Int("")
    reqOut = Param.MessageBuffer("")
    snpOut = Param.MessageBuffer("")
    rspOut = Param.MessageBuffer("")
    datOut = Param.MessageBuffer("")
    reqIn = Param.MessageBuffer("")
    snpIn = Param.MessageBuffer("")
    rspIn = Param.MessageBuffer("")
    datIn = Param.MessageBuffer("")
