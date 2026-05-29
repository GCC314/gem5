# Basic Framework Configuration for UBCC
# N=3 nodes, L=2 cores per cluster, D=2 clusters per node
# SegSize = 128MB

import math

import m5
from m5.objects import *

from .CHI_config import (
    CHI_Node, CHI_L1Controller, CHI_L2Controller,
    CHI_HNFController, CHI_SNF_Base, CHI_SNF_MainMem,
    L1ICache, L1DCache, L2Cache,
    Versions, CPUSequencerWrapper,
    TriggerMessageBuffer, OrderedTriggerMessageBuffer,
    Base_CHI_Cache_Controller,
)

DEFAULT_N = 3
DEFAULT_L = 2
DEFAULT_D = 2
DEFAULT_SEG_SIZE = 128 * 1024 * 1024
NODE_ADDR_SHIFT = 40

def _node_base(node_id):
    return node_id << NODE_ADDR_SHIFT


class NodeAddressMap:
    """Per-node PA classification helper.
    Each node i has PA space at [i<<40, i<<40 + 5*SegSize).
    DSM_k in node i's view is at offset 2*SegSize + k*SegSize.
    """
    def __init__(self, num_nodes=DEFAULT_N, seg_size=DEFAULT_SEG_SIZE):
        assert num_nodes == 3
        self.num_nodes = num_nodes
        self.seg_size = seg_size
        self.node_shift = NODE_ADDR_SHIFT

    def nodeBase(self, node_id):
        return node_id << self.node_shift

    def dsmLocalBase(self, node_id):
        return self.nodeBase(node_id) + 2 * self.seg_size

    def isDsm(self, node_id, pa):
        """Check if pa is in node_id's DSM window."""
        base = self.nodeBase(node_id)
        dsm_start = base + 2 * self.seg_size
        dsm_end = dsm_start + self.num_nodes * self.seg_size
        return dsm_start <= pa < dsm_end

    def homeNode(self, node_id, pa):
        """Extract DSM home node from a pa in node_id's view."""
        if not self.isDsm(node_id, pa):
            return -1
        base = self.nodeBase(node_id)
        return (pa - base - 2 * self.seg_size) // self.seg_size

    def isDsmLocal(self, node_id, pa):
        if not self.isDsm(node_id, pa):
            return False
        return self.homeNode(node_id, pa) == node_id

    def isDsmRemote(self, node_id, pa):
        if not self.isDsm(node_id, pa):
            return False
        return self.homeNode(node_id, pa) != node_id

    def srcNodeId(self, pa):
        return (pa >> self.node_shift) & ((1 << 8) - 1)

    def dsmOffset(self, pa):
        return pa & (self.seg_size - 1)

    def buildDsmPA(self, tgt_node_id, home_node_id, offset):
        return (self.nodeBase(tgt_node_id)
                + 2 * self.seg_size
                + home_node_id * self.seg_size
                + offset)


class NodeConfig:
    def __init__(self, node_id, num_nodes=DEFAULT_N, seg_size=DEFAULT_SEG_SIZE):
        self.node_id = node_id
        self.num_nodes = num_nodes
        self.seg_size = seg_size
        self.phy_base = _node_base(node_id)
        self.addr_map = NodeAddressMap(num_nodes, seg_size)
        self.local_private_base = self.phy_base + 0 * seg_size
        self.local_private_end   = self.phy_base + 1 * seg_size
        self.ubcc_exclusive_base = self.phy_base + 1 * seg_size
        self.ubcc_exclusive_end  = self.phy_base + 2 * seg_size

    @property
    def local_private_range(self):
        return AddrRange(self.local_private_base, size=self.seg_size)

    @property
    def ubcc_exclusive_range(self):
        return AddrRange(self.ubcc_exclusive_base, size=self.seg_size)

    @staticmethod
    def dsm_global_range(seg_size=DEFAULT_SEG_SIZE):
        return AddrRange(2 * seg_size, size=3 * seg_size)

    @staticmethod
    def dsm_local_range(node_id, seg_size=DEFAULT_SEG_SIZE, phy_base=0):
        """DSM range for lines homed at this node, in the node's own PA view.
        Each node homes DSM_<node_id> at PhyBase + (2+node_id)*SEG_SIZE.
        """
        base = phy_base + (2 + node_id) * seg_size
        return AddrRange(base, size=seg_size)

    @staticmethod
    def dsm_range_for(node_id, seg_size=DEFAULT_SEG_SIZE, phy_base=0):
        """DSM_k range in the caller's node PA space."""
        base = phy_base + (2 + node_id) * seg_size
        return AddrRange(base, size=seg_size)

def get_all_system_ranges(seg_size=DEFAULT_SEG_SIZE, num_nodes=DEFAULT_N):
    ranges = []
    for n in range(num_nodes):
        cfg = NodeConfig(n, num_nodes, seg_size)
        ranges.append(cfg.local_private_range)
        ranges.append(cfg.ubcc_exclusive_range)
    ranges.append(NodeConfig.dsm_global_range(seg_size))
    return ranges


class ClusterCHI_RNF(CHI_Node):
    def __init__(self, cpus, ruby_system, cache_line_size,
                 l1Icache_type=None, l1Dcache_type=None,
                 l1i_assoc=2, l1d_assoc=2,
                 l1i_size="32kB", l1d_size="32kB",
                 l2_assoc=8, l2_size="256kB"):
        super().__init__(ruby_system)

        if l1Icache_type is None:
            l1Icache_type = L1ICache
        if l1Dcache_type is None:
            l1Dcache_type = L1DCache

        self._block_size_bits = int(math.log(cache_line_size, 2))
        self._seqs = []
        self._cntrls = []
        self._ll_cntrls = []
        self._cpus = cpus

        self._l2_assoc = l2_assoc
        self._l2_size = l2_size

        for cpu in self._cpus:
            cpu.inst_sequencer = RubySequencer(
                version=Versions.getSeqId(), ruby_system=ruby_system)
            cpu.data_sequencer = RubySequencer(
                version=Versions.getSeqId(), ruby_system=ruby_system)
            self._seqs.append(
                CPUSequencerWrapper(cpu.inst_sequencer, cpu.data_sequencer))
            l1i_cache = l1Icache_type(
                start_index_bit=self._block_size_bits, is_icache=True,
                assoc=l1i_assoc, size=l1i_size)
            l1d_cache = l1Dcache_type(
                start_index_bit=self._block_size_bits, is_icache=False,
                assoc=l1d_assoc, size=l1d_size)
            cpu.l1i = CHI_L1Controller(
                ruby_system, cpu.inst_sequencer, l1i_cache, NULL)
            cpu.l1d = CHI_L1Controller(
                ruby_system, cpu.data_sequencer, l1d_cache, NULL)
            cpu.inst_sequencer.dcache = NULL
            cpu.data_sequencer.dcache = cpu.l1d.cache
            cpu.l1d.sc_lock_enabled = True
            cpu._ll_cntrls = [cpu.l1i, cpu.l1d]
            for c in cpu._ll_cntrls:
                self._cntrls.append(c)
                self.connectController(c)
                self._ll_cntrls.append(c)

    def addPrivL2Cache(self, cache_type=L2Cache):
        self._ll_cntrls = []
        for cpu in self._cpus:
            l2_cache = cache_type(
                start_index_bit=self._block_size_bits, is_icache=False,
                assoc=self._l2_assoc, size=self._l2_size)
            cpu.l2 = CHI_L2Controller(self._ruby_system, l2_cache, NULL)
            self._cntrls.append(cpu.l2)
            self.connectController(cpu.l2)
            self._ll_cntrls.append(cpu.l2)
            for c in cpu._ll_cntrls:
                c.downstream_destinations = [cpu.l2]
            cpu._ll_cntrls = [cpu.l2]

    def getSequencers(self):
        return self._seqs

    def getAllControllers(self):
        return self._cntrls

    def getNetworkSideControllers(self):
        return self._cntrls

    def setDownstream(self, cntrls):
        for c in self._ll_cntrls:
            c.downstream_destinations = cntrls

    def getCpus(self):
        return self._cpus





class UBBCL_SNF(CHI_SNF_Base):
    def __init__(self, ruby_system, parent, mem_ctrl, addr_ranges=None):
        super().__init__(ruby_system, parent)
        if mem_ctrl:
            self._cntrl.memory_out_port = mem_ctrl.port
        if addr_ranges:
            self._cntrl.addr_ranges = addr_ranges
        elif mem_ctrl:
            self._cntrl.addr_ranges = self.getMemRange(mem_ctrl)

    def set_addr_ranges(self, addr_ranges):
        self._cntrl.addr_ranges = addr_ranges

    def set_memory_port(self, mem_ctrl):
        self._cntrl.memory_out_port = mem_ctrl.port


class UBBCDL_SNF(UBBCL_SNF):
    pass


class NodeTopology:
    def __init__(self, node_id, node_config, ruby_system,
                 cls_per_node, cpus_per_cluster, cache_line_size):
        self.node_id = node_id
        self.node_config = node_config
        self.ruby_system = ruby_system

        self.clusters = []
        self.hnf = None
        self.l_snf = None
        self.dl_snf = None
        self.ep_snf = None
        self.ep_rnf = None
        self.ep_backend = None


class EPNodeWrapper(CHI_Node):
    def __init__(self, ruby_system):
        super().__init__(ruby_system)
        self._cntrl = None

    def getAllControllers(self):
        return [self._cntrl] if self._cntrl else []

    def getNetworkSideControllers(self):
        return [self._cntrl] if self._cntrl else []

    def setController(self, cntrl):
        setattr(self, 'ctrl', cntrl)
        self._cntrl = cntrl


class HNNodeWrapper(CHI_Node):
    def __init__(self, ruby_system):
        super().__init__(ruby_system)
        self._cntrl = None

    def getAllControllers(self):
        return [self._cntrl] if self._cntrl else []

    def getNetworkSideControllers(self):
        return [self._cntrl] if self._cntrl else []

    def setController(self, cntrl):
        setattr(self, 'ctrl', cntrl)
        self._cntrl = cntrl
