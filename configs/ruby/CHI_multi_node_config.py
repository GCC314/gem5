"""
CHI multi-node configuration for domain isolation.

Creates N logical CHI domains in a single RubySystem using contiguous,
non-interleaved HN-F address range partitioning. Each node has its own
RN-F cluster, HN-F, and SN-F. Cross-node ordinary CHI message isolation
is achieved through address-range routing and downstream filtering.

Used as --chi-config replacement. Set --num-l3caches=N --num-dirs=N.
"""

import math

import m5
from m5.objects import *

from ruby.CHI_config import (
    L1ICache, L1DCache, L2Cache, Versions, NoC_Params,
    CHI_Node, CHI_L1Controller, CHI_L2Controller, CHI_HNFController,
    CPUSequencerWrapper, CHI_MN,
    CHI_SNF_MainMem as _BaseSNF,
    CHI_SNF_BootMem, CHI_RNI_DMA, CHI_RNI_IO,
)

CORES_PER_NODE = 2

_snf_node_counter = 0


def _set_node_ids(controllers, node_id):
    for c in controllers:
        c._node_id = node_id


class MultiNodeCHI_RNF(CHI_Node):
    """Per-node Request Node with cluster-shared L2."""

    def __init__(self, cpus, ruby_system,
                 l1Icache_type, l1Dcache_type, cache_line_size, node_id,
                 l1Iprefetcher_type=None, l1Dprefetcher_type=None):
        super().__init__(ruby_system)
        self._block_size_bits = int(math.log(cache_line_size, 2))
        self._seqs, self._cntrls, self._ll_cntrls = [], [], []
        self._cpus, self._node_id = cpus, node_id

        for cpu in cpus:
            cpu.inst_sequencer = RubySequencer(
                version=Versions.getSeqId(), ruby_system=ruby_system)
            cpu.data_sequencer = RubySequencer(
                version=Versions.getSeqId(), ruby_system=ruby_system)
            self._seqs.append(
                CPUSequencerWrapper(cpu.inst_sequencer, cpu.data_sequencer))

            l1i_cache = l1Icache_type(
                start_index_bit=self._block_size_bits, is_icache=True)
            l1d_cache = l1Dcache_type(
                start_index_bit=self._block_size_bits, is_icache=False)
            l1i_pf = l1Iprefetcher_type() if l1Iprefetcher_type else NULL
            l1d_pf = l1Dprefetcher_type() if l1Dprefetcher_type else NULL

            cpu.l1i = CHI_L1Controller(
                ruby_system, cpu.inst_sequencer, l1i_cache, l1i_pf)
            cpu.l1d = CHI_L1Controller(
                ruby_system, cpu.data_sequencer, l1d_cache, l1d_pf)
            cpu.inst_sequencer.dcache = NULL
            cpu.data_sequencer.dcache = cpu.l1d.cache
            cpu.l1d.sc_lock_enabled = True
            cpu._ll_cntrls = [cpu.l1i, cpu.l1d]
            for c in cpu._ll_cntrls:
                self._cntrls.append(c)
                self.connectController(c)

        _set_node_ids(self._cntrls, node_id)

    def addSharedL2Cache(self, cache_type, pf_type=None):
        self._ll_cntrls = []
        l2_cache = cache_type(
            start_index_bit=self._block_size_bits, is_icache=False)
        l2_pf = pf_type() if pf_type else NULL
        self.shared_l2 = CHI_L2Controller(
            self._ruby_system, l2_cache, l2_pf)
        self.shared_l2._node_id = self._node_id
        self._cntrls.append(self.shared_l2)
        self.connectController(self.shared_l2)
        self._ll_cntrls.append(self.shared_l2)
        for cpu in self._cpus:
            for c in cpu._ll_cntrls:
                c.downstream_destinations = [self.shared_l2]
            cpu._ll_cntrls = [self.shared_l2]

    def getSequencers(self):
        return self._seqs

    def getAllControllers(self):
        return self._cntrls

    def getNetworkSideControllers(self):
        return self._cntrls

    def setDownstream(self, cntrls):
        # Default: all HN-Fs. Node isolation is via address range routing.
        # Enable strict filtering by setting _strict_downstream=True
        strict = getattr(self, '_strict_downstream', False)
        if strict:
            cntrls = [c for c in cntrls
                      if getattr(c, '_node_id', -1) == self._node_id]
        for c in self._ll_cntrls:
            c.downstream_destinations = cntrls

    @classmethod
    def generate(cls, options, ruby_system, cpus):
        cores_per = getattr(options, 'cores_per_node', CORES_PER_NODE)
        num_nodes = len(cpus) // cores_per
        if len(cpus) % cores_per != 0:
            m5.fatal(f"CPU count ({len(cpus)}) not multiple of "
                     f"cores_per_node ({cores_per})")
        rnfs = []
        for ni in range(num_nodes):
            ncpus = cpus[ni * cores_per:(ni + 1) * cores_per]
            rnf = cls(ncpus, ruby_system,
                      L1ICache(size=options.l1i_size, assoc=options.l1i_assoc),
                      L1DCache(size=options.l1d_size, assoc=options.l1d_assoc),
                      options.cacheline_size, node_id=ni)
            rnf.addSharedL2Cache(
                L2Cache(size=options.l2_size, assoc=options.l2_assoc))
            rnfs.append(rnf)
        return rnfs


class MultiNodeCHI_HNF(CHI_Node):
    """Per-node Home Node with contiguous address partition."""

    _addr_ranges = {}

    @classmethod
    def createAddrRanges(cls, sys_mem_ranges, cache_line_size, hnfs):
        """Contiguous partition: HN-F i gets [i*chunk, (i+1)*chunk)."""
        num_nodes = len(hnfs)
        block_bits = int(math.log(cache_line_size, 2))
        total = sum(r.size() for r in sys_mem_ranges)
        chunk = total // num_nodes
        chunk = (chunk >> block_bits) << block_bits
        cls._addr_ranges = {}
        for ni in hnfs:
            start = ni * chunk
            cls._addr_ranges[ni] = (
                [AddrRange(start, size=chunk)], block_bits - 1)

    @classmethod
    def getAddrRanges(cls, hnf_idx):
        return cls._addr_ranges[hnf_idx]

    def __init__(self, hnf_idx, ruby_system, llcache_type, parent):
        super().__init__(ruby_system)
        addr_ranges, intlvHighBit = self.getAddrRanges(hnf_idx)
        ll_cache = llcache_type(start_index_bit=intlvHighBit + 1)
        self._cntrl = CHI_HNFController(
            ruby_system, ll_cache, NULL, addr_ranges)
        self._cntrl._node_id = hnf_idx
        self._node_id = hnf_idx
        if parent is None:
            self.cntrl = self._cntrl
        else:
            parent.cntrl = self._cntrl
        self.connectController(self._cntrl)

    def getAllControllers(self):
        return [self._cntrl]

    def getNetworkSideControllers(self):
        return [self._cntrl]

    def setDownstream(self, cntrls):
        strict = getattr(self, '_strict_downstream', False)
        if strict:
            cntrls = [c for c in cntrls
                      if getattr(c, '_node_id', -1) == self._node_id]
        super().setDownstream(cntrls)


class MultiNodeCHI_SNF(_BaseSNF):
    """Per-node memory controller with auto-assigned node_id."""

    _next_id = 0

    def __init__(self, ruby_system, parent, mem_ctrl=None):
        super().__init__(ruby_system, parent, mem_ctrl)
        self._cntrl._node_id = MultiNodeCHI_SNF._next_id
        self._node_id = MultiNodeCHI_SNF._next_id
        MultiNodeCHI_SNF._next_id += 1


# Override for --chi-config
CHI_RNF = MultiNodeCHI_RNF
CHI_HNF = MultiNodeCHI_HNF
CHI_SNF_MainMem = MultiNodeCHI_SNF
