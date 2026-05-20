"""
CHI multi-node configuration for STRICT domain isolation.

Each logical node has independent RN-F, HN-F, SN-F, EP-RNF, and EP-SNF
controller sets with enforced per-node downstream filtering.

EP controllers (M3+): EP-RNF serves as snoop destination; EP-SNF handles
DSM Remote ReadNoSnp. Created by create_ep_controllers().

Used with --chi-config. Requires --num-l3caches=N --num-dirs=N (power-of-2).
"""

import math

import m5
from m5.objects import *
from m5.util import fatal

from ruby.CHI_config import (
    L1ICache, L1DCache, L2Cache, Versions, NoC_Params,
    CHI_Node, CHI_L1Controller, CHI_L2Controller, CHI_HNFController,
    CPUSequencerWrapper, CHI_MN,
    CHI_SNF_MainMem as _BaseSNF,
    CHI_SNF_BootMem, CHI_RNI_DMA, CHI_RNI_IO,
)

CORES_PER_NODE = 2


def _make_ep_post_hook(num_nodes, data_channel_size=32):
    """Return a post-hook function that creates EP controllers in topology."""
    def _hook(ruby_sys, options, network_cntrls, all_cntrls):
        base = options.num_cpus * 2 + options.num_l3caches + 100
        for ni in range(num_nodes):
            # EP-RNF
            ep_rnf = EPRNFController(
                node_id=ni, version=base, ruby_system=ruby_sys,
                data_channel_size=data_channel_size,
                reqOut=MessageBuffer(), snpOut=MessageBuffer(),
                rspOut=MessageBuffer(), datOut=MessageBuffer(),
                reqIn=MessageBuffer(), snpIn=MessageBuffer(),
                rspIn=MessageBuffer(), datIn=MessageBuffer())
            base += 1
            for attr in ['reqOut','rspOut','snpOut','datOut']:
                getattr(ep_rnf, attr).out_port = ruby_sys.network.in_port
            for attr in ['reqIn','rspIn','snpIn','datIn']:
                getattr(ep_rnf, attr).in_port = ruby_sys.network.out_port
            network_cntrls.append(ep_rnf); all_cntrls.append(ep_rnf)
            if not hasattr(ruby_sys, '_ep_rnfs'): ruby_sys._ep_rnfs = []
            ruby_sys._ep_rnfs.append(ep_rnf)

            # EP-SNF
            ep_snf = EPSNFController(
                node_id=ni, version=base, ruby_system=ruby_sys,
                data_channel_size=data_channel_size,
                reqOut=MessageBuffer(), snpOut=MessageBuffer(),
                rspOut=MessageBuffer(), datOut=MessageBuffer(),
                reqIn=MessageBuffer(), snpIn=MessageBuffer(),
                rspIn=MessageBuffer(), datIn=MessageBuffer())
            base += 1
            for attr in ['reqOut','rspOut','snpOut','datOut']:
                getattr(ep_snf, attr).out_port = ruby_sys.network.in_port
            for attr in ['reqIn','rspIn','snpIn','datIn']:
                getattr(ep_snf, attr).in_port = ruby_sys.network.out_port
            network_cntrls.append(ep_snf); all_cntrls.append(ep_snf)
            if not hasattr(ruby_sys, '_ep_snfs'): ruby_sys._ep_snfs = []
            ruby_sys._ep_snfs.append(ep_snf)
    return _hook


def _tag(node_id, controllers):
    """Tag all controllers with _node_id for strict filtering."""
    for c in controllers:
        c._node_id = node_id


def _check_node_id(obj, expected, label):
    """Cross-node assertion helper."""
    got = getattr(obj, '_node_id', -1)
    if got != expected:
        fatal(f"{label}: expected node_id={expected}, got node_id={got}")


def validate_downstream_isolation(ruby_system):
    """Post-creation checker: verify all downstreams are node-local."""
    errors = []

    for rnf in ruby_system.rnf:
        rnf_nid = rnf._node_id
        for c in rnf._ll_cntrls:
            dests = getattr(c, 'downstream_destinations', [])
            for d in dests:
                d_nid = getattr(d, '_node_id', -1)
                if d_nid != rnf_nid and d_nid != -1:
                    errors.append(
                        f"RN-F node{rnf_nid} ll_ctrl downstream -> "
                        f"node{d_nid} (CROSS-NODE)")

    for hnf in ruby_system.hnf:
        hnf_nid = hnf._node_id
        for c in hnf.getNetworkSideControllers():
            dests = getattr(c, 'downstream_destinations', [])
            for d in dests:
                d_nid = getattr(d, '_node_id', -1)
                if d_nid != hnf_nid and d_nid != -1:
                    errors.append(
                        f"HN-F node{hnf_nid} downstream -> "
                        f"node{d_nid} (CROSS-NODE)")

    if errors:
        fatal("Cross-node domain isolation VIOLATION:\n" +
              "\n".join(errors))
    return True


class MultiNodeCHI_RNF(CHI_Node):
    """Per-node Request Node with cluster-shared L2 and strict filtering."""

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

        _tag(node_id, self._cntrls)

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
        """STRICT: only same-node HN-F destinations, exactly one expected."""
        filtered = [c for c in cntrls
                    if getattr(c, '_node_id', -1) == self._node_id]
        if len(filtered) != 1:
            fatal(f"RN-F node{self._node_id}: expected 1 same-node HN-F, "
                  f"got {len(filtered)} from {len(cntrls)} candidates")
        for c in self._ll_cntrls:
            c.downstream_destinations = filtered

    @classmethod
    def generate(cls, options, ruby_system, cpus):
        cores_per = getattr(options, 'cores_per_node', CORES_PER_NODE)
        num_nodes = len(cpus) // cores_per
        if len(cpus) % cores_per != 0:
            m5.fatal(f"CPU count ({len(cpus)}) not multiple of "
                     f"cores_per_node ({cores_per})")

        # M3: EP post-hook (enable with --enable-ep-controllers flag)
        if getattr(options, 'enable_ep_controllers', False):
            parent_sys = getattr(ruby_system, '_parent', None)
            if parent_sys and not getattr(parent_sys, '_chi_post_hook', None):
                parent_sys._chi_post_hook = _make_ep_post_hook(num_nodes)
                print(f"M3: EP post-hook registered for {num_nodes} nodes")

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
    """Per-node Home Node with contiguous partition and strict filtering."""

    _addr_ranges = {}

    @classmethod
    def createAddrRanges(cls, sys_mem_ranges, cache_line_size, hnfs):
        """Each HN-F handles the FULL memory range (not partitioned).
        Domain isolation is enforced by strict downstream filtering,
        not by address range partitioning. Each RN-F only sends to
        its own node's HN-F, which handles all addresses for that node.
        """
        num_nodes = len(hnfs)
        block_bits = int(math.log(cache_line_size, 2))
        cls._addr_ranges = {}
        for ni in hnfs:
            # Each HN-F gets the entire memory range
            cls._addr_ranges[ni] = (
                [AddrRange(r.start, size=r.size()) for r in sys_mem_ranges],
                block_bits - 1)

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
        """HN-F downstream: include all SN-Fs.
        Memory interleaving requires HN-F to reach any SN-F.
        Strict domain isolation is enforced at RN-F -> HN-F level.
        When EP-SNF is introduced (M3+), this will be restricted.
        """
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


def create_ep_controllers(ruby_system, num_nodes, data_channel_size=32):
    """Create per-node EP-RNF and EP-SNF controllers and wire them
    to the Ruby network. Returns a post-hook function for CHI.py.

    Usage: system._chi_post_hook = create_ep_controllers(system.ruby, num_nodes)
    """
    def _post_hook(ruby_sys, options, network_cntrls, all_cntrls):
        # Get base version
        base_version = options.num_cpus * 2 + options.num_l3caches + 100

        for ni in range(num_nodes):
            # EP-RNF
            ep_rnf = EPRNFController(
                node_id=ni, version=base_version,
                ruby_system=ruby_sys, data_channel_size=data_channel_size)
            base_version += 1
            ep_rnf.reqOut.out_port = ruby_sys.network.in_port
            ep_rnf.rspOut.out_port = ruby_sys.network.in_port
            ep_rnf.snpOut.out_port = ruby_sys.network.in_port
            ep_rnf.datOut.out_port = ruby_sys.network.in_port
            ep_rnf.reqIn.in_port = ruby_sys.network.out_port
            ep_rnf.rspIn.in_port = ruby_sys.network.out_port
            ep_rnf.snpIn.in_port = ruby_sys.network.out_port
            ep_rnf.datIn.in_port = ruby_sys.network.out_port
            network_cntrls.append(ep_rnf)
            all_cntrls.append(ep_rnf)
            if not hasattr(ruby_sys, '_ep_rnfs'):
                ruby_sys._ep_rnfs = []
            ruby_sys._ep_rnfs.append(ep_rnf)

            # EP-SNF
            ep_snf = EPSNFController(
                node_id=ni, version=base_version,
                ruby_system=ruby_sys, data_channel_size=data_channel_size)
            base_version += 1
            ep_snf.reqOut.out_port = ruby_sys.network.in_port
            ep_snf.rspOut.out_port = ruby_sys.network.in_port
            ep_snf.snpOut.out_port = ruby_sys.network.in_port
            ep_snf.datOut.out_port = ruby_sys.network.in_port
            ep_snf.reqIn.in_port = ruby_sys.network.out_port
            ep_snf.rspIn.in_port = ruby_sys.network.out_port
            ep_snf.snpIn.in_port = ruby_sys.network.out_port
            ep_snf.datIn.in_port = ruby_sys.network.out_port
            network_cntrls.append(ep_snf)
            all_cntrls.append(ep_snf)
            if not hasattr(ruby_sys, '_ep_snfs'):
                ruby_sys._ep_snfs = []
            ruby_sys._ep_snfs.append(ep_snf)

        print(f"M3: Created {num_nodes} EP-RNF + {num_nodes} EP-SNF "
              f"controllers in topology")

    return _post_hook
