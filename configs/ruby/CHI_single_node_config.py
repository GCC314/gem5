"""
CHI single-node clustered configuration module.

Defines ClusterCHI_RNF which groups CPUs into clusters, each cluster
sharing a single L2 cache (RN-F last-level controller).
Used with --chi-config to override the default CHI_RNF in CHI.py.

Architecture:
  CPU0,1 (Cluster0) -> L1I/D -> Shared L2 (RN-F) -> HN-F/L3 -> SN-F/DRAM
  CPU2,3 (Cluster1) -> L1I/D -> Shared L2 (RN-F) -> HN-F/L3 -> SN-F/DRAM
"""

import math

import m5
from m5.objects import *

from ruby.CHI_config import (
    L1ICache,
    L1DCache,
    L2Cache,
    Versions,
    NoC_Params,
    CHI_Node,
    CHI_L1Controller,
    CHI_L2Controller,
    CPUSequencerWrapper,
    CHI_HNF,
    CHI_MN,
    CHI_SNF_MainMem,
    CHI_SNF_BootMem,
    CHI_RNI_DMA,
    CHI_RNI_IO,
)


class ClusterCHI_RNF(CHI_Node):

    def __init__(
        self,
        cpus,
        ruby_system,
        l1Icache_type,
        l1Dcache_type,
        cache_line_size,
        cluster_id,
        l1Iprefetcher_type=None,
        l1Dprefetcher_type=None,
    ):
        super().__init__(ruby_system)

        self._block_size_bits = int(math.log(cache_line_size, 2))
        self._seqs = []
        self._cntrls = []
        self._ll_cntrls = []
        self._cpus = cpus
        self._cluster_id = cluster_id

        for cpu in self._cpus:
            cpu.inst_sequencer = RubySequencer(
                version=Versions.getSeqId(), ruby_system=ruby_system
            )
            cpu.data_sequencer = RubySequencer(
                version=Versions.getSeqId(), ruby_system=ruby_system
            )
            self._seqs.append(
                CPUSequencerWrapper(cpu.inst_sequencer, cpu.data_sequencer)
            )

            l1i_cache = l1Icache_type(
                start_index_bit=self._block_size_bits, is_icache=True
            )
            l1d_cache = l1Dcache_type(
                start_index_bit=self._block_size_bits, is_icache=False
            )

            l1i_pf = l1Iprefetcher_type() if l1Iprefetcher_type else NULL
            l1d_pf = l1Dprefetcher_type() if l1Dprefetcher_type else NULL

            cpu.l1i = CHI_L1Controller(
                ruby_system, cpu.inst_sequencer, l1i_cache, l1i_pf
            )
            cpu.l1d = CHI_L1Controller(
                ruby_system, cpu.data_sequencer, l1d_cache, l1d_pf
            )

            cpu.inst_sequencer.dcache = NULL
            cpu.data_sequencer.dcache = cpu.l1d.cache
            cpu.l1d.sc_lock_enabled = True

            cpu._ll_cntrls = [cpu.l1i, cpu.l1d]
            for c in cpu._ll_cntrls:
                self._cntrls.append(c)
                self.connectController(c)

    def addSharedL2Cache(self, cache_type, pf_type=None):
        self._ll_cntrls = []
        l2_cache = cache_type(
            start_index_bit=self._block_size_bits, is_icache=False
        )
        l2_pf = pf_type() if pf_type else NULL

        self.shared_l2 = CHI_L2Controller(
            self._ruby_system, l2_cache, l2_pf
        )
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
        for c in self._ll_cntrls:
            c.downstream_destinations = cntrls

    def getCpus(self):
        return self._cpus

    @classmethod
    def generate(cls, options, ruby_system, cpus):
        cores_per_cluster = getattr(options, 'cores_per_cluster', 2)
        num_clusters = len(cpus) // cores_per_cluster
        if len(cpus) % cores_per_cluster != 0:
            m5.fatal(
                f"Number of CPUs ({len(cpus)}) must be a multiple of "
                f"cores_per_cluster ({cores_per_cluster})"
            )

        rnfs = []
        for ci in range(num_clusters):
            cluster_cpus = cpus[
                ci * cores_per_cluster: (ci + 1) * cores_per_cluster
            ]
            rnf = cls(
                cluster_cpus,
                ruby_system,
                L1ICache(size=options.l1i_size, assoc=options.l1i_assoc),
                L1DCache(size=options.l1d_size, assoc=options.l1d_assoc),
                options.cacheline_size,
                cluster_id=ci,
            )
            rnf.addSharedL2Cache(
                L2Cache(size=options.l2_size, assoc=options.l2_assoc)
            )
            rnfs.append(rnf)
        return rnfs


CHI_RNF = ClusterCHI_RNF
