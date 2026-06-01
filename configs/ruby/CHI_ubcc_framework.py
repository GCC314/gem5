"""UBCC Basic Framework - Ruby CHI multi-node topology builder.
Creates N=3, L=2, D=2 topology with EP endpoints.
HN_i routes by address classification to L_SNF_i / DL_SNF_i / EP_SNF_i.
RN-F downstream: same-node HN only (TC-TOPO-2).

Q1: DSM VA Mapping helper — call setup_dsm_va_mapping() from test scripts
    after Process creation to map DSM_VA_BASE + k*SEG to PA for each node.
"""
import math

import m5
from m5.objects import *

from . import CHI_config as chi_defs
from .CHI_basic_framework_config import (
    NodeConfig, NodeAddressMap,
    DEFAULT_N, DEFAULT_L, DEFAULT_D, DEFAULT_SEG_SIZE,
    ClusterCHI_RNF, EPNodeWrapper, HNNodeWrapper,
)


def _make_hnf(ruby_system, addr_ranges, llcache_type, node_id):
    hnf_cache = llcache_type()
    hnf_cntrl = chi_defs.CHI_HNFController(
        ruby_system, hnf_cache, NULL, addr_ranges)
    wrapper = HNNodeWrapper(ruby_system)
    wrapper.setController(hnf_cntrl)
    wrapper.connectController(hnf_cntrl)
    wrapper._node_id = node_id
    return wrapper, hnf_cntrl


# ---- Q1: DSM VA Mapping Helper ----
# Call this from test scripts AFTER creating Process objects but BEFORE
# m5.instantiate().  Maps DSM_VA_BASE + k*SEG_SIZE → node-k's DSM PA base.
# This ensures ARM workload ldr/str to DSM VA addresses correctly traverse
# the page table to the CHI Ruby physical address space.

def setup_dsm_va_mapping(processes, num_nodes=DEFAULT_N, seg_size=DEFAULT_SEG_SIZE):
    """Install VA→PA mappings for DSM regions on all processes.

    Each process gets VA range [DSM_VA_BASE + k*SEG, DSM_VA_BASE + (k+1)*SEG)
    mapped to the REQUESTING NODE's DSM PA base for home node k.

    For process on node i accessing DSM data homed at node k,
    the PA must be in node i's DSM_k range:
        PA = (i << 40) + (2 + k) * seg_size

    This ensures the local RNF recognizes the address as DSM and
    forwards it correctly through the CHI EP layer.

    Args:
        processes: List of Process objects (ordered by CPU index, one per CPU).
        num_nodes: Number of nodes (default 3).
        seg_size: Segment size in bytes (default 128MB).

    Usage in test script:
        from ruby.CHI_ubcc_framework import setup_dsm_va_mapping
        setup_dsm_va_mapping([proc for cpu in cpus for proc in cpu.workload])
    """
    addr_map = NodeAddressMap(num_nodes, seg_size)

    # DSM_VA_BASE = (MaxAddr + 1) - 4 * SEG_SIZE
    # Must be page-aligned for EmulationPageTable::map() assertion.
    # (0xFFFFFFFFFFFF + 1) = 0x1000000000000 for 48-bit VA max.
    # Each node k's DSM_k window is at DSM_VA_BASE + k * SEG_SIZE
    dsm_va_base = (0xFFFFFFFFFFFF + 1) - 4 * seg_size

    # Compute CPUs per node for node_id assignment.
    # Processes are ordered by CPU index: proc[i] belongs to CPU i.
    # CPU i belongs to node i // cpus_per_node.
    _cpus_per_node = len(processes) // num_nodes if num_nodes > 0 else 1

    for _proc_idx, proc in enumerate(processes):
        if proc is None:
            continue
        # Determine which node this process/CPU is on
        _req_node_id = _proc_idx // _cpus_per_node
        _req_node_base = _req_node_id << addr_map.node_shift

        for nid in range(num_nodes):
            # PA must be in the REQUESTOR node's DSM_k range:
            #   req_node_base + (2 + nid) * seg_size
            dsm_pa_base = _req_node_base + (2 + nid) * seg_size
            dsm_va = dsm_va_base + nid * seg_size
            proc.map(dsm_va, dsm_pa_base, seg_size, cacheable=True)

    print(f"[Q1-DSM-MAP] Installed DSM VA→PA mappings for {num_nodes} nodes, "
           f"base VA=0x{dsm_va_base:x}, {len(processes)} processes "
           f"({_cpus_per_node} CPUs/node)")


# ---- Q1: L3 Allocation / Deallocation Policy ----
# These configure the HN-F (L3) cache controller's allocation behavior
# to ensure DSM lines are cached correctly and invalidated on UBCC recall.

def configure_l3_dsm_policy(hnf_cntrl):
    """Configure HN-F L3 alloc/dealloc for DSM line handling.

    Q2 FIX: Disable L3 caching for DSM lines.  With L3 caching of DSM
    lines, the HN-F serves subsequent reads/writes from L3 without
    forwarding to EP_SNF, which bypasses the UBCC recall path (M6).
    Stale L3 data then causes cross-node coherence failures (TC3 fails).
    """
    hnf_cntrl.alloc_on_readshared     = False  # No L3 caching for DSM
    hnf_cntrl.alloc_on_readunique     = False  # No L3 caching for DSM
    hnf_cntrl.alloc_on_readonce       = False
    hnf_cntrl.alloc_on_writeback      = False
    hnf_cntrl.alloc_on_atomic         = False
    hnf_cntrl.dealloc_on_unique       = False
    hnf_cntrl.dealloc_on_shared       = False


def _make_snf(ruby_system, addr_ranges):
    snf = chi_defs.CHI_SNF_MainMem(ruby_system, None, None)
    snf._cntrl.addr_ranges = addr_ranges
    return snf


def _make_dram_memctrl(addr_range, system=None, name=None):
    """Create a MemCtrl/DDR4_2400_8x8 and optionally parent it under System.

    gem5 v25.1 requires AbstractMemory (and its parent MemCtrl) to be
    reachable from System for MemStats::regStats().  Without proper
    parentage, m5.instantiate() will trigger a fatal assertion.

    Args:
        addr_range: Address range for the DRAM controller.
        system: If provided, parent the MemCtrl under this System SimObject.
        name: Child name under system (required when system is provided).
    """
    dram = DDR4_2400_8x8(range=addr_range)
    mc = MemCtrl(dram=dram)
    if system and name:
        system.add_child(name, mc)
    return mc


def _make_ep_node(ruby_system, ep_cntrl, node_id):
    wrapper = EPNodeWrapper(ruby_system)
    wrapper.setController(ep_cntrl)
    wrapper.connectController(ep_cntrl)
    wrapper._node_id = node_id
    return wrapper


def create_ubcc_system(options, full_system, system, dma_ports, bootmem,
                        ruby_system, cpus):
    if buildEnv["PROTOCOL"] != "CHI":
        m5.panic("UBCC framework requires CHI protocol build")

    # Q2 FIX: RubySystem needs a clock domain to schedule network events.
    # In older gem5 versions this was inherited; v25.1 requires explicit set.
    ruby_system.clk_domain = system.clk_domain

    num_nodes = DEFAULT_N
    seg_size = DEFAULT_SEG_SIZE
    cache_line = system.cache_line_size.value
    addr_map = NodeAddressMap(num_nodes, seg_size)
    params = chi_defs.NoC_Params

    class HNFCache(RubyCache):
        dataAccessLatency = 10
        tagAccessLatency = 2
        size = getattr(options, "l3_size", "256kB")
        assoc = getattr(options, "l3_assoc", 16)

    cpu_sequencers = []
    network_nodes = []
    all_cntrls = []
    mem_backstores = []

    per_node = {nid: {} for nid in range(num_nodes)}

    total_cpus = num_nodes * DEFAULT_D * DEFAULT_L
    assert len(cpus) == total_cpus, \
        f"Need {total_cpus} CPUs, got {len(cpus)}"

    for node_id in range(num_nodes):
        nd = per_node[node_id]
        cfg = NodeConfig(node_id, num_nodes, seg_size)

        # ── Create SNFs FIRST (before HN-F) ─────────────────────────
        # Q2 FIX: SNF controllers must be added to the SimObject tree
        # BEFORE the HN-F so their C++ objects exist when HN-F's
        # downstream_destinations param is resolved during instantiation.
        l_backstore_range = AddrRange(cfg.local_private_base, size=2 * seg_size)
        nd['l_memctrl'] = _make_dram_memctrl(l_backstore_range, system,
                                             f"l_mc_n{node_id}")
        nd['l_snf'] = chi_defs.CHI_SNF_MainMem(
            ruby_system, None, nd['l_memctrl'],
            # Q2 FIX: Pass explicit addr_ranges instead of relying on
            # getMemRange which may return a single value not a list.
            addr_ranges=[cfg.local_private_range, cfg.ubcc_exclusive_range])
        setattr(ruby_system, f"l_snf_node{node_id}", nd['l_snf'])
        network_nodes.append(nd['l_snf'])
        all_cntrls.extend(nd['l_snf'].getAllControllers())
        mem_backstores.append(nd['l_memctrl'])

        dl_range = NodeConfig.dsm_range_for(node_id, seg_size, cfg.phy_base)
        nd['dl_memctrl'] = _make_dram_memctrl(dl_range, system,
                                              f"dl_mc_n{node_id}")
        nd['dl_snf'] = chi_defs.CHI_SNF_MainMem(ruby_system, None,
                                                nd['dl_memctrl'],
                                                addr_ranges=[dl_range])
        setattr(ruby_system, f"dl_snf_node{node_id}", nd['dl_snf'])
        network_nodes.append(nd['dl_snf'])
        all_cntrls.extend(nd['dl_snf'].getAllControllers())
        mem_backstores.append(nd['dl_memctrl'])

        ep_backend = EPBackend(node_id=node_id, ruby_system=ruby_system)

        nd['ep_snf_cntrl'] = EPSNFController(
            version=chi_defs.Versions.getVersion(chi_defs.CHI_Cache_Controller),
            ruby_system=ruby_system, node_id=node_id,
            data_channel_size=params.data_width,
            ep_backend=ep_backend,
            addr_ranges=[NodeConfig.dsm_range_for(nid, seg_size, cfg.phy_base)
                         for nid in range(num_nodes)])
        nd['ep_snf_wrapper'] = _make_ep_node(
            ruby_system, nd['ep_snf_cntrl'], node_id)
        setattr(ruby_system, f"ep_snf_node{node_id}", nd['ep_snf_wrapper'])
        network_nodes.append(nd['ep_snf_wrapper'])
        all_cntrls.append(nd['ep_snf_cntrl'])

        nd['ep_rnf_cntrl'] = EPRNFController(
            version=chi_defs.Versions.getVersion(chi_defs.CHI_Cache_Controller),
            ruby_system=ruby_system, node_id=node_id,
            data_channel_size=params.data_width,
            ep_backend=ep_backend,
            addr_ranges=[NodeConfig.dsm_range_for(
                node_id, seg_size, cfg.phy_base)])
        nd['ep_rnf_wrapper'] = _make_ep_node(
            ruby_system, nd['ep_rnf_cntrl'], node_id)
        setattr(ruby_system, f"ep_rnf_node{node_id}", nd['ep_rnf_wrapper'])
        network_nodes.append(nd['ep_rnf_wrapper'])
        all_cntrls.append(nd['ep_rnf_cntrl'])

        # ── Create HN-F AFTER SNFs ─────────────────────────────────
        hnf_ranges = [
            cfg.local_private_range,
            cfg.ubcc_exclusive_range,
        ]
        for nid in range(num_nodes):
            hnf_ranges.append(
                NodeConfig.dsm_range_for(nid, seg_size, cfg.phy_base))
        nd['hnf_wrapper'], nd['hnf_cntrl'] = _make_hnf(
            ruby_system, hnf_ranges, HNFCache, node_id)
        configure_l3_dsm_policy(nd['hnf_cntrl'])
        setattr(ruby_system, f"hnf_node{node_id}", nd['hnf_wrapper'])
        network_nodes.append(nd['hnf_wrapper'])
        all_cntrls.append(nd['hnf_cntrl'])

        nd['clusters'] = []
        for cluster_i in range(DEFAULT_D):
            node_cpu_base = node_id * DEFAULT_D * DEFAULT_L
            cluster_base = node_cpu_base + cluster_i * DEFAULT_L
            cluster_cpus = cpus[cluster_base:cluster_base + DEFAULT_L]

            cluster = ClusterCHI_RNF(
                cluster_cpus, ruby_system, cache_line,
                l1i_assoc=2, l1d_assoc=2, l1i_size="32kB", l1d_size="32kB",
                l2_assoc=8, l2_size="256kB")
            cluster.addPrivL2Cache()
            setattr(ruby_system,
                    f"cluster_n{node_id}_c{cluster_i}", cluster)
            nd['clusters'].append(cluster)
            network_nodes.append(cluster)
            all_cntrls.extend(cluster.getAllControllers())
            cpu_sequencers.extend(cluster.getSequencers())

        # Q2 Fix B: Set L1/L2 addr_ranges to include DSM PA ranges so
        # functionalRead() in populateGrantData() can find cached data
        # in L1/L2 caches.  Without explicit DSM ranges, L1/L2
        # controllers' respondTo() returns false for DSM addresses,
        # causing RubySystem::functionalRead() to skip them.
        dsm_ranges = [NodeConfig.dsm_range_for(nid, seg_size, cfg.phy_base)
                      for nid in range(num_nodes)]
        for cluster in nd['clusters']:
            for cntrl in cluster.getAllControllers():
                cntrl.addr_ranges = [
                    cfg.local_private_range,
                    cfg.ubcc_exclusive_range,
                ] + dsm_ranges

    for node_id in range(num_nodes):
        nd = per_node[node_id]
        hnf_c_list = [nd['hnf_cntrl']]
        for cluster in nd['clusters']:
            cluster.setDownstream(hnf_c_list)

    for node_id in range(num_nodes):
        nd = per_node[node_id]
        snf_dests = []
        snf_dests.extend(nd['l_snf'].getAllControllers())
        # Q2 FIX: Route ALL DSM through EP_SNF first so UBCC directory
        # is consulted before local DDR4.  EP_SNF handles recalls when
        # another node owns the line modified/exclusive.
        snf_dests.append(nd['ep_snf_cntrl'])
        snf_dests.extend(nd['dl_snf'].getAllControllers())
        nd['hnf_wrapper'].setDownstream(snf_dests)
        # Q2 FIX: Force re-evaluation of downstream_destinations param
        # after the Python list was updated, so the C++ params struct
        # picks up the correct values during m5.instantiate().
        nd['hnf_cntrl'].unproxyParams()

    for cntrl in all_cntrls:
        cntrl.data_channel_size = params.data_width

    ruby_system.network.number_of_virtual_networks = 4
    ruby_system.network.control_msg_size = params.cntrl_msg_size
    ruby_system.network.data_msg_size = params.data_width

    for k in dir(params):
        if not k.startswith("__"):
            setattr(options, k, getattr(params, k))

    from .Ruby import create_topology

    if options.topology == "CustomMesh":
        topology = create_topology(network_nodes, options)
    elif options.topology in ["Crossbar", "Pt2Pt"]:
        network_cntrls = []
        for node in network_nodes:
            network_cntrls.extend(node.getNetworkSideControllers())
        topology = create_topology(network_cntrls, options)
    else:
        m5.fatal(f"{options.topology} not supported!")

    # ---- Q1: DSM VA mapping for processes ----
    # Collect all Process objects across all CPUs and map DSM VA regions
    # to the corresponding home node's DSM PA base for each node.
    processes = [proc for cpu in cpus for proc in cpu.workload]
    setup_dsm_va_mapping(processes, num_nodes, seg_size)

    return (cpu_sequencers, [], topology)
