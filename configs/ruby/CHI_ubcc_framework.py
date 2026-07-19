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
    print(f"[Q3 DEBUG] HN-F node{node_id}: machineID=Cache.{hnf_cntrl.version}")
    # DMT disabled — our design requires CompData to return to HN-F
    # so HN-F can use it to satisfy the local L2's ReadShared.
    # This is the non-DMT CHI path, which in standard gem5 has
    # inherent DRAM latency separating the HN-F's state transitions.
    # With our zero-latency EP-SNF, we must add a minimum delay to
    # prevent same-tick TBE reservation races.
    hnf_cntrl.enable_DMT = False
    hnf_cntrl.epRnfMachineVersion = -1  # v4: default, overridden if EP-RNF exists
    hnf_cntrl.number_of_TBEs = 4096
    hnf_cntrl.number_of_repl_TBEs = 4096
    hnf_cntrl.number_of_snoop_TBEs = 4096
    hnf_cntrl.number_of_DVM_TBEs = 4096
    hnf_cntrl.number_of_DVM_snoop_TBEs = 4096
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

def setup_dsm_va_mapping(processes, num_nodes=DEFAULT_N, seg_size=DEFAULT_SEG_SIZE,
                         num_sockets=1, req_node_ids=None):
    """Install VA→PA mappings for DSM regions on all processes.

    Each process gets VA range [DSM_VA_BASE + k*SEG, DSM_VA_BASE + (k+1)*SEG)
    mapped to the REQUESTING NODE's DSM PA base for home node k.

    v4-dual-socket: DSM segments are indexed by (homeNode, homeSocket).
    With num_sockets=1, homeSocket=0 and layout degenerates to original.

    Args:
        processes: List of Process objects (ordered by CPU index, one per CPU).
        num_nodes: Number of nodes (default 3).
        seg_size: Segment size in bytes (default 128MB).
        num_sockets: Number of sockets per node (default 1).
    """
    addr_map = NodeAddressMap(num_nodes, seg_size, num_sockets)

    # DSM_VA_BASE = (MaxAddr + 1) - (num_nodes * num_sockets + 1) * SEG_SIZE
    # Must be page-aligned for EmulationPageTable::map() assertion.
    total_dsm_segs = num_nodes * num_sockets
    dsm_va_base = (0xFFFFFFFFFFFF + 1) - (total_dsm_segs + 1) * seg_size

    # Compute CPUs per node for node_id assignment (legacy all-node mode).
    _cpus_per_node = len(processes) // num_nodes if num_nodes > 0 else 1

    for _proc_idx, proc in enumerate(processes):
        if proc is None:
            continue
        # Split mode passes explicit req_node_ids (the local node for each
        # process); legacy mode derives it from global CPU index.
        if req_node_ids is not None:
            _req_node_id = req_node_ids[_proc_idx]
        else:
            _req_node_id = _proc_idx // _cpus_per_node
        _req_node_base = _req_node_id << addr_map.node_shift

        seg_idx = 0
        for nid in range(num_nodes):
            for sid in range(num_sockets):
                dsm_pa_base = _req_node_base + (2 + seg_idx) * seg_size
                dsm_va = dsm_va_base + seg_idx * seg_size
                proc.map(dsm_va, dsm_pa_base, seg_size, cacheable=True)
                seg_idx += 1

    print(f"[Q1-DSM-MAP] Installed DSM VA→PA mappings for {num_nodes} nodes, "
           f"{num_sockets} sockets/node ({total_dsm_segs} DSM segments), "
           f"base VA=0x{dsm_va_base:x}, {len(processes)} processes "
           f"({_cpus_per_node} CPUs/node)")


# ---- Q1: L3 Allocation / Deallocation Policy ----
# These configure the HN-F (L3) cache controller's allocation behavior
# to ensure DSM lines are cached correctly and invalidated on UBCC recall.

def configure_l3_dsm_policy(hnf_cntrl):
    """Configure HN-F L3 alloc/dealloc for DSM line handling.

    v4: Enable L3 caching for shared and unique DSM lines so that
    EP-RNF registration via shared_hint can record dir_sharers properly,
    and local upgrades (SC→UC) can trigger SnpCleanInvalid to EP-RNF.
    ReadOnce remains disabled to prevent bypass of the UBCC recall path.
    """
    # v4: L3 DSM caching — alloc_on_readunique=True required per scheme_v4 §6.1
    hnf_cntrl.alloc_on_readshared     = True
    hnf_cntrl.alloc_on_readunique     = True
    hnf_cntrl.alloc_on_readonce       = False  # v4: disable to prevent UBCC recall bypass
    hnf_cntrl.alloc_on_writeback      = True
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

    # UBCC config is plumbed through the `options` object (set by the test
    # harness before create_system runs), not environment variables. `_opt`
    # reads an attribute from options with a fallback default.
    def _opt(name, default):
        return getattr(options, name, default)

    num_nodes = int(_opt("ubcc_num_nodes", DEFAULT_N))
    # Multi-process split: ubcc_local_node selects the single node this gem5
    # process owns. -1 (default) builds ALL nodes in one process (legacy
    # single-process mode, used for regression parity).
    local_node = int(_opt("ubcc_local_node", -1))
    seg_size = DEFAULT_SEG_SIZE
    num_sockets = int(_opt("ubcc_num_sockets", 1))
    ubcc_epoch_bits = int(_opt("ubcc_epoch_bits", 64))
    ubcc_bf_bytes = int(_opt("ubcc_bf_bytes", 65536))
    ubcc_force_resident_entries = int(_opt("ubcc_force_resident_entries", 0))
    ubcc_meta_max_flights = int(_opt("ubcc_meta_max_flights", 8))
    ubcc_meta_read_ticks = int(_opt("ubcc_meta_read_ticks", 8000))
    ubcc_meta_write_ticks = int(_opt("ubcc_meta_write_ticks", 7500))
    ubcc_meta_delete_ticks = int(_opt("ubcc_meta_delete_ticks", 7500))
    cache_line = system.cache_line_size.value
    # node_list: which nodes this process builds. In split mode, exactly one.
    if local_node < 0:
        node_list = list(range(num_nodes))
    else:
        assert 0 <= local_node < num_nodes, \
            f"UBCC_LOCAL_NODE={local_node} out of range [0,{num_nodes})"
        node_list = [local_node]
    cpus_per_node = DEFAULT_D * DEFAULT_L
    print(f"[UBCC-CONFIG] epoch_bits={ubcc_epoch_bits} num_sockets={num_sockets} "
          f"num_nodes={num_nodes} local_node={local_node} "
          f"build_nodes={node_list}")
    addr_map = NodeAddressMap(num_nodes, seg_size, num_sockets)
    params = chi_defs.NoC_Params

    class HNFCache(RubyCache):
        dataAccessLatency = 10
        tagAccessLatency = 4
        size = getattr(options, "l3_size", "256kB")
        assoc = getattr(options, "l3_assoc", 16)

    cpu_sequencers = []
    network_nodes = []
    all_cntrls = []
    mem_backstores = []

    per_node = {nid: {} for nid in node_list}

    expected_cpus = len(node_list) * cpus_per_node
    assert len(cpus) == expected_cpus, \
        f"Need {expected_cpus} CPUs ({len(node_list)} nodes x {cpus_per_node}), got {len(cpus)}"

    # Map a global node_id to its slice within the passed-in cpus list.
    # In split mode cpus holds only the local node's CPUs (slice index 0).
    def _node_cpu_slice(nid):
        local_idx = node_list.index(nid)
        base = local_idx * cpus_per_node
        return cpus[base:base + cpus_per_node]

    for node_id in node_list:
        nd = per_node[node_id]
        cfg = NodeConfig(node_id, num_nodes, seg_size, num_sockets)

        # ── Create SNFs FIRST (before HN-F) ─────────────────────────
        # Q2 FIX: SNF controllers must be added to the SimObject tree
        # BEFORE the HN-F so their C++ objects exist when HN-F's
        # downstream_destinations param is resolved during instantiation.
        metadata_private_size = 16 * 1024 * 1024
        cfg.metadata_private_size = metadata_private_size
        cfg.metadata_private_end = cfg.metadata_private_base + metadata_private_size

        l_backstore_range = AddrRange(
            cfg.local_private_base,
            size=((2 + num_nodes * num_sockets) * seg_size + metadata_private_size),
        )
        nd['l_memctrl'] = _make_dram_memctrl(l_backstore_range, system,
                                             f"l_mc_n{node_id}")
        nd['l_snf'] = chi_defs.CHI_SNF_MainMem(
            ruby_system, None, nd['l_memctrl'],
            # Q2 FIX: Pass explicit addr_ranges instead of relying on
            # getMemRange which may return a single value not a list.
            # v4-dual-socket: per-socket private + routing + backstore
            addr_ranges=(
                cfg.all_local_private_ranges()
                + cfg.all_metadata_private_ranges()
                + cfg.all_metadata_backstore_ranges()
            ))
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

        # v4: Create per-socket UBAdapters
        # For num_sockets=1, socket_id=0 and behavior is identical to before.
        nd['ub_adapters'] = []
        nd['meta_rnfs'] = []
        for socket_id in range(num_sockets):
            ub_adapter = UBAdapter(node_id=node_id, socket_id=socket_id,
                                   num_nodes=num_nodes, num_sockets=num_sockets,
                                   local_node=local_node)
            # Parent each adapter explicitly in the SimObject tree so its init()
            # runs (binds ubio(node, socket) Port). A VectorParam reference alone
            # leaves it an orphan; explicit setattr is the unambiguous parent.
            setattr(ruby_system, f"ub_adapter_node{node_id}_s{socket_id}", ub_adapter)
            nd['ub_adapters'].append(ub_adapter)

        # Backward-compat aliases for first socket
        
        nd['ub_adapter'] = nd['ub_adapters'][0] if nd['ub_adapters'] else None
        nd['meta_rnf'] = None

        import os
        _silent_env = os.environ.get("EP_SILENT_UPGRADE")
        _direct_env = os.environ.get("EP_DIRECT_FWD")
        ep_backend = EPBackend(node_id=node_id, ruby_system=ruby_system,
                                 meta_rnf=NULL,
                                 ub_adapter=nd['ub_adapter'],
                                # Socket-plane: pass ALL per-socket UBAdapters so
                                # each becomes a tree child (init() runs, binds its
                                # own ubio Port) and is registered for getUBAdapter.
                                ub_adapters=nd['ub_adapters'],
                                num_sockets=num_sockets,
                                num_nodes=num_nodes,
                                ubcc_epoch_bits=ubcc_epoch_bits,
                                ubcc_bf_bytes=ubcc_bf_bytes,
                                 ubcc_force_resident_entries=
                                      ubcc_force_resident_entries,
                                 metadata_private_base=cfg.metadata_private_base,
                                 metadata_private_size=f"{metadata_private_size}B",
                                  silent_upgrade=bool(int(_silent_env))
                                       if _silent_env is not None else False,
                                  direct_fwd=bool(int(_direct_env))
                                       if _direct_env is not None else False)

        # v4-dual-socket: Create per-socket EP-SNF controllers (§3.2 change 2)
        nd['ep_snf_cntrls'] = []
        nd['ep_snf_wrappers'] = []
        for sid in range(num_sockets):
            # v4-dual-socket: pass socket_id to EPSNFController for self-registration
            ep_snf = EPSNFController(
                version=chi_defs.Versions.getVersion(chi_defs.CHI_Cache_Controller),
                ruby_system=ruby_system, node_id=node_id,
                socket_id=sid,
                data_channel_size=params.data_width,
                ep_backend=ep_backend,
                addr_ranges=[NodeConfig.dsm_range_for(nid, seg_size, cfg.phy_base,
                                                        num_sockets, sid)
                             for nid in range(num_nodes)])
            ep_snf_wrapper = _make_ep_node(ruby_system, ep_snf, node_id)
            nd['ep_snf_cntrls'].append(ep_snf)
            nd['ep_snf_wrappers'].append(ep_snf_wrapper)
            if num_sockets == 1:
                setattr(ruby_system, f"ep_snf_node{node_id}", ep_snf_wrapper)
            else:
                setattr(ruby_system, f"ep_snf_node{node_id}_s{sid}", ep_snf_wrapper)
            network_nodes.append(ep_snf_wrapper)
            all_cntrls.append(ep_snf)
        if num_sockets == 1:
            nd['ep_snf_cntrl'] = nd['ep_snf_cntrls'][0]
            nd['ep_snf_wrapper'] = nd['ep_snf_wrappers'][0]

        # v4-dual-socket: Create per-socket HN-F controllers (§3.2 change 3)
        nd['hnf_cntrls'] = []
        nd['hnf_wrappers'] = []
        for sid in range(num_sockets):
            hnf_ranges = [
                cfg.local_private_range(sid),
                cfg.metadata_private_range(sid),
                cfg.metadata_backstore_range(sid),
            ] + [NodeConfig.dsm_range_for(nid, seg_size, cfg.phy_base,
                                          num_sockets, sid)
                 for nid in range(num_nodes)]
            hnf_wrapper, hnf_cntrl = _make_hnf(
                ruby_system, hnf_ranges, HNFCache, node_id)
            configure_l3_dsm_policy(hnf_cntrl)
            nd['hnf_cntrls'].append(hnf_cntrl)
            nd['hnf_wrappers'].append(hnf_wrapper)
            if num_sockets == 1:
                setattr(ruby_system, f"hnf_node{node_id}", hnf_wrapper)
            else:
                setattr(ruby_system, f"hnf_node{node_id}_s{sid}", hnf_wrapper)
            network_nodes.append(hnf_wrapper)
            all_cntrls.append(hnf_cntrl)
        if num_sockets == 1:
            nd['hnf_cntrl'] = nd['hnf_cntrls'][0]
            nd['hnf_wrapper'] = nd['hnf_wrappers'][0]

        # v4-dual-socket: Create per-socket MetaRNF controllers (§3.2 change 4)
        nd['meta_rnf_cntrls'] = []
        nd['meta_rnf_wrappers'] = []
        for sid in range(num_sockets):
            meta_rnf = MetaRNFController(
                version=chi_defs.Versions.getVersion(chi_defs.CHI_Cache_Controller),
                ruby_system=ruby_system, node_id=node_id,
                socket_id=sid,
                flight_slots=ubcc_meta_max_flights,
                data_channel_size=params.data_width,
                addr_ranges=[cfg.metadata_backstore_range(sid)],
                metadata_private_range=cfg.metadata_backstore_range(sid),
                downstream_destinations=[nd['hnf_cntrls'][sid]])
            meta_rnf_wrapper = _make_ep_node(ruby_system, meta_rnf, node_id)
            nd['meta_rnf_cntrls'].append(meta_rnf)
            nd['meta_rnf_wrappers'].append(meta_rnf_wrapper)
            if num_sockets == 1:
                setattr(ruby_system, f"meta_rnf_node{node_id}", meta_rnf_wrapper)
            else:
                setattr(ruby_system, f"meta_rnf_node{node_id}_s{sid}", meta_rnf_wrapper)
            network_nodes.append(meta_rnf_wrapper)
            all_cntrls.append(meta_rnf)
        if num_sockets == 1:
            nd['meta_rnf_cntrl'] = nd['meta_rnf_cntrls'][0]
            nd['meta_rnf_wrapper'] = nd['meta_rnf_wrappers'][0]
            nd['meta_rnf'] = nd['meta_rnf_cntrls'][0]
        else:
            nd['meta_rnf'] = nd['meta_rnf_cntrls'][0]

        # v4-dual-socket: EP-RNF binds ALL local HN-Fs (§3.2 change 5)
        nd['ep_rnf_cntrl'] = EPRNFController(
            version=chi_defs.Versions.getVersion(chi_defs.CHI_Cache_Controller),
            ruby_system=ruby_system, node_id=node_id, num_nodes=num_nodes,
            data_channel_size=params.data_width,
            ep_backend=ep_backend,
            addr_ranges=[NodeConfig.dsm_range_for(
                node_id, seg_size, cfg.phy_base, num_sockets, sid)
                for sid in range(num_sockets)],
            downstream_destinations=[nd['hnf_cntrls'][sid]
                                     for sid in range(num_sockets)])
        nd['ep_rnf_wrapper'] = _make_ep_node(
            ruby_system, nd['ep_rnf_cntrl'], node_id)
        setattr(ruby_system, f"ep_rnf_node{node_id}", nd['ep_rnf_wrapper'])
        network_nodes.append(nd['ep_rnf_wrapper'])
        all_cntrls.append(nd['ep_rnf_cntrl'])

        # v4: Inject EP-RNF MachineVersion into all HN-Fs so they can derive
        # epRnfMachineID in initializeTBE for dir_sharers tracking.
        for hnf_cntrl in nd['hnf_cntrls']:
            hnf_cntrl.epRnfMachineVersion = nd['ep_rnf_cntrl'].version

        nd['clusters'] = []
        node_cpus = _node_cpu_slice(node_id)
        for cluster_i in range(DEFAULT_D):
            cluster_base = cluster_i * DEFAULT_L
            cluster_cpus = node_cpus[cluster_base:cluster_base + DEFAULT_L]

            # v4-dual-socket: explicit socket_id from cluster index
            # TODO: derive from CPU object socket metadata when available
            cluster_socket = cluster_i % num_sockets
            cluster = ClusterCHI_RNF(
                cluster_cpus, ruby_system, cache_line,
                l1i_assoc=2, l1d_assoc=2, l1i_size="32kB", l1d_size="32kB",
                l2_assoc=8, l2_size="256kB",
                socket_id=cluster_socket)
            cluster.addPrivL2Cache()
            setattr(ruby_system,
                    f"cluster_n{node_id}_c{cluster_i}", cluster)
            nd['clusters'].append(cluster)
            network_nodes.append(cluster)
            all_cntrls.extend(cluster.getAllControllers())
            cpu_sequencers.extend(cluster.getSequencers())

        # Q3: Extend deadlock threshold to accommodate UBCC retry delays
        # TC98 fix: Increase from 2G to 10G for hot-contention scenarios (16-core race)
        for seq in cpu_sequencers:
            seq.deadlock_threshold = 10000000000

        # Q2 Fix B: Set L1/L2 addr_ranges to include DSM PA ranges so
        # functionalRead() in populateGrantData() can find cached data
        # in L1/L2 caches.  Without explicit DSM ranges, L1/L2
        # controllers' respondTo() returns false for DSM addresses,
        # causing RubySystem::functionalRead() to skip them.
        # v4-dual-socket: per-socket private + routing + backstore + DSM
        dsm_ranges = [NodeConfig.dsm_range_for(nid, seg_size, cfg.phy_base,
                                                num_sockets, sid)
                       for nid in range(num_nodes)
                       for sid in range(num_sockets)]
        for cluster in nd['clusters']:
            for cntrl in cluster.getAllControllers():
                cntrl.addr_ranges = (
                    cfg.all_local_private_ranges()
                    + cfg.all_metadata_private_ranges()
                    + cfg.all_metadata_backstore_ranges()
                    + dsm_ranges
                )

    for node_id in node_list:
        nd = per_node[node_id]
        # v4-dual-socket: cluster downstream to ALL local HN-Fs (§3.2 change 8)
        hnf_c_list = [nd['hnf_cntrls'][sid] for sid in range(num_sockets)]
        for cluster in nd['clusters']:
            cluster.setDownstream(hnf_c_list)

    for node_id in node_list:
        nd = per_node[node_id]
        # v4-dual-socket: each HN-F only connects to its socket's EP-SNF (§3.2 change 9)
        for sid in range(num_sockets):
            snf_dests = []
            snf_dests.extend(nd['l_snf'].getAllControllers())
            # F1: Route ALL DSM through EP_SNF as the SINGLE downstream.
            # DL_SNF only serves local-private/routing/metadata memory;
            # DSM must not go through DL_SNF.
            snf_dests.append(nd['ep_snf_cntrls'][sid])
            nd['hnf_wrappers'][sid].setDownstream(snf_dests)
            nd['hnf_cntrls'][sid].unproxyParams()

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
    # Each CPU belongs to node_list[cpu_index // cpus_per_node]; build a
    # per-process node-id list so DSM VA mapping uses the correct requesting
    # node base even in split mode (where only the local node's CPUs exist).
    req_node_ids = []
    for _cpu_idx, cpu in enumerate(cpus):
        _nid = node_list[_cpu_idx // cpus_per_node]
        for _proc in cpu.workload:
            req_node_ids.append(_nid)
    setup_dsm_va_mapping(processes, num_nodes, seg_size, num_sockets,
                         req_node_ids=req_node_ids)

    return (cpu_sequencers, [], topology)
