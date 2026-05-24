"""UBCC Basic Framework - Ruby CHI multi-node topology builder.
Creates N=3, L=2, D=2 topology with EP endpoints.
HN_i routes by address classification to L_SNF_i / DL_SNF_i / EP_SNF_i.
RN-F downstream: same-node HN only (TC-TOPO-2).
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


def _make_snf(ruby_system, addr_ranges):
    snf = chi_defs.CHI_SNF_MainMem(ruby_system, None, None)
    snf._cntrl.addr_ranges = addr_ranges
    return snf


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

    per_node = {nid: {} for nid in range(num_nodes)}

    total_cpus = num_nodes * DEFAULT_D * DEFAULT_L
    assert len(cpus) == total_cpus, \
        f"Need {total_cpus} CPUs, got {len(cpus)}"

    for node_id in range(num_nodes):
        nd = per_node[node_id]
        nd['hnf_wrapper'], nd['hnf_cntrl'] = _make_hnf(
            ruby_system,
            [NodeConfig(node_id, num_nodes, seg_size).local_private_range,
             NodeConfig(node_id, num_nodes, seg_size).ubcc_exclusive_range],
            HNFCache, node_id)

        setattr(ruby_system, f"hnf_node{node_id}", nd['hnf_wrapper'])
        network_nodes.append(nd['hnf_wrapper'])
        all_cntrls.append(nd['hnf_cntrl'])

        nd['l_snf'] = _make_snf(
            ruby_system,
            [NodeConfig(node_id, num_nodes, seg_size).local_private_range,
             NodeConfig(node_id, num_nodes, seg_size).ubcc_exclusive_range])
        setattr(ruby_system, f"l_snf_node{node_id}", nd['l_snf'])
        network_nodes.append(nd['l_snf'])
        all_cntrls.extend(nd['l_snf'].getAllControllers())

        nd['dl_snf'] = _make_snf(
            ruby_system,
            [NodeConfig.dsm_range_for(node_id, seg_size)])
        setattr(ruby_system, f"dl_snf_node{node_id}", nd['dl_snf'])
        network_nodes.append(nd['dl_snf'])
        all_cntrls.extend(nd['dl_snf'].getAllControllers())

        ep_backend = EPBackend(node_id=node_id)

        nd['ep_rnf_cntrl'] = EPRNFController(
            version=chi_defs.Versions.getVersion(chi_defs.CHI_Cache_Controller),
            ruby_system=ruby_system, node_id=node_id,
            data_channel_size=params.data_width,
            ep_backend=ep_backend,
            addr_ranges=[NodeConfig.dsm_range_for(
                node_id, seg_size)])
        nd['ep_rnf_wrapper'] = _make_ep_node(
            ruby_system, nd['ep_rnf_cntrl'], node_id)
        setattr(ruby_system, f"ep_rnf_node{node_id}", nd['ep_rnf_wrapper'])
        network_nodes.append(nd['ep_rnf_wrapper'])
        all_cntrls.append(nd['ep_rnf_cntrl'])

        nd['ep_snf_cntrl'] = EPSNFController(
            version=chi_defs.Versions.getVersion(chi_defs.CHI_Cache_Controller),
            ruby_system=ruby_system, node_id=node_id,
            data_channel_size=params.data_width,
            ep_backend=ep_backend,
            addr_ranges=[NodeConfig.dsm_range_for(nid, seg_size)
                         for nid in range(num_nodes)
                         if nid != node_id])
        nd['ep_snf_wrapper'] = _make_ep_node(
            ruby_system, nd['ep_snf_cntrl'], node_id)
        setattr(ruby_system, f"ep_snf_node{node_id}", nd['ep_snf_wrapper'])
        network_nodes.append(nd['ep_snf_wrapper'])
        all_cntrls.append(nd['ep_snf_cntrl'])

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

    for node_id in range(num_nodes):
        nd = per_node[node_id]
        hnf_c_list = [nd['hnf_cntrl']]
        for cluster in nd['clusters']:
            cluster.setDownstream(hnf_c_list)

    for node_id in range(num_nodes):
        nd = per_node[node_id]
        snf_dests = []
        snf_dests.extend(nd['l_snf'].getAllControllers())
        snf_dests.extend(nd['dl_snf'].getAllControllers())
        snf_dests.append(nd['ep_snf_cntrl'])
        nd['hnf_wrapper'].setDownstream(snf_dests)

    for cntrl in all_cntrls:
        cntrl.data_channel_size = params.data_width

    ruby_system.network.number_of_virtual_networks = 4
    ruby_system.network.control_msg_size = params.cntrl_msg_size
    ruby_system.network.data_msg_size = params.data_width

    for k in dir(params):
        if not k.startswith("__"):
            setattr(options, k, getattr(params, k))

    mem_dests = []
    nd0 = per_node[0]
    mem_dests.extend(nd0['l_snf'].getAllControllers())

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

    return (cpu_sequencers, mem_dests, topology)
