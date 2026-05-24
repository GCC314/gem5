"""UBCC Basic Framework - Ruby CHI multi-node topology builder.
Creates N=3, L=2, D=2 topology with EP endpoints.
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


def create_ubcc_system(options, full_system, system, dma_ports, bootmem,
                        ruby_system, cpus):
    if buildEnv["PROTOCOL"] != "CHI":
        m5.panic("UBCC framework requires CHI protocol build")

    num_nodes = DEFAULT_N
    seg_size = DEFAULT_SEG_SIZE
    addr_map = NodeAddressMap(num_nodes, seg_size)

    params = chi_defs.NoC_Params
    NodeCls = chi_defs.CHI_Node

    class HNFCache(RubyCache):
        dataAccessLatency = 10
        tagAccessLatency = 2
        size = getattr(options, "l3_size", "256kB")
        assoc = getattr(options, "l3_assoc", 16)

    assert system.cache_line_size.value == getattr(options, "cacheline_size", 64)

    cpu_sequencers = []
    network_nodes = []
    all_cntrls = []

    total_cpus = num_nodes * DEFAULT_L * DEFAULT_D
    assert len(cpus) == total_cpus, \
        f"Expected {total_cpus} CPUs, got {len(cpus)}"

    cpu_idx = 0
    rnf_clusters = []

    for node_id in range(num_nodes):
        node_cfg = NodeConfig(node_id, num_nodes, seg_size)

        hn_addr_ranges = [node_cfg.local_private_range]
        for nid in range(num_nodes):
            hn_addr_ranges.append(
                NodeConfig.dsm_range_for(nid, seg_size))

        hnf_cache = HNFCache()
        hnf_cntrl = chi_defs.CHI_HNFController(
            ruby_system, hnf_cache, NULL, hn_addr_ranges)
        hnf_node = HNNodeWrapper(ruby_system)
        hnf_node.setController(hnf_cntrl)
        hnf_node.connectController(hnf_cntrl)
        setattr(ruby_system, f"hnf_node{node_id}", hnf_node)

        network_nodes.append(hnf_node)
        all_cntrls.append(hnf_cntrl)

        l_snf = chi_defs.CHI_SNF_MainMem(ruby_system, None, None)
        l_snf._cntrl.addr_ranges = [node_cfg.local_private_range]
        setattr(ruby_system, f"l_snf_node{node_id}", l_snf)
        network_nodes.append(l_snf)
        all_cntrls.extend(l_snf.getAllControllers())

        dl_snf = chi_defs.CHI_SNF_MainMem(ruby_system, None, None)
        dl_snf._cntrl.addr_ranges = [NodeConfig.dsm_range_for(
            node_id, seg_size)]
        setattr(ruby_system, f"dl_snf_node{node_id}", dl_snf)
        network_nodes.append(dl_snf)
        all_cntrls.extend(dl_snf.getAllControllers())

        ep_backend = EPBackend(node_id=node_id)

        ep_rnf = EPRNFController(
            ruby_system=ruby_system, node_id=node_id,
            data_channel_size=params.data_width,
            ep_backend=ep_backend)
        cls_ep_rnf = EPNodeWrapper(ruby_system)
        cls_ep_rnf.setController(ep_rnf)
        cls_ep_rnf.connectController(ep_rnf)
        setattr(ruby_system, f"ep_rnf_node{node_id}", cls_ep_rnf)
        network_nodes.append(cls_ep_rnf)
        all_cntrls.append(ep_rnf)

        ep_snf = EPSNFController(
            ruby_system=ruby_system, node_id=node_id,
            data_channel_size=params.data_width,
            ep_backend=ep_backend)
        cls_ep_snf = EPNodeWrapper(ruby_system)
        cls_ep_snf.setController(ep_snf)
        cls_ep_snf.connectController(ep_snf)
        setattr(ruby_system, f"ep_snf_node{node_id}", cls_ep_snf)
        network_nodes.append(cls_ep_snf)
        all_cntrls.append(ep_snf)

        for cluster_i in range(DEFAULT_D):
            cluster_cpus = cpus[cluster_i * DEFAULT_L:
                                 (cluster_i + 1) * DEFAULT_L]
            cluster = ClusterCHI_RNF(
                cluster_cpus, ruby_system, system.cache_line_size.value)
            cluster.addPrivL2Cache()
            setattr(ruby_system, f"cluster_n{node_id}_c{cluster_i}", cluster)
            rnf_clusters.append(cluster)
            network_nodes.append(cluster)
            all_cntrls.extend(cluster.getAllControllers())
            cpu_sequencers.extend(cluster.getSequencers())

    hnf_dests = []
    for node_id in range(num_nodes):
        hnf = getattr(ruby_system, f"hnf_node{node_id}")
        hnf_dests.extend(hnf.getAllControllers())

    mem_dests = []
    ep_snf_dests = []
    for node_id in range(num_nodes):
        l_snf = getattr(ruby_system, f"l_snf_node{node_id}")
        mem_dests.extend(l_snf.getAllControllers())
        dl_snf = getattr(ruby_system, f"dl_snf_node{node_id}")
        mem_dests.extend(dl_snf.getAllControllers())
        ep_snf = getattr(ruby_system, f"ep_snf_node{node_id}")
        mem_dests.extend(ep_snf.getAllControllers())
        ep_snf_dests.append(ep_snf.getAllControllers()[0])

    for cluster in rnf_clusters:
        cluster.setDownstream(hnf_dests)

    for node_id in range(num_nodes):
        hnf = getattr(ruby_system, f"hnf_node{node_id}")
        hnf.setDownstream(mem_dests)

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

    return (cpu_sequencers, mem_dests, topology)
