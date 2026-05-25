# Basic Framework SE-mode configuration for UBCC
# Phase 1: Address and Process Control
# Phase 2+: Ruby/CHI topology

import argparse
import sys

import m5
from m5.objects import *
from m5.util import addToPath

addToPath("../../")

from ruby.CHI_basic_framework_config import (
    NodeConfig, NodeAddressMap,
    DEFAULT_N, DEFAULT_L, DEFAULT_D, DEFAULT_SEG_SIZE,
)


def config_system(args):
    system = System()

    system.clk_domain = SrcClockDomain()
    system.clk_domain.clock = args.clock
    system.clk_domain.voltage_domain = VoltageDomain()

    system.mem_mode = args.mem_mode

    system.workload = SEWorkload.init_compatible(args.cmd)

    num_nodes = DEFAULT_N

    seg_size = DEFAULT_SEG_SIZE

    addr_map = NodeAddressMap(num_nodes, seg_size)

    dsm_va_base = MaxAddr - 4 * seg_size

    # DSM VA base is a Python local — not attached to system SimObject
    dsm_va_end = dsm_va_base + num_nodes * seg_size

    node_memories = []
    for node_id in range(num_nodes):
        cfg = NodeConfig(node_id, num_nodes, seg_size)
        membus = SystemXBar()
        system.addChildByName(f"node{node_id}_membus", membus)

        local_mem = SimpleMemory(range=cfg.local_private_range)
        local_mem.port = membus.mem_side_ports
        system.addChildByName(f"node{node_id}_local_mem", local_mem)

        ubcc_mem = SimpleMemory(range=cfg.ubcc_exclusive_range)
        ubcc_mem.port = membus.mem_side_ports
        system.addChildByName(f"node{node_id}_ubcc_mem", ubcc_mem)

        dsm_node_mem = SimpleMemory(
            range=NodeConfig.dsm_range_for(node_id, seg_size, cfg.phy_base))
        dsm_node_mem.port = membus.mem_side_ports
        system.addChildByName(f"node{node_id}_dsm_mem", dsm_node_mem)

        node_memories.append({
            'membus': membus,
            'local_mem': local_mem,
            'ubcc_mem': ubcc_mem,
            'dsm_mem': dsm_node_mem,
        })

    system.memories = [
        system.node0_local_mem, system.node0_ubcc_mem, system.node0_dsm_mem,
        system.node1_local_mem, system.node1_ubcc_mem, system.node1_dsm_mem,
        system.node2_local_mem, system.node2_ubcc_mem, system.node2_dsm_mem,
    ]

    system.membus = SystemXBar()
    system.addChildByName("system_membus", system.membus)

    for node_id in range(num_nodes):
        getattr(system, f"node{node_id}_membus").mem_side_ports = \
            system.membus.cpu_side_ports

    total_cpus = num_nodes * DEFAULT_L * DEFAULT_D
    args.num_cpus = total_cpus

    cpu_list = []
    cpu_clusters = []

    cpu_idx = 0
    for node_id in range(num_nodes):
        node_cpus = []
        for cluster_i in range(DEFAULT_D):
            cluster_cpus = []
            for core_i in range(DEFAULT_L):
                if args.cpu_type == "atomic":
                    cpu = AtomicSimpleCPU(cpu_id=cpu_idx)
                elif args.cpu_type == "timing":
                    cpu = TimingSimpleCPU(cpu_id=cpu_idx)
                else:
                    cpu = AtomicSimpleCPU(cpu_id=cpu_idx)
                cpu.createThreads()
                cpu.phys_pool_id = node_id * 3

                cpu.icache_port = \
                    getattr(system, f"node{node_id}_membus").cpu_side_ports
                cpu.dcache_port = \
                    getattr(system, f"node{node_id}_membus").cpu_side_ports

                proc = system.workload.createProcess()
                proc.cmd = [args.cmd]
                proc.executable = args.cmd
                proc.phys_pool_id = node_id * 3

                for nid in range(num_nodes):
                    dsm_pa_base = addr_map.dsmLocalBase(nid)
                    dsm_va = dsm_va_base + nid * seg_size
                    proc.map(dsm_va, dsm_pa_base, seg_size, cacheable=True)

                cpu.workload = [proc]
                cpu_list.append(cpu)
                cluster_cpus.append(cpu)
                cpu_idx += 1
            cpu_clusters.append(cluster_cpus)
        node_cpus_per_cluster = cpu_clusters[-DEFAULT_D:]

    system.cpu = cpu_list
    system.cpu_clusters = cpu_clusters
    system.mem_ranges = system.memories[0].range
    for mem in system.memories[1:]:
        system.mem_ranges = AddrRangeList(list(system.mem_ranges) + list(mem.range))

    system.workload.setSystem(system)

    root = Root(full_system=False, system=system)
    m5.instantiate()

    for i, cpu in enumerate(cpu_list):
        print(f"[node_id={i // (DEFAULT_D * DEFAULT_L)}] "
              f"cpu {i}: phys_pool_id={cpu.phys_pool_id}")
    print(f"DSM VA base: {hex(dsm_va_base)}")
    print(f"DSM local base per node: {[hex(addr_map.dsmLocalBase(nid)) for nid in range(num_nodes)]}")
    print("Instantiated N=%d L=%d D=%d system" %
          (num_nodes, DEFAULT_L, DEFAULT_D))

    exit_event = m5.simulate()
    print(f"Exiting @ tick {m5.curTick()} because {exit_event.getCause()}")
    sys.exit(0 if exit_event.getCode() == 0 else 1)


def main():
    parser = argparse.ArgumentParser(description="UBCC Basic Framework SE")
    parser.add_argument("--clock", type=str, default="2GHz")
    parser.add_argument("--mem-mode", type=str, default="timing",
                        choices=["timing", "atomic", "atomic_noncaching"])
    parser.add_argument("--cpu-type", type=str, default="atomic",
                        choices=["atomic", "timing"])
    parser.add_argument("cmd", type=str, help="Command to run")
    parser.add_argument("options", nargs=argparse.REMAINDER,
                        help="Options for the command")
    args = parser.parse_args()
    config_system(args)


if __name__ == "__m5_main__":
    main()
