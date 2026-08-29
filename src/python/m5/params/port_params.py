# Copyright (c) 2012-2014, 2017-2019, 2021, 2024-2025 Arm Limited
# All rights reserved.
#
# The license below extends only to copyright in the software and shall
# not be construed as granting a license to any other intellectual
# property including but not limited to intellectual property relating
# to a hardware implementation of the functionality of the software
# licensed hereunder.  You may use the software subject to the license
# terms below provided that you ensure that this notice is replicated
# unmodified and in its entirety in all distributions of the software,
# modified or unmodified, in source code or in binary form.
#
# Copyright (c) 2004-2006 The Regents of The University of Michigan
# Copyright (c) 2010-2011 Advanced Micro Devices, Inc.
# All rights reserved.
#
# Redistribution and use in source and binary forms, with or without
# modification, are permitted provided that the following conditions are
# met: redistributions of source code must retain the above copyright
# notice, this list of conditions and the following disclaimer;
# redistributions in binary form must reproduce the above copyright
# notice, this list of conditions and the following disclaimer in the
# documentation and/or other materials provided with the distribution;
# neither the name of the copyright holders nor the names of its
# contributors may be used to endorse or promote products derived from
# this software without specific prior written permission.
#
# THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
# "AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
# LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
# A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
# OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
# SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
# LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
# DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
# THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
# (INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
# OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.

import copy
import traceback

from .. import proxy
from ..util import (
    Singleton,
    fatal,
)
from .base_params import (
    isSimObject,
    isSimObjectClass,
)

#####################################################################
#
# Port objects
#
# Ports are used to interconnect objects in the memory system.
#
#####################################################################


# Port reference: encapsulates a reference to a particular port on a
# particular SimObject.
class PortRef:
    @staticmethod
    def _safe_parent_chain(obj, limit=12):
        chain = []
        seen = {}
        for depth in range(limit):
            if obj is None:
                chain.append((depth, None, None, None))
                break
            obj_id = id(obj)
            if obj_id in seen:
                chain.append((depth, "CYCLE", hex(obj_id), seen[obj_id]))
                break
            seen[obj_id] = depth
            chain.append((depth, type(obj).__name__,
                          getattr(obj, "_name", None), hex(obj_id)))
            obj = getattr(obj, "_parent", None)
        return chain

    @staticmethod
    def _safe_obj_info(obj):
        if obj is None:
            return {"object": None}

        values = getattr(obj, "_values", None)
        eventq = values.get("eventq_index") if values is not None else None
        parent = getattr(obj, "_parent", None)
        name = getattr(obj, "_name", None)
        parent_value = None
        parent_child = None

        if parent is not None and name is not None:
            parent_values = getattr(parent, "_values", None)
            parent_children = getattr(parent, "_children", None)
            if parent_values is not None:
                parent_value = parent_values.get(name)
            if parent_children is not None:
                parent_child = parent_children.get(name)

        return {
            "type": type(obj).__name__,
            "name": name,
            "obj_id": hex(id(obj)),
            "values_id": hex(id(values)) if values is not None else None,
            "eventq_type": type(eventq).__name__
            if eventq is not None else None,
            "eventq_id": hex(id(eventq)) if eventq is not None else None,
            "eventq_proxy": proxy.isproxy(eventq)
            if eventq is not None else False,
            "parent_type": type(parent).__name__
            if parent is not None else None,
            "parent_name": getattr(parent, "_name", None)
            if parent is not None else None,
            "parent_id": hex(id(parent)) if parent is not None else None,
            "parent_value_id": hex(id(parent_value))
            if parent_value is not None else None,
            "parent_child_id": hex(id(parent_child))
            if parent_child is not None else None,
            "obj_is_parent_value": obj is parent_value,
            "obj_is_parent_child": obj is parent_child,
            "cc_params": getattr(obj, "_ccParams", None) is not None,
            "cc_object_value": getattr(obj, "_ccObject", None),
            "parent_chain": PortRef._safe_parent_chain(obj),
        }

    @staticmethod
    def _root_membership(obj):
        try:
            from m5.objects import Root

            root = Root.getInstance()
        except Exception:
            root = None

        if root is None:
            return False, []

        in_root = False
        candidates = []
        obj_name = getattr(obj, "_name", None)
        obj_type = type(obj).__name__
        obj_parent = getattr(obj, "_parent", None)
        parent_name = getattr(obj_parent, "_name", None)
        parent_type = (
            type(obj_parent).__name__ if obj_parent is not None else None
        )

        try:
            descendants = list(root.descendants())
        except Exception as exc:
            return False, [{"descendants_error": type(exc).__name__}]

        for candidate in descendants:
            if candidate is obj:
                in_root = True
            candidate_parent = getattr(candidate, "_parent", None)
            candidate_parent_type = type(candidate_parent).__name__ \
                if candidate_parent is not None else None
            candidate_parent_name = getattr(candidate_parent, "_name", None) \
                if candidate_parent is not None else None
            if (type(candidate).__name__ == obj_type and
                    getattr(candidate, "_name", None) == obj_name and
                    candidate_parent_type == parent_type and
                    candidate_parent_name == parent_name):
                candidates.append(PortRef._safe_obj_info(candidate))

        return in_root, candidates

    @staticmethod
    def _dump_getport_failure(side, ref):
        obj = getattr(ref, "simobj", None)
        in_root, candidates = PortRef._root_membership(obj)
        print(
            "[GETPORT-FAIL] side={} ref_type={} ref_id={} "
            "port={!r} index={} info={} in_root={} candidates={}".format(
                side, type(ref).__name__, hex(id(ref)),
                getattr(ref, "name", None), getattr(ref, "index", -1),
                PortRef._safe_obj_info(obj), in_root, candidates),
            flush=True,
        )
        traceback.print_exc()

    def __init__(self, simobj, name, role, is_source):
        assert isSimObject(simobj) or isSimObjectClass(simobj)
        self.simobj = simobj
        self.name = name
        self.role = role
        self.is_source = is_source
        self.peer = None  # not associated with another port yet
        self.ccConnected = False  # C++ port connection done?
        self.index = -1  # always -1 for non-vector ports

    def __str__(self):
        return f"{self.simobj}.{self.name}"

    def __len__(self):
        # Return the number of connected ports, i.e. 0 is we have no
        # peer and 1 if we do.
        return int(self.peer != None)

    # for config.ini, print peer's name (not ours)
    def ini_str(self):
        return str(self.peer)

    # for config.json
    def get_config_as_dict(self):
        return {
            "role": self.role,
            "peer": str(self.peer),
            "is_source": str(self.is_source),
        }

    def __getattr__(self, attr):
        if attr == "peerObj":
            # shorthand for proxies
            return self.peer.simobj
        raise AttributeError(
            f"'{self.__class__.__name__}' object has no attribute '{attr}'"
        )

    # Full connection is symmetric (both ways).  Called via
    # SimObject.__setattr__ as a result of a port assignment, e.g.,
    # "obj1.portA = obj2.portB", or via VectorPortElementRef.__setitem__,
    # e.g., "obj1.portA[3] = obj2.portB".
    def connect(self, other):
        if (self.name == "out_port" and
                type(self.simobj).__name__ == "MessageBuffer" and
                getattr(self.simobj, "_name", None) == "reqOut"):
            parent = getattr(self.simobj, "_parent", None)
            print(
                "[REQOUT-CONNECT] reqout_id={} values_id={} "
                "controller_type={} controller_name={!r} "
                "controller_id={} ref_id={} other_type={} other_id={} "
                "other_simobj_type={} other_simobj_id={}".format(
                    hex(id(self.simobj)), hex(id(self.simobj._values)),
                    type(parent).__name__ if parent is not None else None,
                    getattr(parent, "_name", None)
                    if parent is not None else None,
                    hex(id(parent)) if parent is not None else None,
                    hex(id(self)), type(other).__name__, hex(id(other)),
                    type(getattr(other, "simobj", None)).__name__,
                    hex(id(other.simobj))
                    if getattr(other, "simobj", None) is not None else None),
                flush=True,
            )
            traceback.print_stack(limit=10)

        if isinstance(other, VectorPortRef):
            # reference to plain VectorPort is implicit append
            other = other._get_next()
        if self.peer and not proxy.isproxy(self.peer):
            fatal(
                "Port %s is already connected to %s, cannot connect %s\n",
                self,
                self.peer,
                other,
            )
        self.peer = other

        if proxy.isproxy(other):
            other.set_param_desc(PortParamDesc())
            return
        elif not isinstance(other, PortRef):
            raise TypeError(
                f"assigning non-port reference '{other}' to port '{self}'"
            )

        if not Port.is_compat(self, other):
            fatal(
                "Ports %s and %s with roles '%s' and '%s' "
                "are not compatible",
                self,
                other,
                self.role,
                other.role,
            )

        if other.peer is not self:
            other.connect(self)

    # Allow a compatible port pair to be spliced between a port and its
    # connected peer. Useful operation for connecting instrumentation
    # structures into a system when it is necessary to connect the
    # instrumentation after the full system has been constructed.
    def splice(self, new_1, new_2):
        if not self.peer or proxy.isproxy(self.peer):
            fatal("Port %s not connected, cannot splice in new peers\n", self)

        if not isinstance(new_1, PortRef) or not isinstance(new_2, PortRef):
            raise TypeError(
                f"Splicing non-port references '{new_1}','{new_2}' to port '{self}'"
            )

        old_peer = self.peer

        if Port.is_compat(old_peer, new_1) and Port.is_compat(self, new_2):
            old_peer.peer = new_1
            new_1.peer = old_peer
            self.peer = new_2
            new_2.peer = self
        elif Port.is_compat(old_peer, new_2) and Port.is_compat(self, new_1):
            old_peer.peer = new_2
            new_2.peer = old_peer
            self.peer = new_1
            new_1.peer = self
        else:
            fatal(
                "Ports %s(%s) and %s(%s) can't be compatibly spliced with "
                "%s(%s) and %s(%s)",
                self,
                self.role,
                old_peer,
                old_peer.role,
                new_1,
                new_1.role,
                new_2,
                new_2.role,
            )

    def clone(self, simobj, memo):
        if self in memo:
            return memo[self]
        newRef = copy.copy(self)
        memo[self] = newRef
        newRef.simobj = simobj
        assert isSimObject(newRef.simobj)
        if self.peer and not proxy.isproxy(self.peer):
            peerObj = self.peer.simobj(_memo=memo)
            newRef.peer = self.peer.clone(peerObj, memo)
            assert not isinstance(newRef.peer, VectorPortRef)
        return newRef

    def unproxy(self, simobj):
        assert simobj is self.simobj
        if proxy.isproxy(self.peer):
            try:
                realPeer = self.peer.unproxy(self.simobj)
            except:
                print(
                    f"Error in unproxying port '{self.name}' of {self.simobj.path()}"
                )
                raise
            self.connect(realPeer)

    # Call C++ to create corresponding port connection between C++ objects
    def ccConnect(self):
        if self.ccConnected:
            return

        peer = self.peer
        if not peer:
            return

        interesting = (
            (self.name == "out_port" and
             type(self.simobj).__name__ == "MessageBuffer" and
             getattr(self.simobj, "_name", None) == "reqOut") or
            (getattr(peer, "name", None) == "out_port" and
             type(getattr(peer, "simobj", None)).__name__ ==
             "MessageBuffer" and
             getattr(getattr(peer, "simobj", None), "_name", None) ==
             "reqOut")
        )

        if interesting:
            print(
                "[REQOUT-CCCONNECT] self_ref={} self_info={} "
                "peer_ref={} peer_info={} symmetric={}".format(
                    {
                        "type": type(self).__name__,
                        "id": hex(id(self)),
                        "port": self.name,
                        "index": getattr(self, "index", -1),
                    },
                    PortRef._safe_obj_info(self.simobj),
                    {
                        "type": type(peer).__name__,
                        "id": hex(id(peer)),
                        "port": getattr(peer, "name", None),
                        "index": getattr(peer, "index", -1),
                    },
                    PortRef._safe_obj_info(getattr(peer, "simobj", None)),
                    getattr(peer, "peer", None) is self),
                flush=True,
            )

        try:
            port = self.simobj.getPort(self.name, self.index)
        except Exception:
            PortRef._dump_getport_failure("self", self)
            raise

        try:
            peer_port = peer.simobj.getPort(peer.name, peer.index)
        except Exception:
            PortRef._dump_getport_failure("peer", peer)
            raise

        port.bind(peer_port)

        self.ccConnected = True


# A reference to an individual element of a VectorPort... much like a
# PortRef, but has an index.
class VectorPortElementRef(PortRef):
    def __init__(self, simobj, name, role, is_source, index):
        PortRef.__init__(self, simobj, name, role, is_source)
        self.index = index

    def __str__(self):
        return "%s.%s[%d]" % (self.simobj, self.name, self.index)


# A reference to a complete vector-valued port (not just a single element).
# Can be indexed to retrieve individual VectorPortElementRef instances.
class VectorPortRef:
    def __init__(self, simobj, name, role, is_source):
        assert isSimObject(simobj) or isSimObjectClass(simobj)
        self.simobj = simobj
        self.name = name
        self.role = role
        self.is_source = is_source
        self.elements = []

    def __str__(self):
        return f"{self.simobj}.{self.name}[:]"

    def __len__(self):
        # Return the number of connected peers, corresponding the the
        # length of the elements.
        return len(self.elements)

    # for config.ini, print peer's name (not ours)
    def ini_str(self):
        return " ".join([el.ini_str() for el in self.elements])

    # for config.json
    def get_config_as_dict(self):
        return {
            "role": self.role,
            "peer": [el.ini_str() for el in self.elements],
            "is_source": str(self.is_source),
        }

    def __getitem__(self, key):
        if not isinstance(key, int):
            raise TypeError("VectorPort index must be integer")
        if key >= len(self.elements):
            # need to extend list
            ext = [
                VectorPortElementRef(
                    self.simobj, self.name, self.role, self.is_source, i
                )
                for i in range(len(self.elements), key + 1)
            ]
            self.elements.extend(ext)
        return self.elements[key]

    def _get_next(self):
        return self[len(self.elements)]

    def __setitem__(self, key, value):
        if not isinstance(key, int):
            raise TypeError("VectorPort index must be integer")
        self[key].connect(value)

    def connect(self, other):
        if isinstance(other, (list, tuple)):
            # Assign list of port refs to vector port.
            # For now, append them... not sure if that's the right semantics
            # or if it should replace the current vector.
            for ref in other:
                self._get_next().connect(ref)
        else:
            # scalar assignment to plain VectorPort is implicit append
            self._get_next().connect(other)

    def clone(self, simobj, memo):
        if self in memo:
            return memo[self]
        newRef = copy.copy(self)
        memo[self] = newRef
        newRef.simobj = simobj
        assert isSimObject(newRef.simobj)
        newRef.elements = [el.clone(simobj, memo) for el in self.elements]
        return newRef

    def unproxy(self, simobj):
        [el.unproxy(simobj) for el in self.elements]

    def ccConnect(self):
        [el.ccConnect() for el in self.elements]


# Port description object.  Like a ParamDesc object, this represents a
# logical port in the SimObject class, not a particular port on a
# SimObject instance.  The latter are represented by PortRef objects.
class Port:
    # Port("role", "description")

    _compat_dict = {}

    @classmethod
    def compat(cls, role, peer):
        cls._compat_dict.setdefault(role, set()).add(peer)
        cls._compat_dict.setdefault(peer, set()).add(role)

    @classmethod
    def is_compat(cls, one, two):
        for port in one, two:
            if not port.role in Port._compat_dict:
                fatal("Unrecognized role '%s' for port %s\n", port.role, port)
        return one.role in Port._compat_dict[two.role]

    def __init__(self, role, desc, is_source=False):
        self.desc = desc
        self.role = role
        self.is_source = is_source

    # Generate a PortRef for this port on the given SimObject with the
    # given name
    def makeRef(self, simobj):
        return PortRef(simobj, self.name, self.role, self.is_source)

    # Connect an instance of this port (on the given SimObject with
    # the given name) with the port described by the supplied PortRef
    def connect(self, simobj, ref):
        self.makeRef(simobj).connect(ref)

    # No need for any pre-declarations at the moment as we merely rely
    # on an unsigned int.
    def cxx_predecls(self, code):
        pass

    def pybind_predecls(self, code):
        cls.cxx_predecls(self, code)

    # Declare an unsigned int with the same name as the port, that
    # will eventually hold the number of connected ports (and thus the
    # number of elements for a VectorPort).
    def cxx_decl(self, code):
        code("unsigned int port_${{self.name}}_connection_count;")


Port.compat("GEM5 REQUESTOR", "GEM5 RESPONDER")


class RequestPort(Port):
    # RequestPort("description")
    def __init__(self, desc):
        super().__init__("GEM5 REQUESTOR", desc, is_source=True)


class ResponsePort(Port):
    # ResponsePort("description")
    def __init__(self, desc):
        super().__init__("GEM5 RESPONDER", desc)


# VectorPort description object.  Like Port, but represents a vector
# of connections (e.g., as on a XBar).
class VectorPort(Port):
    def makeRef(self, simobj):
        return VectorPortRef(simobj, self.name, self.role, self.is_source)


class VectorRequestPort(VectorPort):
    # VectorRequestPort("description")
    def __init__(self, desc):
        super().__init__("GEM5 REQUESTOR", desc, is_source=True)


class VectorResponsePort(VectorPort):
    # VectorResponsePort("description")
    def __init__(self, desc):
        super().__init__("GEM5 RESPONDER", desc)


# Old names, maintained for compatibility.
MasterPort = RequestPort
SlavePort = ResponsePort
VectorMasterPort = VectorRequestPort
VectorSlavePort = VectorResponsePort


# 'Fake' ParamDesc for Port references to assign to the _pdesc slot of
# proxy objects (via set_param_desc()) so that proxy error messages
# make sense.
class PortParamDesc(metaclass=Singleton):
    ptype_str = "Port"
    ptype = Port
