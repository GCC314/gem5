#include "mem/ruby/protocol/chi/ep/EPSNFController.hh"

#include <cstring>

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "debug/RubyEP.hh"
#include "debug/RubyEPVerbose.hh"
#include "mem/ruby/common/DataBlock.hh"
#include "mem/ruby/protocol/CHI/CHIDataMsg.hh"
#include "mem/ruby/protocol/CHI/CHIRequestMsg.hh"
#include "mem/ruby/protocol/CHI/CHIResponseMsg.hh"
#include "mem/ruby/protocol/chi/ep/UBAdapter.hh"
#include "params/EPSNFController.hh"
#include "sim/eventq.hh"

namespace gem5
{

namespace ruby
{

using namespace CHI;

// ---- SimObject param → static locals (set by EPSNFController::init) ----
static uint64_t s_retry_cycles = 0;        // from _params.retry_cycles
static uint64_t s_delta_noc_cycles = 0;    // from _params.delta_noc_cycles

static uint64_t epsnf_retry_cycles() {
    return s_retry_cycles;
}

static uint64_t epsnf_delta_noc_cycles() {
    return s_delta_noc_cycles;
}

EPSNFController::EPSNFController(const Params &p)
  : EPController(p), _backend(p.ep_backend),
    _socketId(p.socket_id)
{
    // allocateWriteIdentity reserves 4/1/16/32 bits for node/socket/controller/
    // sequence. These are protocol configuration limits, not truncating casts.
    fatal_if(_nodeId < 0 || _nodeId >= 16,
             "EP_SNF node_id=%d: write identity supports project nodes [0,15]",
             _nodeId);
    fatal_if(_socketId < 0 || _socketId >= 2,
             "EP_SNF node_id=%d: write identity supports sockets [0,1], got %d",
             _nodeId, _socketId);
    fatal_if(m_version > 0xffff,
             "EP_SNF node_id=%d socket=%d: controller version %d exceeds "
             "16-bit write identity namespace", _nodeId, _socketId, m_version);
    // v4-dual-socket: self-register with EPBackend
    if (_backend) {
        _backend->registerEpSnf(_socketId, this);
    }
}

void
EPSNFController::init()
{
    EPController::init();
    DPRINTF(RubyEPVerbose, "[WB-DIAG] stage=ARMED tracePa=0x14030000 node=%d socket=%d\n",
            _nodeId, _socketId);
    if (debug::RubyEPVerbose) {
        trace::output().flush();
    }
    fatal_if(!_backend, "EP_SNF node_id=%d: no backend attached", _nodeId);

    // Phase 1: Store SimObject params into file-local statics.
    // Params always take effect (no env fallback).
    s_retry_cycles = params().retry_cycles;
    s_delta_noc_cycles = params().delta_noc_cycles;

    // F4: selfTest disabled (§gap_analysis)
    // selfTest();
}

void
EPSNFController::selfTest()
{
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d selfTest begin\n", _nodeId);

    auto test_req = std::make_shared<CHIRequestMsg>(
        curTick(), cacheLineSize, m_ruby_system);
    test_req->m_addr = 0x10000000;
    test_req->m_type = CHIRequestType_ReadNoSnp;
    test_req->m_requestor = m_machineID;

    auto *req_buf = reqIn;
    if (req_buf->areNSlotsAvailable(1, curTick())) {
        req_buf->enqueue(test_req, curTick(), cyclesToTicks(Cycles(1)),
                         false, false);
        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d selfTest: injected ReadNoSnp\n", _nodeId);
    }

    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d selfTest end\n", _nodeId);
}

void
EPSNFController::wakeup()
{
    EPController::wakeup();

    processPendingOutputs();

    // Q3: Send deferred CompData (1-tick delay for TBE race fix)
    processDeferredData();

    // v4: Send deferred grants (§4.4.2, I10 timing invariant)
    processDeferredGrants();

    // Phase 2 async: Poll backend first so newly-arrived QLM responses
    // are available for pending writeback evaluation (§5.3).
    if (_backend)
        _backend->wakeup();

    processPendingHAWrites();

    // Q3: Process retry queue — request grants that were previously BUSY
    if (!_retryQueue.empty()) {
        bool needWakeup = false;
        for (auto it = _retryQueue.begin(); it != _retryQueue.end(); ) {
            if (it->parkPhase != 0) {
                ++it;
                continue;
            }
            if (it->nativeDataPublished) {
                ++it;
                continue;
            }
            if (_pendingData.size() + _deferredCompData.size() +
                    dataMsgsPerLine > 128 ||
                (it->needsReadReceipt && _pendingResponses.size() +
                    2 * _pendingWrites.size() >= 127)) {
                needWakeup = true;
                break;
            }
            int homeNode = -1;
            if (!_backend->holdAuthority(it->linePa, it->authorityBorrow)) {
                needWakeup = true;
                ++it;
                continue;
            }
            int grantResult = _backend->handleRemoteMiss(
                it->linePa, it->neededPerm, it->writeIntent,
                it->ingressSocket, homeNode);
            if (grantResult >= 0) {
                // Grant succeeded — send CompData (same as normal flow)
                NetDest hnDest(m_ruby_system);
                hnDest.add(it->hnReq);
                NetDest dataDest(m_ruby_system);
                if (it->dataToFwdReq)
                    dataDest.add(it->fwdReq);
                else
                    dataDest.add(it->hnReq);

                // v4: Determine CompData type and shared_hint from neededPerm
                CHIDataType dataType = (it->neededPerm == 0)
                    ? CHIDataType_CompData_SC
                    : CHIDataType_CompData_UC;
                bool sharedHint = (it->neededPerm == 0);

                DataBlock grantData(cacheLineSize);
                GrantDataSource grantSource = GrantDataSource::NoData;
                int grantDataState = _backend->takeGrantData(
                    it->linePa, grantData, grantSource);
                fatal_if(grantDataState < 0,
                         "EP_SNF node_id=%d: missing grant data state PA=0x%lx",
                         _nodeId, it->linePa);
                fatal_if(grantDataState == 0 &&
                             grantSource != GrantDataSource::NoData,
                         "EP_SNF node_id=%d: unavailable grant data PA=0x%lx source=%d",
                          _nodeId, it->linePa, static_cast<int>(grantSource));
                // Early HN retirement is legal only once the complete line is
                // local and all native data beats can drain without Home.
                if (it->needsReadReceipt) {
                    auto rsp = std::make_shared<CHIResponseMsg>(
                        curTick(), cacheLineSize, m_ruby_system,
                        it->linePa, CHIResponseType_ReadReceipt,
                        m_machineID, hnDest, false, false,
                        it->nativeTxnId, 0, MessageSizeType_Control);
                    sendResponseReliable(rsp);
                }
                int storeRequester = -1;
                uint64_t storeEpoch = 0;
                fatal_if(!_backend->getStoreAuthorization(
                             it->linePa, it->ingressSocket, storeRequester,
                             storeEpoch),
                         "EP_SNF node_id=%d: missing grant authorization "
                         "PA=0x%lx", _nodeId, it->linePa);
                const bool partialMicro = params().park_microtest_partial &&
                    !params().park_microtest &&
                    !_parkMicrotestIssued;
                if (partialMicro) {
                    fatal_if(dataMsgsPerLine != 2,
                             "Partial Park micro requires exactly two native beats");
                    _parkMicrotestIssued = true;
                    it->nativeDataPublished = true;
                }
                for (int i = 0; i < dataMsgsPerLine; i++) {
                    int offset = i * dataChannelSize;
                    int chunkSize = (i == dataMsgsPerLine - 1) ?
                        (cacheLineSize - offset) : dataChannelSize;
                    WriteMask wm(cacheLineSize);
                    wm.setMask(offset, chunkSize);
                    DataBlock db(cacheLineSize);
                    if (grantDataState > 0) {
                        const uint8_t *chunk = grantData.getData(offset, chunkSize);
                        db.setData(chunk, offset, chunkSize);
                    }
                    auto dat = std::make_shared<CHIDataMsg>(
                        curTick(), cacheLineSize, m_ruby_system);
                    dat->m_addr = it->linePa;
                    dat->m_ep_native_id = it->dataToFwdReq ? 0 : it->nativeIncarnation;
                    dat->m_type = dataType;
                    dat->m_responder = m_machineID;
                    dat->m_Destination = dataDest;
                    dat->m_dataBlk = db;
                    dat->m_bitMask = wm;
                    dat->m_MessageSize = MessageSizeType_Data;
                    // v4: Set shared_hint for shared grants
                    dat->m_m_shared_hint = sharedHint;
                    dat->m_ubcc_store_auth_valid = true;
                    dat->m_ubcc_store_requester = storeRequester;
                    dat->m_ubcc_permission_epoch = storeEpoch;
                    const bool lastBeat = (i == dataMsgsPerLine - 1);
                    std::function<void()> onSent;
                    if (lastBeat &&
                        (_backend->haEndpointEnabled() || it->publishOnData)) {
                        onSent = [this, linePa = it->linePa,
                                  neededPerm = it->neededPerm,
                                  writeIntent = it->writeIntent,
                                  publishOnData = it->publishOnData] {
                            // HA InstallAck is emitted by the HN final observer,
                            // not when the SN merely enqueues its last data beat.
                            if (publishOnData) {
                                _backend->notifyLocalLinePublished(
                                    linePa, _socketId);
                            }
                        };
                    }
                    if (partialMicro && lastBeat) {
                        // Bounded, test-only delayed second beat. Data is real
                        // and already present locally; no fake grant or ACK.
                        schedule(new EventFunctionWrapper(
                            [this, dat, onSent] {
                                sendDataReliable(dat, onSent);
                            }, name() + ".partialParkData", true),
                            clockEdge(Cycles(80)));
                    } else {
                        sendDataReliable(dat, std::move(onSent));
                    }
                }
                if (partialMicro) {
                    const Addr line = it->linePa;
                    const uint64_t control = allocateWriteIdentity();
                    schedule(new EventFunctionWrapper([this, line, control] {
                        fatal_if(!parkAcquisition(line, control, [this, line, control] {
                            inform("[PARK-MICRO] stage=PARKED node=%d line=%#lx control=%lu",
                                   _nodeId, line, control);
                            _backend->getEpRnfController()->startCleanUnique(
                                line, [this, line, control](bool ok) {
                                    fatal_if(!ok, "Partial Park native control failed");
                                    inform("[PARK-MICRO] stage=LOCAL_CONTROL_DONE node=%d line=%#lx control=%lu",
                                           _nodeId, line, control);
                                    resumeAcquisition(line, control);
                                }, _socketId);
                        }), "Partial Park admission failed");
                    }, name() + ".partialPark", true), clockEdge(Cycles(16)));
                    ++it;
                } else {
                    it->nativeDataPublished = true;
                    ++it;
                }
            } else {
                needWakeup = true;
                ++it;
            }
        }
        if (needWakeup)
            scheduleEvent(Cycles(epsnf_retry_cycles()));
    }
}

void
EPSNFController::print(std::ostream& out) const
{
    out << "[EP_SNF node_id=" << _nodeId << " v=" << m_version << "]";
}

uint64_t
EPSNFController::allocateWriteIdentity()
{
    fatal_if(_nextWriteIdentity > 0xffffffffULL,
             "EP_SNF node_id=%d socket=%d: write identity exhausted",
             _nodeId, _socketId);
    // Keep endpoint publication identities disjoint from UBAdapter's ordinary
    // low, monotonically allocated request IDs.  Node 0/socket 0/controller 0
    // previously produced publication id 1, which could alias an earlier
    // owner-writeback response in the adapter/UBCC replay namespace.
    const uint64_t id = (static_cast<uint64_t>(_nodeId) << 60) |
                        (static_cast<uint64_t>(_socketId) << 59) |
                        (1ULL << 58) |
                        (static_cast<uint64_t>(m_version) << 32) |
                        _nextWriteIdentity++;
    fatal_if(id == 0 || _pendingWrites.count(id),
             "EP_SNF node_id=%d socket=%d: duplicate write id=%lu",
             _nodeId, _socketId, id);
    return id;
}

bool
EPSNFController::recvRequestMsg(const CHIRequestMsg *msg)
{
    // A new same-K request is not evidence that the old observer is dead.
    // Keep the old descriptor until its exact CloseAck consumes queue ownership.
    for (const auto &entry : _retryQueue) {
        if (entry.linePa == msg->m_addr && entry.hnReq == msg->m_requestor &&
            entry.nativeIncarnation != msg->m_ep_native_id &&
            (msg->m_type == CHIRequestType_ReadNoSnp ||
             msg->m_type == CHIRequestType_ReadNoSnpSep))
            return false;
    }
    if ((msg->m_addr & 0xffffffffffULL) == 0x10c21280ULL) {
        DPRINTF(RubyEPVerbose, "[INV147] stage=SNF_REQUEST node=%d localPA=%#lx key=%#lx type=%s proxy=%s txn=%lu requestor=%s socket=%d\n",
                _nodeId, msg->m_addr, msg->m_addr & 0xffffffffffULL,
                msg->m_type, msg->m_ep_proxy_op, msg->m_txnId,
                msg->m_requestor, msg->m_ubcc_ingress_socket);
    }
    DPRINTF(RubyCHIGeneric, "[DEBUG-EPSNF-RECV] EP_SNF node_id=%d recvRequestMsg type=%s addr=0x%lx\n",
            _nodeId, msg->m_type, msg->m_addr);
    DPRINTF(RubyEP, "EP_SNF node_id=%d recvRequestMsg type=%d addr=0x%lx "
            "dataToFwdReq=%d\n", _nodeId, msg->m_type, msg->m_addr,
            msg->m_dataToFwdRequestor);

    // A WriteNoSnp is a two-phase operation. DBIDResp grants the
    // sender permission to transmit NCBWrData; waiting for that data before
    // replying deadlocks the request/data handshake.
    if (msg->m_type == CHIRequestType_WriteNoSnp ||
        msg->m_type == CHIRequestType_WriteNoSnpPtl) {

        // Reserve payload/DBID and both response descriptors before consuming
        // the native request. Data and completion remain independently runnable.
        const size_t writeLimit = msg->m_ubcc_internal_writeback ? 64 : 56;
        if (_pendingWrites.size() >= writeLimit ||
            _pendingResponses.size() + 2 * _pendingWrites.size() >= 128) {
            scheduleEvent(Cycles(1));
            return false;
        }

        if (!_backend) {
            fatal("EP_SNF node_id=%d: no backend for write\n", _nodeId);
        }
        _backend->checkDsmAddr(msg->m_addr);

        PendingWrite pending;
        pending.sourceSocket = msg->m_ubcc_ingress_socket;
        pending.dbid = allocateWriteIdentity();
        pending.storeCommitId = pending.dbid;
        pending.originalTxnId = msg->m_txnId;
        pending.linePa = msg->m_addr;
        fatal_if(pending.sourceSocket < 0 ||
                      !_backend->getUBAdapter(pending.sourceSocket),
                  "EP_SNF node_id=%d: invalid WriteNoSnp requester socket %d",
                  _nodeId, pending.sourceSocket);
        pending.requestor = msg->m_requestor;
        pending.requesterNode = msg->m_ubcc_store_requester;
        pending.permissionEpoch = msg->m_ubcc_permission_epoch;
        pending.disposition = static_cast<UBWriteDisposition>(
            msg->m_ubcc_write_disposition);
        pending.internalPublication = msg->m_ubcc_internal_writeback;
        // The HN's actual InvalidateOnly TBE marks this sub-operation. Capture
        // the control identity once, before any publication retry; ordinary HN
        // maintenance on the same PA must never borrow this authorization.
        if (pending.internalPublication &&
            msg->m_ep_proxy_op == EpProxyOp_InvalidateOnly &&
            !_backend->haEndpointEnabled()) {
            OuterInvalidateMsg parent;
            fatal_if(!_backend->getPendingInvalidation(
                         pending.linePa, pending.sourceSocket, parent),
                     "InvalidateOnly publication has no live control PA=%#lx",
                     pending.linePa);
            pending.parentInvalidateReqId = parent.reqId;
            pending.parentInvalidateEpoch = parent.epoch;
        }
        fatal_if(msg->m_ubcc_store_auth_valid == pending.internalPublication,
                 "EP_SNF node_id=%d: WriteNoSnp must be exactly one of "
                 "authorized StoreCommit or HN internal publication PA=0x%lx",
                 _nodeId, msg->m_addr);
        fatal_if(!msg->m_ubcc_store_auth_valid &&
                     !pending.internalPublication,
                  "EP_SNF node_id=%d socket=%d: native/non-StoreCommit "
                 "WriteNoSnp is unsupported on the DSM endpoint PA=0x%lx; "
                 "HN-F must route only authorized StoreCommit traffic here",
                 _nodeId, pending.sourceSocket, msg->m_addr);
        fatal_if((!pending.internalPublication && pending.requesterNode < 0) ||
                      (pending.internalPublication &&
                       (pending.requesterNode != -1 ||
                        pending.permissionEpoch != 0)) ||
                       (pending.disposition != UBWriteDisposition::MemoryOnly &&
                        !(pending.internalPublication &&
                          pending.disposition == UBWriteDisposition::DropOwner &&
                          msg->m_type == CHIRequestType_WriteNoSnp)),
                 "EP_SNF node_id=%d: invalid StoreCommit authorization "
                 "PA=0x%lx requester=%d disposition=%d", _nodeId,
                 msg->m_addr, pending.requesterNode,
                 static_cast<int>(pending.disposition));
        if (msg->m_type == CHIRequestType_WriteNoSnpPtl) {
            const int offset = msg->m_accAddr - msg->m_addr;
            fatal_if(offset < 0 || msg->m_accSize <= 0 ||
                         offset >= cacheLineSize ||
                         msg->m_accSize > cacheLineSize - offset,
                     "EP_SNF node_id=%d: invalid partial WriteNoSnp range "
                     "PA=0x%lx acc=0x%lx size=%d", _nodeId, msg->m_addr,
                     msg->m_accAddr, msg->m_accSize);
            for (int i = 0; i < msg->m_accSize; ++i)
                pending.expectedMask |= 1ULL << (offset + i);
        } else {
            pending.expectedMask = ~0ULL;
        }

        // The replacement TBE proves local quiescence; capture this node's
        // existing permission incarnation, never infer the owner from Home's
        // current PA lookup. Keep wire-data identity separate from the release
        // snapshot, since NCBWrData must still match the original CHI request.
        if (pending.internalPublication &&
            pending.disposition == UBWriteDisposition::DropOwner &&
            !_backend->haEndpointEnabled()) {
            const auto permission = _backend->inspectRequesterState(pending.linePa);
            if (pending.linePa == 0x14030000)
                DPRINTF(RubyEPVerbose, "[WB-DIAG] stage=SNF_PERMISSION pa=%#lx node=%d socket=%d valid=%d state=%d epoch=%lu\n", pending.linePa, _nodeId, pending.sourceSocket, permission.valid, permission.state, permission.epoch);
            auto &bank = _backend->boundaryTransactions();
            const auto *stable = bank.get(bank.find(pending.linePa));
            if ((stable && stable->custody && !stable->persisted) ||
                (permission.valid && permission.state ==
                 static_cast<int>(RequesterLineState::R_M))) {
                pending.replacementOwnerRelease = true;
                pending.releaseRequester = stable ? stable->owner : _nodeId;
                pending.releaseEpoch = stable ? stable->epoch : permission.epoch;
                pending.boundaryToken = bank.write(pending.linePa,
                    pending.releaseEpoch, pending.releaseRequester,
                    pending.storeCommitId, pending.sourceSocket);
                if (!pending.boundaryToken.valid()) {
                    scheduleEvent(Cycles(1));
                    return false;
                }
            }
        }

        if (_backend->haEndpointEnabled() && !pending.internalPublication) {
            pending.haWrite = true;
            fatal_if(!_backend->resolveHAStoreTarget(
                         msg->m_addr, pending.sourceSocket, pending.homePa,
                         pending.homeNode, pending.homeSocket,
                          pending.permissionEpoch),
                      "EP_SNF node_id=%d: cannot resolve HA Write target PA=0x%lx",
                      _nodeId, msg->m_addr);
            pending.permissionEpoch = msg->m_ubcc_permission_epoch;
        } else {
            int paViewNode = _nodeId;
            if (!_backend->addrMap().isDsm(paViewNode, msg->m_addr))
                paViewNode = _backend->addrMap().srcNodeId(msg->m_addr);
            pending.homeNode = _backend->addrMap().homeNode(paViewNode,
                                                            msg->m_addr);
            pending.homeSocket = _backend->addrMap().homeSocket(paViewNode,
                                                                 msg->m_addr);
            fatal_if(pending.homeNode < 0 || pending.homeSocket < 0,
                     "EP_SNF node_id=%d: cannot route StoreCommit PA=0x%lx",
                     _nodeId, msg->m_addr);
            pending.homePa = _backend->addrMap().buildDsmPA(
                pending.homeNode, pending.homeNode,
                _backend->addrMap().dsmOffset(msg->m_addr),
                pending.homeSocket);
        }
        if (pending.linePa == 0x14030000) {
            DPRINTF(RubyEPVerbose, "[WB-DIAG] stage=SNF_CREATE pa=%#lx homePA=%#lx node=%d socket=%d home=%d homeSocket=%d txn=%lu dbid=%lu store=%lu internal=%d disposition=%d proxy=%d release=%d requester=%d epoch=%lu releaseRequester=%d releaseEpoch=%lu parent=%lu parentEpoch=%lu mask=%#lx\n",
                    pending.linePa, pending.homePa, _nodeId, pending.sourceSocket,
                    pending.homeNode, pending.homeSocket, pending.originalTxnId,
                    pending.dbid, pending.storeCommitId, pending.internalPublication,
                    static_cast<int>(pending.disposition), static_cast<int>(msg->m_ep_proxy_op),
                    pending.replacementOwnerRelease, pending.requesterNode,
                    pending.permissionEpoch, pending.releaseRequester, pending.releaseEpoch,
                    pending.parentInvalidateReqId, pending.parentInvalidateEpoch, pending.expectedMask);
        }
        fatal_if(!_pendingWrites.emplace(pending.dbid, pending).second,
                 "EP_SNF: reserved write descriptor unavailable id=%lu",
                 pending.dbid);

        // ── Phase C4 trace point 2: WriteNoSnp receipt ──
        {
            uint64_t off = msg->m_addr & 0x1FFFULL;
            uint64_t ckOff = msg->m_addr & 0xFFFFFULL;
            if (ckOff < 0x80000ULL && (off % 64 == 0)) {
                DPRINTF(RubyEP,
                    "[C4-ESNF-WNOSNP] node=%d addr=0x%lx off=0x%lx "
                    "expectedMask=0x%lx\n",
                    _nodeId, msg->m_addr, off, pending.expectedMask);
            }
        }

        NetDest hnDest(m_ruby_system);
        hnDest.add(msg->m_requestor);
        auto rsp = std::make_shared<CHIResponseMsg>(
            curTick(), cacheLineSize, m_ruby_system,
            pending.linePa, CHIResponseType_DBIDResp,
            m_machineID, hnDest,
            // CHI-cache routes ordinary responses by addr.  usesTxnId is
            // reserved for the separate DVM TBE table, where txnId itself is
            // used as the wakeup address.  In particular, a replacement TBE's
            // request TxnID is its storage slot (commonly 0xffe/0xfff), not a
            // cache-line address.  Keep the original request identity in the
            // message for diagnostics, but route this response to linePa; the
            // independently allocated DBID remains the NCBWrData identity.
            false, false, pending.originalTxnId, pending.dbid,
            MessageSizeType_Control);
        sendResponseReliable(rsp);
        DPRINTF(RubyEP,
                "[EPSNF-WRITE-DBID] node=%d addr=0x%lx expected=0x%lx\n",
                _nodeId, msg->m_addr, pending.expectedMask);

        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d: WriteNoSnp pending, "
                "addr=0x%lx requestor=0x%x\n",
                _nodeId, msg->m_addr, msg->m_requestor);
        return true;
    }

    // ---- M5 R1: Request type gate ----
    // EP_SNF only services ReadNoSnp (and ReadNoSnpSep) requests.
    // All other request types are rejected.
    if (msg->m_type != CHIRequestType_ReadNoSnp &&
        msg->m_type != CHIRequestType_ReadNoSnpSep) {
        fatal("EP_SNF node_id=%d: received unsupported request type "
              "msg_type=%d for PA=0x%lx\n", _nodeId, msg->m_type, msg->m_addr);
        return false;
    }

    if (!_backend) {
        fatal("EP_SNF node_id=%d: no backend attached\n", _nodeId);
    }

    _backend->checkDsmAddr(msg->m_addr);

    if (msg->m_ep_proxy_op == EpProxyOp_RecallUnique) {
        inform(
                     "[RECALL-PROXY-EPSNF] node=%d localPA=0x%lx type=%d "
                     "proxy=RecallUnique neededPerm=%d writeIntent=%d tick=%lu\n",
                     _nodeId, msg->m_addr, static_cast<int>(msg->m_type),
                     msg->m_ubcc_needed_perm,
                     msg->m_ubcc_write_intent ? 1 : 0, curTick());
    }

    // ---- M5: Read UBCC Sideband Fields ----
    int neededPerm = msg->m_ubcc_needed_perm;  // 0=Shared, 1=Unique
    bool writeIntent = msg->m_ubcc_write_intent;
    int ingressSocket = msg->m_ubcc_ingress_socket;

    DPRINTF(RubyCHIGeneric,
        "[DEBUG-EP-SNF] node=%d type=%d addr=0x%lx neededPerm=%d writeIntent=%d\n",
        _nodeId, (int)msg->m_type, msg->m_addr, neededPerm, (int)writeIntent);

    DPRINTF(RubyCHIGeneric,
            "EP_SNF node_id=%d: sideband neededPerm=%d writeIntent=%d\n",
            _nodeId, neededPerm, writeIntent);

    // Validate: neededPerm must be 0 (Shared) or 1 (Unique)
    if (neededPerm != 0 && neededPerm != 1) {
        fatal("EP_SNF node_id=%d: invalid neededPerm=%d (must be 0 or 1) "
              "PA=0x%lx\n", _nodeId, neededPerm, msg->m_addr);
    }

    // Validate: Shared + true is illegal
    if (neededPerm == 0 && writeIntent) {
        fatal("EP_SNF node_id=%d: illegal sideband Shared+writeIntent=true "
              "PA=0x%lx\n", _nodeId, msg->m_addr);
    }

    // A Home-issued recall already owns the HA transaction. The EP-RNF read
    // below exists only to make the local HN-F snoop/capture the old owner's
    // cache data; recursively requesting HA permission would deadlock Home
    // while it waits for this RecallResp.
    const bool internalRecall =
        _backend->haEndpointEnabled() && _backend->hasActiveRecall(msg->m_addr);

    // Map sideband to outer request and dispatch
    // Bound demand descriptors before issuing any outer side effect. Response
    // and data channels drain independently of this admission decision.
    if (_retryQueue.size() >= 64 ||
        _pendingData.size() + _deferredCompData.size() +
            dataMsgsPerLine * (_retryQueue.size() + 1) > 128 ||
        _pendingResponses.size() + 2 * _pendingWrites.size() >= 127) {
        scheduleEvent(Cycles(1));
        return false;
    }
    int homeNode = -1;
    if (msg->m_ep_proxy_op == EpProxyOp_RecallUnique) {
        inform(
                     "[RECALL-PROXY-OUTER-ENTER] node=%d localPA=0x%lx "
                     "tick=%lu\n",
                     _nodeId, msg->m_addr, curTick());
    }
    BoundaryBorrowToken authorityBorrow;
    BoundaryTransactions::Token foreground;
    if (!internalRecall) {
        foreground = _backend->boundaryTransactions().foreground(
            msg->m_addr, 0, _nodeId, allocateWriteIdentity(), _socketId);
        if (!foreground.valid()) {
            scheduleEvent(Cycles(1));
            return false;
        }
    }
    const bool admitted = internalRecall ||
        _backend->holdAuthority(msg->m_addr, authorityBorrow);
    int grantResult = !admitted ? -2 : internalRecall
        ? static_cast<int>(OuterGrantType::GlobalGrantShared)
        : _backend->handleRemoteDemandMiss(
              msg->m_addr, neededPerm, writeIntent, ingressSocket, homeNode);

    // Q3: If grant blocked, queue for retry instead of sending stale data
    if (grantResult < 0) {
        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d: grant BUSY for PA=0x%lx, queuing retry\n",
                _nodeId, msg->m_addr);
        EPSNFController::RetryEntry entry;
        entry.foreground = foreground;
        entry.linePa = msg->m_addr;
        entry.neededPerm = neededPerm;
        entry.writeIntent = writeIntent;
        entry.ingressSocket = ingressSocket;
        entry.hnReq = msg->m_requestor;
        entry.fwdReq = msg->m_fwdRequestor;
        entry.dataToFwdReq = msg->m_dataToFwdRequestor;
        entry.publishOnData = msg->m_ubcc_publish_on_data;
        entry.needsReadReceipt = msg->m_type == CHIRequestType_ReadNoSnpSep;
        entry.nativeTxnId = msg->m_txnId;
        entry.nativeIncarnation = msg->m_ep_native_id;
        entry.authorityBorrow = authorityBorrow;
        fatal_if(!_retryQueue.push_back(entry),
                 "EP_SNF: reserved read descriptor unavailable");
        if (params().park_microtest && !params().park_microtest_partial &&
            !_parkMicrotestIssued) {
            _parkMicrotestIssued = true;
            const Addr line = entry.linePa;
            const uint64_t control = allocateWriteIdentity();
            fatal_if(!parkAcquisition(line, control, [this, line, control] {
                inform("[PARK-MICRO] stage=PARKED node=%d line=%#lx control=%lu",
                       _nodeId, line, control);
                auto *rnf = _backend->getEpRnfController();
                fatal_if(!rnf, "Park microtest requires real EP-RNF");
                rnf->startCleanUnique(line, [this, line, control](bool ok) {
                    fatal_if(!ok, "Park microtest native invalidation failed");
                    inform("[PARK-MICRO] stage=LOCAL_CONTROL_DONE node=%d line=%#lx control=%lu",
                           _nodeId, line, control);
                    resumeAcquisition(line, control);
                }, _socketId);
            }), "Park microtest failed admission");
        }
        scheduleEvent(Cycles(epsnf_retry_cycles()));
        return true;
    }

    // ---- Record sideband for inspection ----
    if (!internalRecall) {
        _backend->recordSideband(msg->m_addr, neededPerm, writeIntent,
                                 (neededPerm == 0) ? 0 : 1,
                                 grantResult, homeNode);
    }

    // ---- Q2 FIX: DMT-aware routing ----
    // If dataToFwdRequestor is set, the HN-F expects CompData to go
    // directly to the fwdRequestor (original L2) and only a response
    // (ReadReceipt for ReadNoSnpSep, nothing for plain ReadNoSnp) to
    // come back to the HN-F.
    NetDest hnDest(m_ruby_system);
    hnDest.add(msg->m_requestor);

    NetDest dataDest(m_ruby_system);
    if (msg->m_dataToFwdRequestor) {
        dataDest.add(msg->m_fwdRequestor);
    } else {
        dataDest.add(msg->m_requestor);
    }

    DataBlock grantData(cacheLineSize);
    GrantDataSource grantSource = GrantDataSource::NoData;
    int grantDataState = internalRecall ? 0 : _backend->takeGrantData(
        msg->m_addr, grantData, grantSource);
    fatal_if(grantDataState < 0,
             "EP_SNF node_id=%d: missing grant data state PA=0x%lx",
             _nodeId, msg->m_addr);
    if (grantDataState > 0) {
        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d: CompData populated with grant data "
                "first_byte=0x%02x\n", _nodeId, grantData.getByte(0));
    } else {
        // F3: Data not ready — defer/retry instead of silent zero-fill.
        // Only NoData (explicit zero-fill) is allowed through without data.
        if (grantSource != GrantDataSource::NoData) {
            DPRINTF(RubyCHIGeneric,
                    "EP_SNF node_id=%d: grant data not ready (dataSource=%d), "
                    "deferring to retry queue\n",
                    _nodeId, static_cast<int>(grantSource));
            EPSNFController::RetryEntry entry;
            entry.foreground = foreground;
            entry.linePa = msg->m_addr;
            entry.neededPerm = neededPerm;
            entry.writeIntent = writeIntent;
            entry.ingressSocket = ingressSocket;
            entry.hnReq = msg->m_requestor;
            entry.fwdReq = msg->m_fwdRequestor;
            entry.dataToFwdReq = msg->m_dataToFwdRequestor;
            entry.publishOnData = msg->m_ubcc_publish_on_data;
            entry.needsReadReceipt = msg->m_type == CHIRequestType_ReadNoSnpSep;
            entry.nativeTxnId = msg->m_txnId;
            entry.nativeIncarnation = msg->m_ep_native_id;
            entry.authorityBorrow = authorityBorrow;
            fatal_if(!_retryQueue.push_back(entry),
                     "EP_SNF: reserved read descriptor unavailable");
            scheduleEvent(Cycles(epsnf_retry_cycles()));
            return true;
        }
        // NoData: zero-fill is the correct behavior
        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d: CompData fallback to zeros "
                "(NoData source)\n", _nodeId);
    }

    // Do not release the HN routing context while data still depends on Home.
    // The retry descriptor retains this obligation until the complete line is
    // available; both immediate and deferred paths send it exactly once.
    if (msg->m_type == CHIRequestType_ReadNoSnpSep) {
        auto rsp = std::make_shared<CHIResponseMsg>(
            curTick(), cacheLineSize, m_ruby_system,
            msg->m_addr, CHIResponseType_ReadReceipt,
            m_machineID, hnDest, false, false, msg->m_txnId, 0,
            MessageSizeType_Control);
        sendResponseReliable(rsp);
    }

    // ---- v4: shared_hint + CompData type for shared grants (§4.4.2, §5.1) ----
    // Shared grant (neededPerm==0): CompData_SC with m_m_shared_hint=true
    // Unique grant (neededPerm==1): CompData_UC (baseline unique fill)
    CHIDataType dataType = (neededPerm == 0) ? CHIDataType_CompData_SC
                                             : CHIDataType_CompData_UC;
    bool sharedHint = (neededPerm == 0);  // v4: always register EP-RNF for shared
    int storeRequester = -1;
    uint64_t storeEpoch = 0;
    if (!internalRecall) {
        fatal_if(!_backend->getStoreAuthorization(
                     msg->m_addr, ingressSocket, storeRequester, storeEpoch),
                 "EP_SNF node_id=%d: missing grant authorization PA=0x%lx",
                 _nodeId, msg->m_addr);
    }

    // ---- Q2 FIX: Splitting CompData into data-channel-sized chunks ----
    // The L2/HN-F's ExpectedMap counts data responses in CHUNKS
    // (blockSize / data_channel_size).  Generalize the chunk loop
    // using dataChannelSize (bytes per data message) and
    // dataMsgsPerLine (number of data messages per cache line).
    // P1-1: Replace hardcoded halfSize = cacheLineSize/2 with
    // dataChannelSize and dataMsgsPerLine from EPController params.
    //
    // With dataChannelSize=32 and cacheLineSize=64, dataMsgsPerLine=2.
    // With dataChannelSize=64 and cacheLineSize=64, dataMsgsPerLine=1.
    for (int i = 0; i < dataMsgsPerLine; i++) {
        int offset = i * dataChannelSize;
        int chunkSize = (i == dataMsgsPerLine - 1) ?
            (cacheLineSize - offset) : dataChannelSize;

        WriteMask wm(cacheLineSize);
        wm.setMask(offset, chunkSize);

        DataBlock db(cacheLineSize);
        // P0-1: Guard against nullptr to avoid memcpy crash.
        // P0-2: Use correct destination offset (offset, not 0).
        if (grantDataState > 0) {
            const uint8_t *chunk = grantData.getData(offset, chunkSize);
            db.setData(chunk, offset, chunkSize);
        }
        // else db keeps default zeros

        auto dat = std::make_shared<CHIDataMsg>(
            curTick(), cacheLineSize, m_ruby_system);
        dat->m_addr = msg->m_addr;
        dat->m_ep_native_id = msg->m_dataToFwdRequestor ? 0 : msg->m_ep_native_id;
        dat->m_type = dataType;
        dat->m_responder = m_machineID;
        dat->m_Destination = dataDest;
        dat->m_dataBlk = db;
        dat->m_bitMask = wm;
        dat->m_MessageSize = MessageSizeType_Data;
        // v4: Set shared_hint on CompData for shared grants
        dat->m_m_shared_hint = sharedHint;
        dat->m_ubcc_store_auth_valid = !internalRecall;
        dat->m_ubcc_store_requester = storeRequester;
        dat->m_ubcc_permission_epoch = storeEpoch;
        // Q3: Defer send by 1 tick to prevent same-tick TBE race
        // at HN-F (see docs/tbe-race-condition.svg for details).
        PendingDataOutput pending;
        pending.msg = dat;
        if (i == dataMsgsPerLine - 1 && !internalRecall &&
            (_backend->haEndpointEnabled() || msg->m_ubcc_publish_on_data)) {
            pending.onSent = [this, linePa = msg->m_addr, neededPerm,
                              writeIntent,
                              publishOnData = msg->m_ubcc_publish_on_data] {
                // HA completion waits for HN coherent publication.
                if (publishOnData) {
                    _backend->notifyLocalLinePublished(linePa, _socketId);
                }
            };
        }
        _deferredCompData.push_back(std::move(pending));
    }

    if (!internalRecall) {
        RetryEntry retained{};
        retained.linePa = msg->m_addr;
        retained.neededPerm = neededPerm;
        retained.writeIntent = writeIntent;
        retained.ingressSocket = ingressSocket;
        retained.hnReq = msg->m_requestor;
        retained.fwdReq = msg->m_fwdRequestor;
        retained.dataToFwdReq = msg->m_dataToFwdRequestor;
        retained.publishOnData = msg->m_ubcc_publish_on_data;
        retained.needsReadReceipt = false;
        retained.nativeTxnId = msg->m_txnId;
        retained.nativeIncarnation = msg->m_ep_native_id;
        retained.authorityBorrow = authorityBorrow;
        retained.foreground = foreground;
        retained.nativeDataPublished = true;
        fatal_if(!_retryQueue.push_back(retained),
                 "EPSNF lost reserved native completion descriptor");
    }

    // Schedule deferred sends.
    // NOTE: This is the *normal success* path — the grant already succeeded
    // and the data is ready; we only defer by 1 tick to avoid the same-tick
    // TBE race at the HN-F (see comment above / docs/tbe-race-condition.svg).
    // Do NOT use epsnf_retry_cycles() here: that value is a *back-off* interval
    // meant only for genuine BUSY / not-ready retries (a large back-off avoids
    // a request storm while the request is still traversing the remote node).
    // Using it here needlessly delayed every CompData by ~10µs.
    if (!_deferredCompData.empty()) {
        // 1-tick TBE-race defer + optional cross-socket NoC Δ (default 0).
        scheduleEvent(Cycles(1 + epsnf_delta_noc_cycles()));
    }

    return true;
}

void
EPSNFController::processDeferredData()
{
    for (auto &pending : _deferredCompData) {
        sendDataReliable(pending.msg, std::move(pending.onSent));
    }
    _deferredCompData.clear();
}

void
EPSNFController::sendResponseReliable(std::shared_ptr<CHIResponseMsg> msg,
                                      std::function<void()> onSent)
{
    if (_pendingResponses.empty() && sendResponseMsg(msg)) {
        if (onSent)
            onSent();
        return;
    }

    _pendingResponses.push_back({std::move(msg), std::move(onSent)});
    scheduleEvent(Cycles(1));
}

void
EPSNFController::sendDataReliable(std::shared_ptr<CHIDataMsg> msg,
                                  std::function<void()> onSent)
{
    if (_pendingData.empty() && sendDataMsg(msg)) {
        if (onSent)
            onSent();
        return;
    }

    PendingDataOutput pending;
    pending.msg = std::move(msg);
    pending.onSent = std::move(onSent);
    _pendingData.push_back(std::move(pending));
    scheduleEvent(Cycles(1));
}

void
EPSNFController::processPendingOutputs()
{
    while (!_pendingResponses.empty()) {
        auto &pending = _pendingResponses.front();
        if (!sendResponseMsg(pending.msg))
            break;
        auto onSent = std::move(pending.onSent);
        _pendingResponses.pop_front();
        if (onSent)
            onSent();
    }

    while (!_pendingData.empty()) {
        auto &pending = _pendingData.front();
        if (!sendDataMsg(pending.msg))
            break;
        auto onSent = std::move(pending.onSent);
        _pendingData.pop_front();
        if (onSent)
            onSent();
    }

    if (!_pendingResponses.empty() || !_pendingData.empty())
        scheduleEvent(Cycles(1));
}

// ---- v4: Deferred Grant Processing (§4.4.2, §7.6) ----
void
EPSNFController::processDeferredGrants()
{
    while (!_deferredGrants.empty()) {
        DeferredGrantEntry &entry = _deferredGrants.front();

        // Determine CompData type from grant type and sharedHint
        CHIDataType dataType;
        if (entry.sharedHint) {
            dataType = CHIDataType_CompData_SC;   // §4.4.2 item 4: shared grant
        } else {
            switch (entry.grantType) {
                case OuterGrantType::GlobalGrantShared:
                    dataType = CHIDataType_CompData_SC;
                    break;
                case OuterGrantType::GlobalGrantExclusive:
                    dataType = CHIDataType_CompData_UC;
                    break;
                case OuterGrantType::GlobalGrantModified:
                    dataType = CHIDataType_CompData_UD_PD;
                    break;
                default:
                    dataType = CHIDataType_CompData_UC;
                    break;
            }
        }

        // Send data chunks
        for (int i = 0; i < dataMsgsPerLine; i++) {
            int offset = i * dataChannelSize;
            int chunkSize = (i == dataMsgsPerLine - 1) ?
                (cacheLineSize - offset) : dataChannelSize;

            WriteMask wm(cacheLineSize);
            wm.setMask(offset, chunkSize);

            DataBlock db(cacheLineSize);
            // Copy grant data into chunk
            const uint8_t *src = entry.data.getData(offset, chunkSize);
            if (src) {
                db.setData(src, offset, chunkSize);
            }

            auto dat = std::make_shared<CHIDataMsg>(
                curTick(), cacheLineSize, m_ruby_system);
            dat->m_addr = entry.linePa;
            dat->m_type = dataType;
            dat->m_responder = m_machineID;
            dat->m_dataBlk = db;
            dat->m_bitMask = wm;
            dat->m_MessageSize = MessageSizeType_Data;
            // v4: Set shared_hint on CompData for shared grants
            dat->m_m_shared_hint = entry.sharedHint;

            // Defer by 1 tick for timing invariant (§4.4.2 item 2, I10)
            PendingDataOutput pending;
            pending.msg = dat;
            _deferredCompData.push_back(std::move(pending));
        }

        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d: deferred grant sent for PA=0x%lx "
                "epoch=%lu reqId=%lu\n",
                _nodeId, entry.linePa, entry.epoch, entry.reqId);

        _deferredGrants.erase(_deferredGrants.begin());
    }
}

bool
EPSNFController::recvSnoopMsg(const CHIRequestMsg *msg)
{
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d recvSnoopMsg\n", _nodeId);
    if (_backend)
        _backend->checkDsmAddr(msg->m_addr);
    return true;
}

bool
EPSNFController::recvResponseMsg(const CHIResponseMsg *msg)
{
    if (msg->m_type == CHIResponseType_AcquireCloseAck) {
        for (auto it = _retryQueue.begin(); it != _retryQueue.end(); ++it) {
            if (it->linePa != msg->m_addr ||
                it->nativeIncarnation != msg->m_txnId ||
                it->hnReq != msg->m_responder ||
                it->parkControl != msg->m_dbid || !it->closeSent)
                continue;
            rememberRetiredControl(*it);
            if (const auto *owner = _backend->boundaryTransactions().get(it->foreground)) {
                const auto id = owner->foregroundId;
                const auto socket = owner->foregroundSocket;
                if (_backend->haEndpointEnabled())
                    _backend->boundaryTransactions().finishForeground(it->foreground,
                        id, socket, BoundaryTransactions::HomeCommit);
                _backend->boundaryTransactions().finishForeground(it->foreground,
                    id, socket, BoundaryTransactions::NativeClose);
            }
            _backend->closeAuthority(it->authorityBorrow);
            _retryQueue.erase(it);
            if (_microLatePark && ++_microOtherClosed == 257) {
                inform("[CLOSE-MICRO] other_closed=257 old_observer_live=1");
                auto late = std::move(_microLatePark);
                late();
            }
            DPRINTF(RubyEP, "[EP-ACQUIRE-RETIRED] node=%d socket=%d addr=%#lx incarnation=%lu occupancy=%u closeAck=1\n",
                    _nodeId, _socketId, msg->m_addr, msg->m_txnId,
                    static_cast<unsigned>(_retryQueue.size()));
            scheduleEvent(Cycles(1));
            return true;
        }
        return true; // Duplicate ACK cannot retire a new incarnation.
    }
    if (msg->m_type == CHIResponseType_AcquireParkAck ||
        msg->m_type == CHIResponseType_AcquireResumeAck) {
        for (auto &entry : _retryQueue) {
            if (entry.linePa != msg->m_addr ||
                entry.nativeIncarnation != msg->m_txnId ||
                entry.hnReq != msg->m_responder ||
                entry.parkControl != msg->m_dbid)
                continue;
            if (msg->m_type == CHIResponseType_AcquireParkAck) {
                // An exact duplicate must not call the parent barrier twice.
                if (entry.parkPhase != 1)
                    return true;
                entry.parkPhase = 2;
                auto callback = std::move(entry.onParked);
                if (callback) callback();
            } else {
                if (entry.parkPhase != 3)
                    return true;
                entry.parkPhase = 0;
                if (params().park_microtest || params().park_microtest_partial) {
                    inform("[PARK-MICRO] stage=RESUMED node=%d line=%#lx control=%lu",
                           _nodeId, entry.linePa, entry.parkControl);
                }
                scheduleEvent(Cycles(1));
                if (entry.nativeDone) {
                    // The normal completion may have arrived while this
                    // handshake owned the descriptor. Settle each obligation
                    // independently, then retire through the same identity API.
                    const auto line = entry.linePa;
                    const auto incarnation = entry.nativeIncarnation;
                    nativeAcquireDone(line, incarnation);
                }
            }
            return true;
        }
        for (const auto &retired : _retiredControls) {
            if (retired.line == msg->m_addr &&
                retired.incarnation == msg->m_txnId &&
                retired.control == msg->m_dbid &&
                retired.responder == msg->m_responder)
                return true;
        }
        // ACKs have no response obligation. Unknown generations may never
        // consume a current descriptor or invoke its role callback.
        DPRINTF(RubyEP, "[EP-PARK-STALE-ACK] addr=%#lx incarnation=%lu control=%lu\n",
                msg->m_addr, msg->m_txnId, msg->m_dbid);
        return true;
    }
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d recvResponseMsg\n", _nodeId);
    return true;
}

bool
EPSNFController::canParkAcquisitions(Addr line, uint64_t control) const
{
    if (!control) return false;
    for (const auto &entry : _retryQueue) {
        if (entry.linePa == line &&
            (!entry.nativeIncarnation || entry.parkPhase != 0 || entry.closeSent))
            return false;
    }
    return true;
}

unsigned
EPSNFController::acquisitionCount(Addr line) const
{
    unsigned count = 0;
    for (const auto &entry : _retryQueue)
        if (entry.linePa == line) ++count;
    return count;
}

bool
EPSNFController::parkAcquisition(Addr line, uint64_t control,
                                  std::function<void()> onParked)
{
    const unsigned count = acquisitionCount(line);
    if (!count || !canParkAcquisitions(line, control)) return false;
    // Bounded by the fixed descriptor bank. Install the whole barrier before
    // sending, so even immediate response delivery cannot finish early.
    auto remaining = std::make_shared<unsigned>(count);
    auto complete = std::make_shared<std::function<void()>>(std::move(onParked));
    for (auto &entry : _retryQueue) {
        if (entry.linePa != line) continue;
        fatal_if(!control || !entry.nativeIncarnation || entry.parkPhase,
                 "Invalid acquisition park admission addr=%#lx", line);
        entry.parkControl = control;
        entry.parkPhase = 1;
        entry.onParked = [remaining, complete] {
            assert(*remaining > 0);
            if (--*remaining == 0) {
                auto callback = std::move(*complete);
                if (callback) callback();
            }
        };
        DPRINTF(RubyEP, "[EP-PARK-TX] node=%d socket=%d addr=%#lx incarnation=%lu control=%lu\n",
                _nodeId, _socketId, line, entry.nativeIncarnation, control);
        NetDest destination(m_ruby_system);
        destination.add(entry.hnReq);
        auto msg = std::make_shared<CHIResponseMsg>(
            curTick(), cacheLineSize, m_ruby_system, line,
            CHIResponseType_AcquirePark, m_machineID, destination,
            false, false, entry.nativeIncarnation, control,
            MessageSizeType_Control);
        sendResponseReliable(msg);
    }
    return true;
}

void
EPSNFController::resumeAcquisition(Addr line, uint64_t control)
{
    bool found = false;
    for (auto &entry : _retryQueue) {
        if (entry.linePa != line || entry.parkControl != control) continue;
        found = true;
        if (entry.parkPhase == 3 || entry.parkPhase == 0) continue;
        fatal_if(entry.parkPhase != 2, "Resume before local control completion");
        entry.parkPhase = 3;
        NetDest destination(m_ruby_system);
        destination.add(entry.hnReq);
        auto msg = std::make_shared<CHIResponseMsg>(
            curTick(), cacheLineSize, m_ruby_system, line,
            CHIResponseType_AcquireResume, m_machineID, destination,
            false, false, entry.nativeIncarnation, control,
            MessageSizeType_Control);
        sendResponseReliable(msg);
    }
    // A repeated local completion is harmless after all branches retired.
    if (!found) {
        DPRINTF(RubyEP, "[EP-PARK-STALE-RESUME] addr=%#lx control=%lu\n",
                line, control);
    }
}

void
EPSNFController::rememberRetiredControl(const RetryEntry &entry)
{
    if (!entry.parkControl) return;
    auto &retired = _retiredControls[_retiredControlCursor];
    retired.line = entry.linePa;
    retired.incarnation = entry.nativeIncarnation;
    retired.control = entry.parkControl;
    retired.responder = entry.hnReq;
    _retiredControlCursor = (_retiredControlCursor + 1) % _retiredControls.size();
}

void
EPSNFController::nativeAcquireDone(Addr line, uint64_t incarnation)
{
    for (auto it = _retryQueue.begin(); it != _retryQueue.end(); ++it) {
        if (it->linePa != line || it->nativeIncarnation != incarnation)
            continue;
        fatal_if(!it->nativeDataPublished,
                 "Native acquire retired before publication addr=%#lx id=%lu",
                 line, incarnation);
        it->nativeDone = true;
        // Both existing opt-in flags select the late-retirement fixture. The
        // descriptor already owns the real data and incarnation; only its local
        // retirement callback is delayed. No permission/data is fabricated.
        if (params().park_microtest && params().park_microtest_partial &&
            !_parkMicrotestIssued) {
            _parkMicrotestIssued = true;
            const uint64_t control = allocateWriteIdentity();
            auto late = [this, line, control] {
                inform("[PARK-MICRO] stage=LATE_NATIVE_DONE node=%d line=%#lx control=%lu",
                       _nodeId, line, control);
                fatal_if(!parkAcquisition(line, control, [this, line, control] {
                    inform("[PARK-MICRO] stage=PARKED node=%d line=%#lx control=%lu",
                           _nodeId, line, control);
                    _backend->getEpRnfController()->startCleanUnique(
                        line, [this, line, control](bool ok) {
                            fatal_if(!ok, "Late Park native control failed");
                            inform("[PARK-MICRO] stage=LOCAL_CONTROL_DONE node=%d line=%#lx control=%lu",
                                   _nodeId, line, control);
                            resumeAcquisition(line, control);
                        }, _socketId);
                }), "Late Park fixture lost its retained descriptor");
            };
            if (params().park_microtest_wrap) {
                it->microHeld = true;
                _microLatePark = std::move(late);
            } else {
                schedule(new EventFunctionWrapper(std::move(late),
                    name() + ".latePark", true), clockEdge(Cycles(8)));
            }
            return;
        }
        if (it->parkPhase != 0 || it->closeSent)
            return; // An in-flight control identity is still a live obligation.
        it->closeSent = true;
        NetDest destination(m_ruby_system);
        destination.add(it->hnReq);
        auto close = std::make_shared<CHIResponseMsg>(
            curTick(), cacheLineSize, m_ruby_system, line,
            CHIResponseType_AcquireClose, m_machineID, destination,
            false, false, incarnation, it->parkControl, MessageSizeType_Control);
        auto duplicate = params().park_microtest_wrap
            ? std::make_shared<CHIResponseMsg>(*close) : nullptr;
        sendResponseReliable(close);
        if (params().park_microtest_wrap) {
            // Deliberately duplicate actual Close and exercise duplicate ACK
            // consumption; each message owns a separate immutable wire copy.
            sendResponseReliable(duplicate);
        }
        scheduleEvent(Cycles(1));
        return;
    }
}

bool
EPSNFController::recvDataMsg(const CHIDataMsg *msg)
{
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d recvDataMsg type=%d addr=0x%lx\n",
            _nodeId, msg->m_type, msg->m_addr);
    const uint64_t dbid = msg->m_txnId;
    auto pendingIt = _pendingWrites.find(dbid);
    DPRINTF(RubyEP,
                 "[EPSNF-DATA-RECV] node=%d type=%d addr=0x%lx pendingWrite=%d\n",
                 _nodeId, msg->m_type, msg->m_addr,
                 pendingIt != _pendingWrites.end() ? 1 : 0);

    if (msg->m_type == CHIDataType_NCBWrData) {
        fatal_if(dbid == 0 || pendingIt == _pendingWrites.end(),
                 "EP_SNF node_id=%d: unmatched NCBWrData PA=0x%lx id=%lu",
                 _nodeId, msg->m_addr, dbid);
        PendingWrite &pending = pendingIt->second;
        fatal_if(!msg->m_usesTxnId,
                 "EP_SNF node_id=%d: NCBWrData lacks DBID transaction marker "
                 "PA=0x%lx dbid=%lu", _nodeId, msg->m_addr, dbid);
        fatal_if(msg->m_addr != pending.linePa ||
                      msg->m_ubcc_store_auth_valid ==
                          pending.internalPublication ||
                      msg->m_ubcc_internal_writeback !=
                          pending.internalPublication ||
                      msg->m_ubcc_store_requester != pending.requesterNode ||
                      msg->m_ubcc_permission_epoch != pending.permissionEpoch ||
                     msg->m_ubcc_write_disposition !=
                         static_cast<int>(pending.disposition),
                 "EP_SNF node_id=%d: StoreCommit data identity mismatch id=%lu",
                 _nodeId, dbid);
        uint64_t beatMask = 0;
        for (int i = 0; i < cacheLineSize; ++i) {
            if (msg->m_bitMask.test(i)) {
                fatal_if(!(pending.expectedMask & (1ULL << i)),
                         "EP_SNF node_id=%d: StoreCommit data outside mask id=%lu byte=%d",
                          _nodeId, dbid, i);
                pending.data[i] = msg->m_dataBlk.getByte(i);
                beatMask |= 1ULL << i;
            }
        }
        pending.receivedMask |= beatMask;
        pending.dataComplete = pending.receivedMask == pending.expectedMask;
        if (pending.dataComplete)
            scheduleEvent(Cycles(1));
        return true;
    }

    if (msg->m_type == CHIDataType_CBWrData_UC ||
        msg->m_type == CHIDataType_CBWrData_UD_PD ||
        msg->m_type == CHIDataType_CBWrData_SC ||
        msg->m_type == CHIDataType_CBWrData_SD_PD ||
        msg->m_type == CHIDataType_CBWrData_I) {
        // CHI-cache-transitions.sm proves the topology boundary: RN CBWrData
        // terminates at HN-F; an HN-F forwarding persistence downstream emits
        // NCBWrData. Seeing CB here therefore indicates a routing/configuration
        // violation, not an owner transaction from which identity may be inferred.
        fatal("EP_SNF node_id=%d: illegal CBWrData crossed the HN-F boundary "
              "PA=0x%lx type=%d; refusing PA/QLM identity inference\n",
              _nodeId, msg->m_addr, msg->m_type);
    }

    return true;
}

void
EPSNFController::processPendingHAWrites()
{
    if (!_backend)
        return;

    bool pendingWork = false;
    for (auto it = _pendingWrites.begin(); it != _pendingWrites.end(); ) {
        auto current = it++;
        const uint64_t transactionId = current->first;
        PendingWrite &pending = current->second;
        if (!pending.dataComplete || pending.completionQueued) {
            pendingWork |= !pending.completionQueued;
            continue;
        }

        if (!pending.granted) {
            int result = -1;
            UBHAPermissionRespBody response;
            if (pending.haWrite) {
                result = _backend->requestHAPermission(
                    pending.homePa, HAOperation::Write,
                    pending.permissionEpoch, pending.expectedMask, pending.data,
                    pending.homeNode, pending.homeSocket,
                    pending.permissionReqId, response, pending.sourceSocket);
            } else {
                if (pending.replacementOwnerRelease) {
                    pending.permissionReqId = pending.storeCommitId;
                    result = _backend->handleWritebackWithMeta(
                        pending.linePa, false, pending.data,
                        pending.releaseEpoch, pending.releaseRequester,
                        pending.sourceSocket, &pending.permissionReqId);
                } else if (pending.internalPublication) {
                    result = _backend->publishInternalWriteback(
                        pending.homePa, pending.storeCommitId,
                        pending.expectedMask, pending.data, pending.homeNode,
                        pending.homeSocket, pending.sourceSocket,
                        pending.permissionReqId,
                        pending.parentInvalidateReqId,
                        pending.parentInvalidateEpoch);
                } else {
                    result = _backend->commitStore(
                        pending.homePa, pending.requesterNode,
                        pending.permissionEpoch, pending.storeCommitId,
                        pending.expectedMask, pending.data, pending.homeNode,
                        pending.homeSocket, pending.sourceSocket,
                        pending.permissionReqId);
                }
            }
            if (pending.linePa == 0x14030000 && result != -2)
                DPRINTF(RubyEPVerbose, "[WB-DIAG] stage=SNF_PERSIST_RESULT pa=%#lx node=%d socket=%d store=%lu req=%lu internal=%d release=%d parent=%lu result=%d\n", pending.linePa, _nodeId, pending.sourceSocket, pending.storeCommitId, pending.permissionReqId, pending.internalPublication, pending.replacementOwnerRelease, pending.parentInvalidateReqId, result);
            if (result == -2) {
                pendingWork = true;
                continue;
            }
            // Internal replacement publications carry no architectural owner
            // authorization.  UBCC may reject one transiently while an outer
            // request for the same line is still active; no data was persisted
            // in that case, so retry the same stable publication identity.
            // StoreCommit rejection remains fatal and exact-identity checked.
            if (result == 0 && pending.internalPublication &&
                !pending.replacementOwnerRelease &&
                pending.parentInvalidateReqId == 0) {
                pendingWork = true;
                continue;
            }
            fatal_if(result != 1,
                     "EP_SNF node_id=%d: store persistence failed "
                     "PA=0x%lx id=%lu result=%d",
                     _nodeId, pending.linePa, transactionId, result);

            if (pending.haWrite) {
                fatal_if(response.operation != HAOperation::Write ||
                             response.permissionEpoch != pending.permissionEpoch,
                         "EP_SNF node_id=%d: HA Write response mismatch id=%lu",
                         _nodeId, transactionId);
                if (response.status == HAStatus::RetryableBusy) {
                    pending.permissionReqId = 0;
                    pendingWork = true;
                    continue;
                }
                fatal_if(response.status != HAStatus::Ok || !response.hasData,
                         "EP_SNF node_id=%d: HA Write lacks successful final64 id=%lu",
                         _nodeId, transactionId);
                std::memcpy(pending.data, response.data, 64);
            }
            pending.granted = true;
        }

        publishHAWrite(transactionId, pending);
    }

    if (pendingWork)
        scheduleEvent(Cycles(epsnf_retry_cycles()));
}

void
EPSNFController::publishHAWrite(uint64_t transactionId, PendingWrite &pending)
{
    fatal_if(!pending.granted || pending.permissionReqId == 0,
             "EP_SNF node_id=%d: publish before persistence PA=0x%lx id=%lu",
             _nodeId, pending.linePa, transactionId);
    pending.completionQueued = true;

    ++_haWritePublishCount;
    if (_haWritePublishCount <= kHAWriteTraceLimit) {
        inform("[HA-WRITE-PUBLISH] node=%d pa=0x%lx homePa=0x%lx "
               "reqId=%lu publishCount=%lu tick=%lu\n", _nodeId,
               pending.linePa,
               pending.homePa, pending.permissionReqId,
               _haWritePublishCount, curTick());
    }

    NetDest destination(m_ruby_system);
    destination.add(pending.requestor);
    auto completion = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system, pending.linePa,
        CHIResponseType_Comp, m_machineID, destination,
        // Match the DBIDResp routing contract above: delayed Comp wakes the
        // same active cache/replacement TBE by line address, never the DVM TBE
        // namespace keyed by the original request TxnID/storage slot.
        false, false, pending.originalTxnId, 0, MessageSizeType_Control);
    sendResponseReliable(completion, [this, transactionId] {
        auto it = _pendingWrites.find(transactionId);
        fatal_if(it == _pendingWrites.end() || !it->second.completionQueued,
                 "EP_SNF node_id=%d: missing store completion id=%lu",
                 _nodeId, transactionId);
        PendingWrite &done = it->second;
        if (done.boundaryToken.valid()) {
            fatal_if(!_backend->boundaryTransactions().finishWrite(
                         done.boundaryToken, done.storeCommitId, done.sourceSocket),
                     "EPSNF stale boundary write completion id=%lu", transactionId);
        }
        if (done.haWrite) {
            fatal_if(!_backend->acknowledgeHAPermission(
                         done.homePa, HAOperation::Write, HAStatus::Ok,
                         done.permissionEpoch, done.homeNode, done.homeSocket,
                         done.permissionReqId, done.sourceSocket),
                     "EP_SNF node_id=%d: failed to queue HA Write ack id=%lu",
                     _nodeId, transactionId);
        }
        ++_haWriteAckCount;
        if (_haWriteAckCount <= kHAWriteTraceLimit) {
            inform("[HA-WRITE-ACK] node=%d pa=0x%lx reqId=%lu "
                   "reqCount=%lu respCount=%lu ackCount=%lu tick=%lu\n",
                    _nodeId, done.linePa, done.permissionReqId, _haWriteReqCount,
                   _haWriteRespCount, _haWriteAckCount, curTick());
        }
        _pendingWrites.erase(it);
    });
}

// ---- v4: write-data completion path with dedup (§5.5) ----
// (called from recvDataMsg when a WriteNoSnp beat completes)

} // namespace ruby
} // namespace gem5
