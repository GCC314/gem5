#include "mem/ruby/protocol/chi/ep/EPSNFController.hh"

#include <cstring>

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "debug/RubyEP.hh"
#include "mem/ruby/common/DataBlock.hh"
#include "mem/ruby/protocol/CHI/CHIDataMsg.hh"
#include "mem/ruby/protocol/CHI/CHIRequestMsg.hh"
#include "mem/ruby/protocol/CHI/CHIResponseMsg.hh"
#include "mem/ruby/protocol/chi/ep/UBAdapter.hh"
#include "params/EPSNFController.hh"

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
            int homeNode = -1;
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
                    dat->m_type = dataType;
                    dat->m_responder = m_machineID;
                    dat->m_Destination = dataDest;
                    dat->m_dataBlk = db;
                    dat->m_bitMask = wm;
                    dat->m_MessageSize = MessageSizeType_Data;
                    // v4: Set shared_hint for shared grants
                    dat->m_m_shared_hint = sharedHint;
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
                    sendDataReliable(dat, std::move(onSent));
                }
                it = _retryQueue.erase(it);
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

        const bool fullLine = pending.expectedMask == ~0ULL;
        fatal_if(!_backend->resolveWritePersistence(
                     pending.linePa, pending.sourceSocket,
                     pending.originalTxnId, fullLine,
                     pending.homePa, pending.homeNode, pending.homeSocket,
                     pending.requesterNode, pending.permissionEpoch,
                     pending.ownerWriteback, pending.storeCommit),
                 "EP_SNF node_id=%d: WriteNoSnp lacks EP requester metadata "
                 "PA=0x%lx sourceSocket=%d", _nodeId, pending.linePa,
                 pending.sourceSocket);
        pending.disposition = pending.ownerWriteback
            ? UBWriteDisposition::DropOwner
            : UBWriteDisposition::MemoryOnly;
        pending.internalPublication = !_backend->haEndpointEnabled() &&
            !pending.ownerWriteback;
        if (_backend->haEndpointEnabled()) {
            pending.haWrite = true;
        }
        _pendingWrites.emplace(pending.dbid, pending);

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
        DPRINTF(RubyEP,
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
    int homeNode = -1;
    if (msg->m_ep_proxy_op == EpProxyOp_RecallUnique) {
        DPRINTF(RubyEP,
                "[RECALL-PROXY-OUTER-ENTER] node=%d localPA=0x%lx "
                "tick=%lu\n",
                _nodeId, msg->m_addr, curTick());
    }
    int grantResult = internalRecall
        ? static_cast<int>(OuterGrantType::GlobalGrantShared)
        : _backend->handleRemoteDemandMiss(
              msg->m_addr, neededPerm, writeIntent, ingressSocket, homeNode);

    // Q3: If grant blocked, queue for retry instead of sending stale data
    if (grantResult < 0) {
        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d: grant BUSY for PA=0x%lx, queuing retry\n",
                _nodeId, msg->m_addr);
        EPSNFController::RetryEntry entry;
        entry.linePa = msg->m_addr;
        entry.neededPerm = neededPerm;
        entry.writeIntent = writeIntent;
        entry.ingressSocket = ingressSocket;
        entry.hnReq = msg->m_requestor;
        entry.fwdReq = msg->m_fwdRequestor;
        entry.dataToFwdReq = msg->m_dataToFwdRequestor;
        entry.publishOnData = msg->m_ubcc_publish_on_data;
        _retryQueue.push_back(entry);
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

    // For ReadNoSnpSep (early-dealloc DMT), send ReadReceipt to HN-F.
    if (msg->m_type == CHIRequestType_ReadNoSnpSep) {
        auto rsp = std::make_shared<CHIResponseMsg>(
            curTick(), cacheLineSize, m_ruby_system,
            msg->m_addr, CHIResponseType_ReadReceipt,
            m_machineID, hnDest,
            false, false, 0, 0, MessageSizeType_Control);
        sendResponseReliable(rsp);
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
            entry.linePa = msg->m_addr;
            entry.neededPerm = neededPerm;
            entry.writeIntent = writeIntent;
            entry.ingressSocket = ingressSocket;
            entry.hnReq = msg->m_requestor;
            entry.fwdReq = msg->m_fwdRequestor;
            entry.dataToFwdReq = msg->m_dataToFwdRequestor;
            entry.publishOnData = msg->m_ubcc_publish_on_data;
            _retryQueue.push_back(entry);
            scheduleEvent(Cycles(epsnf_retry_cycles()));
            return true;
        }
        // NoData: zero-fill is the correct behavior
        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d: CompData fallback to zeros "
                "(NoData source)\n", _nodeId);
    }

    // ---- v4: shared_hint + CompData type for shared grants (§4.4.2, §5.1) ----
    // Shared grant (neededPerm==0): CompData_SC with m_m_shared_hint=true
    // Unique grant (neededPerm==1): CompData_UC (baseline unique fill)
    CHIDataType dataType = (neededPerm == 0) ? CHIDataType_CompData_SC
                                             : CHIDataType_CompData_UC;
    bool sharedHint = (neededPerm == 0);  // v4: always register EP-RNF for shared

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
        dat->m_type = dataType;
        dat->m_responder = m_machineID;
        dat->m_Destination = dataDest;
        dat->m_dataBlk = db;
        dat->m_bitMask = wm;
        dat->m_MessageSize = MessageSizeType_Data;
        // v4: Set shared_hint on CompData for shared grants
        dat->m_m_shared_hint = sharedHint;
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
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d recvResponseMsg\n", _nodeId);
    return true;
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
        fatal_if(msg->m_addr != pending.linePa,
                  "EP_SNF node_id=%d: NCBWrData address mismatch id=%lu",
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
                if (pending.ownerWriteback) {
                    result = _backend->handleWritebackWithMeta(
                        pending.linePa, false, pending.data,
                        pending.permissionEpoch, pending.requesterNode,
                        pending.sourceSocket, &pending.permissionReqId);
                } else if (pending.storeCommit) {
                    result = _backend->commitStore(
                        pending.homePa, pending.requesterNode,
                        pending.permissionEpoch, pending.storeCommitId,
                        pending.expectedMask, pending.data, pending.homeNode,
                        pending.homeSocket, pending.sourceSocket,
                        pending.permissionReqId);
                } else if (pending.internalPublication) {
                    result = _backend->publishInternalWriteback(
                        pending.homePa, pending.storeCommitId,
                        pending.expectedMask, pending.data, pending.homeNode,
                        pending.homeSocket, pending.sourceSocket,
                        pending.permissionReqId);
                } else {
                    fatal("EP_SNF node_id=%d: unclassified WriteNoSnp "
                          "PA=0x%lx id=%lu\n", _nodeId, pending.linePa,
                          transactionId);
                }
            }
            if (result == -2) {
                // Data completion and UBAdapter responses both schedule this
                // controller directly. Polling the full pending-write map while
                // a response is in flight adds no liveness and becomes O(N)
                // host work under directory pressure.
                continue;
            }
            if (result == 0 && pending.ownerWriteback) {
                inform("[EP-WB-STALE-DROP] node=%d pa=0x%lx epoch=%lu "
                       "dbid=%lu wireReqId=%lu\n", _nodeId, pending.linePa,
                       pending.permissionEpoch, transactionId,
                       pending.permissionReqId);
                _backend->dropStaleOwnerReplacement(
                    pending.linePa, pending.permissionEpoch);
                pending.granted = true;
                result = 1;
            }
            if (result == 0 && pending.internalPublication) {
                pending.permissionReqId = 0;
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
        if (done.haWrite) {
            fatal_if(!_backend->acknowledgeHAPermission(
                         done.homePa, HAOperation::Write, HAStatus::Ok,
                         done.permissionEpoch, done.homeNode, done.homeSocket,
                         done.permissionReqId, done.sourceSocket),
                     "EP_SNF node_id=%d: failed to queue HA Write ack id=%lu",
                     _nodeId, transactionId);
        }
        _backend->completeHnPersistence(
            done.linePa, done.originalTxnId, done.sourceSocket);
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
