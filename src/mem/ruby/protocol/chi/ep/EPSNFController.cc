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
#include "mem/simple_mem.hh"
#include "mem/packet.hh"
#include "mem/request.hh"
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

    // Phase 2 async: Process pending writebacks after backend poll so that
    // freshly arrived QueryLineMetaResp messages cached in _readyResponses
    // can resolve metadata before the WriteBackReq is attempted.
    processPendingWritebacks();

    // Q3: Process retry queue — request grants that were previously BUSY
    if (!_retryQueue.empty()) {
        bool needWakeup = false;
        for (auto it = _retryQueue.begin(); it != _retryQueue.end(); ) {
            int homeNode = -1;
            int grantResult = _backend->handleRemoteMiss(
                it->linePa, it->neededPerm, it->writeIntent, _socketId, homeNode);
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
                        curTick(), cacheLineSize, m_ruby_system,
                        it->linePa, dataType,
                        m_machineID, dataDest, db, wm,
                        false, 0, false, MessageSizeType_Data);
                    // v4: Set shared_hint for shared grants
                    dat->m_m_shared_hint = sharedHint;
                    sendDataReliable(dat);
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

bool
EPSNFController::recvRequestMsg(const CHIRequestMsg *msg)
{
    DPRINTF(RubyCHIGeneric, "[DEBUG-EPSNF-RECV] EP_SNF node_id=%d recvRequestMsg type=%s addr=0x%lx\n",
            _nodeId, msg->m_type, msg->m_addr);
    warn("EP_SNF node_id=%d recvRequestMsg type=%d addr=0x%lx "
         "dataToFwdReq=%d\n",
         _nodeId, msg->m_type, msg->m_addr, msg->m_dataToFwdRequestor);

    // A WriteNoSnp is a two-phase operation.  CompDBIDResp grants the
    // sender permission to transmit NCBWrData; waiting for that data before
    // replying deadlocks the request/data handshake.
    if (msg->m_type == CHIRequestType_WriteNoSnp ||
        msg->m_type == CHIRequestType_WriteNoSnpPtl) {

        if (!_backend) {
            fatal("EP_SNF node_id=%d: no backend for write\n", _nodeId);
        }
        _backend->checkDsmAddr(msg->m_addr);

        PendingWrite pending;
        if (msg->m_type == CHIRequestType_WriteNoSnpPtl) {
            const int offset = msg->m_accAddr - msg->m_addr;
            pending.expectedMask = ((1ULL << msg->m_accSize) - 1) << offset;
        } else {
            pending.expectedMask = ~0ULL;
        }
        _pendingWrites[msg->m_addr] = pending;

        // ── Phase C4 trace point 2: WriteNoSnp receipt ──
        {
            uint64_t off = msg->m_addr & 0x1FFFULL;
            uint64_t ckOff = msg->m_addr & 0xFFFFFULL;
            if (ckOff < 0x80000ULL && (off % 64 == 0)) {
                inform(
                    "[C4-ESNF-WNOSNP] node=%d addr=0x%lx off=0x%lx "
                    "expectedMask=0x%lx\n",
                    _nodeId, msg->m_addr, off, pending.expectedMask);
            }
        }

        NetDest hnDest(m_ruby_system);
        hnDest.add(msg->m_requestor);
        auto rsp = std::make_shared<CHIResponseMsg>(
            curTick(), cacheLineSize, m_ruby_system,
            msg->m_addr, CHIResponseType_CompDBIDResp,
            m_machineID, hnDest,
            false, false, 0, 0, MessageSizeType_Control);
        sendResponseReliable(rsp);
        inform(
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

    // Map sideband to outer request and dispatch
    int homeNode = -1;
    if (msg->m_ep_proxy_op == EpProxyOp_RecallUnique) {
        inform(
                     "[RECALL-PROXY-OUTER-ENTER] node=%d localPA=0x%lx "
                     "tick=%lu\n",
                     _nodeId, msg->m_addr, curTick());
    }
    int grantResult = _backend->handleRemoteDemandMiss(
        msg->m_addr, neededPerm, writeIntent, _socketId, homeNode);

    // Q3: If grant blocked, queue for retry instead of sending stale data
    if (grantResult < 0) {
        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d: grant BUSY for PA=0x%lx, queuing retry\n",
                _nodeId, msg->m_addr);
        EPSNFController::RetryEntry entry;
        entry.linePa = msg->m_addr;
        entry.neededPerm = neededPerm;
        entry.writeIntent = writeIntent;
        entry.hnReq = msg->m_requestor;
        entry.fwdReq = msg->m_fwdRequestor;
        entry.dataToFwdReq = msg->m_dataToFwdRequestor;
        _retryQueue.push_back(entry);
        scheduleEvent(Cycles(epsnf_retry_cycles()));
        return true;
    }

    // ---- Record sideband for inspection ----
    _backend->recordSideband(msg->m_addr, neededPerm, writeIntent,
                              (neededPerm == 0) ? 0 : 1,  // 0=GlobalReadShared, 1=GlobalReadUnique
                              grantResult, homeNode);

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
    int grantDataState = _backend->takeGrantData(
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
            entry.hnReq = msg->m_requestor;
            entry.fwdReq = msg->m_fwdRequestor;
            entry.dataToFwdReq = msg->m_dataToFwdRequestor;
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
            curTick(), cacheLineSize, m_ruby_system,
            msg->m_addr, dataType,
            m_machineID, dataDest,
            db, wm,
            false, 0, false, MessageSizeType_Data);
        // v4: Set shared_hint on CompData for shared grants
        dat->m_m_shared_hint = sharedHint;
        // Q3: Defer send by 1 tick to prevent same-tick TBE race
        // at HN-F (see docs/tbe-race-condition.svg for details).
        PendingDataOutput pending;
        pending.msg = dat;
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
        sendDataReliable(pending.msg);
    }
    _deferredCompData.clear();
}

void
EPSNFController::sendResponseReliable(std::shared_ptr<CHIResponseMsg> msg)
{
    if (_pendingResponses.empty() && sendResponseMsg(msg))
        return;

    _pendingResponses.push_back(std::move(msg));
    scheduleEvent(Cycles(1));
}

void
EPSNFController::sendDataReliable(std::shared_ptr<CHIDataMsg> msg)
{
    if (_pendingData.empty() && sendDataMsg(msg))
        return;

    PendingDataOutput pending;
    pending.msg = std::move(msg);
    _pendingData.push_back(std::move(pending));
    scheduleEvent(Cycles(1));
}

void
EPSNFController::processPendingOutputs()
{
    while (!_pendingResponses.empty()) {
        if (!sendResponseMsg(_pendingResponses.front()))
            break;
        _pendingResponses.pop_front();
    }

    while (!_pendingData.empty()) {
        auto &pending = _pendingData.front();
        if (!sendDataMsg(pending.msg))
            break;
        _pendingData.pop_front();
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
                curTick(), cacheLineSize, m_ruby_system,
                entry.linePa, dataType,
                m_machineID, NetDest(m_ruby_system),
                db, wm,
                false, 0, false, MessageSizeType_Data);
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
    auto pendingIt = _pendingWrites.find(msg->m_addr);
    DPRINTF(RubyEP,
                 "[EPSNF-DATA-RECV] node=%d type=%d addr=0x%lx pendingWrite=%d\n",
                 _nodeId, msg->m_type, msg->m_addr,
                 pendingIt != _pendingWrites.end() ? 1 : 0);

    // Q2: Write NCBWrData to DDR4 (SimpleMemory) via functionalAccess
    if (msg->m_type == CHIDataType_NCBWrData ||
        msg->m_type == CHIDataType_CBWrData_UC ||
        msg->m_type == CHIDataType_CBWrData_UD_PD ||
        msg->m_type == CHIDataType_CBWrData_SC ||
        msg->m_type == CHIDataType_CBWrData_SD_PD ||
        msg->m_type == CHIDataType_CBWrData_I) {

        uint64_t writePa = msg->m_addr;
        uint8_t buf[64]{};
        auto *phys_mem = m_ruby_system->getPhysMem();
        if (phys_mem) {
            // ---- v4: Cross-node NCBWrData routing (§4.4.2 item 3) ----
            // Translate local PA to home PA so data goes to home node's DDR4,
            // not the local node's memory.
            if (_backend && _backend->isDsmAddrCrossNode(msg->m_addr)) {
                auto &addrMap = _backend->addrMap();
                int homeNode = addrMap.homeNode(_nodeId, msg->m_addr);
                uint64_t offset = addrMap.dsmOffset(msg->m_addr);
                writePa = addrMap.buildDsmPA(homeNode, homeNode, offset);
                DPRINTF(RubyCHIGeneric,
                        "EP_SNF node_id=%d: NCBWrData PA translation: "
                        "local=0x%lx → home=0x%lx (homeNode=%d)\n",
                        _nodeId, msg->m_addr, writePa, homeNode);
            }

            const DataBlock &db = msg->m_dataBlk;
            const WriteMask &wm = msg->m_bitMask;
            // Read current line from DDR4 (at home PA), then apply write mask
            RequestPtr req = std::make_shared<Request>(
                writePa, cacheLineSize, 0, RequestorID(0));
            req->setFlags(Request::PHYSICAL);
            Packet rdPkt(req, MemCmd::ReadReq);
            rdPkt.dataStatic(buf);
            phys_mem->functionalAccess(&rdPkt);

            // Overwrite with incoming data at masked positions
            for (int i = 0; i < cacheLineSize; i++) {
                if (wm.test(i)) {
                    buf[i] = db.getByte(i);
                }
            }

            // ── Phase C4 trace point 1: assembled NCBWrData payload ──
            {
                uint64_t off = writePa & 0x1FFFULL;
                uint64_t absOff = writePa & 0xFFFFFULL;
                // TC132 checkpoint PAs: offsets 0x000-0x7FFC0 (ACTIVE=8192*64)
                if (absOff < 0x80000ULL && (off % 64 == 0)) {
                    uint64_t w0;
                    std::memcpy(&w0, buf, 8);
                    bool full = true;
                    for (int i = 0; i < 64; i++)
                        if (!msg->m_bitMask.test(i)) { full = false; break; }
                    inform(
                        "[C4-EPSNF-BEAT] node=%d pa=0x%lx writePa=0x%lx "
                        "off=0x%lx fullMask=%d w0=0x%016lx pending=%d\n",
                        _nodeId, msg->m_addr, writePa, absOff, full ? 1 : 0, w0,
                        pendingIt != _pendingWrites.end() ? 1 : 0);
                }
            }

            // Write back to DDR4 (at home PA)
            Packet wrPkt(req, MemCmd::WriteReq);
            wrPkt.dataStatic(buf);
            phys_mem->functionalAccess(&wrPkt);

            DPRINTF(RubyCHIGeneric,
                    "EP_SNF node_id=%d: wrote data to DDR4 "
                    "addr=0x%lx type=%d\n",
                    _nodeId, writePa, msg->m_type);
        }

        if (pendingIt != _pendingWrites.end()) {
            uint64_t beatMask = 0;
            for (int i = 0; i < cacheLineSize; ++i) {
                if (msg->m_bitMask.test(i))
                    beatMask |= 1ULL << i;
            }
            pendingIt->second.receivedMask |= beatMask;
            if ((pendingIt->second.receivedMask & pendingIt->second.expectedMask) ==
                pendingIt->second.expectedMask) {
                // writePa has been translated into the home node's address
                // view, so a local-only DSM test suppresses remote writeback
                // notifications after a normal L2 eviction.
                if (_backend && _backend->isDsmAddrCrossNode(writePa)) {
                    // Phase 2 async: do NOT call handleWriteback here —
                    // it would send an untracked QLM whose reqId is lost.
                    // Instead, enqueue directly and let processPendingWritebacks
                    // handle both QLM query and WriteBackReq with stable reqId.
                    inform(
                                 "[EPSNF-WB-PENDING] node=%d pa=0x%lx\n",
                                 _nodeId, writePa);
                    // Phase 2 async item 5: Deduplicate pending writebacks
                    // per line — retain newest dirty payload.
                    bool replaced = false;
                    for (auto &pwb : _pendingWritebacks) {
                        if (pwb.linePa == writePa) {
                            // Phase 2 corrective item 3: consume stale QLM
                            if (pwb.queryInFlight && _backend) {
                                UBAdapter *wa = _backend->getUBAdapter(0);
                                if (wa) {
                                    uint64_t _dE; int _dO; bool _dF;
                                    wa->tryGetQueryLineMetaResp(
                                        pwb.queryReqId, _dE, _dO, _dF);
                                }
                            }
                            pwb.keepAsClean = false;
                            pwb.hasData = true;
                            std::memcpy(pwb.data, buf, 64);
                            // Reset query state — new identity
                            pwb.queryReqId = 0;
                            pwb.queryInFlight = false;
                            pwb.cachedFound = false;
                            pwb.retryCount = 0;
                            pwb.nextRetryTick = 0;
                            replaced = true;
                            inform(
                                "[EPSNF-WB-DEDUP] node=%d pa=0x%lx "
                                "(replaced existing entry)\n",
                                _nodeId, writePa);
                            break;
                        }
                    }
                    if (!replaced) {
                        PendingWriteback pwb;
                        pwb.linePa = writePa;
                        pwb.keepAsClean = false;
                        pwb.hasData = true;
                        std::memcpy(pwb.data, buf, 64);
                        _pendingWritebacks.push_back(pwb);
                    }
                }
                inform(
                             "[EPSNF-WRITE-DONE] node=%d addr=0x%lx received=0x%lx\n",
                             _nodeId, msg->m_addr, pendingIt->second.receivedMask);
                _pendingWrites.erase(pendingIt);
            }
        } else {
            warn(
                         "[EPSNF-DATA-NO-PENDING-WRITE] node=%d type=%d addr=0x%lx\n",
                         _nodeId, msg->m_type, msg->m_addr);
        }
        return true;
    }

    return true;
}

void
EPSNFController::processPendingWritebacks()
{
    if (_pendingWritebacks.empty()) return;

    // Phase 2 async: exponential backoff constants
    static const int MAX_RETRIES = 128;
    static const Tick BASE_BACKOFF_TICKS = 20000;  // 20k ticks (~20 µs)
    static const Tick MAX_BACKOFF_TICKS = 100000000; // 100M ticks cap

    for (auto it = _pendingWritebacks.begin(); it != _pendingWritebacks.end(); ) {
        Tick now = curTick();

        // Bounded retry with backoff: skip if not yet time to retry
        if (it->retryCount > 0 && now < it->nextRetryTick) {
            ++it;
            continue;
        }

        // ── Poll for QLM response if one is in-flight ──
        if (it->queryInFlight && _backend) {
            UBAdapter *wa = _backend->getUBAdapter(0);
            if (wa) {
                uint64_t resolvedEpoch = 0;
                int resolvedOwner = -1;
                bool resolvedFound = false;
                if (wa->tryGetQueryLineMetaResp(it->queryReqId,
                                                resolvedEpoch,
                                                resolvedOwner,
                                                resolvedFound)) {
                    it->queryInFlight = false;
                    it->cachedEpoch = resolvedEpoch;
                    it->cachedOwnerNode = resolvedOwner;
                    it->cachedFound = resolvedFound;
                    inform(
                        "[EPSNF-QLM-READY] node=%d pa=0x%lx reqId=%lu "
                        "found=%d epoch=%lu owner=%d\n",
                        _nodeId, it->linePa, it->queryReqId,
                        resolvedFound, resolvedEpoch, resolvedOwner);
                    if (!resolvedFound || resolvedOwner < 0) {
                        warn(
                            "[EPSNF-WB-QLM-FAIL] node=%d pa=0x%lx reqId=%lu "
                            "found=%d owner=%d epoch=%lu — terminal\n",
                            _nodeId, it->linePa, it->queryReqId,
                            resolvedFound ? 1 : 0, resolvedOwner,
                            resolvedEpoch);
                        it = _pendingWritebacks.erase(it);
                        continue;
                    }
                }
            }
        }

        int wbRet;

        if (it->cachedFound && !it->queryInFlight) {
            // ── Phase 2 item 4: cached metadata → exactly one writeback ──
            EPBackend::WritebackQueryMeta meta;
            meta.valid = true;
            meta.epochVal = it->cachedEpoch;
            meta.requesterNode = (it->cachedOwnerNode >= 0)
                                 ? it->cachedOwnerNode : _nodeId;
            wbRet = _backend->handleWriteback(it->linePa, it->keepAsClean,
                                               it->hasData ? it->data : nullptr,
                                               &meta);

        } else if (it->queryReqId > 0) {
            // ── Phase 2 item 4: retry with stable queryReqId ──
            // Reuse the existing reqId as cachedQlmReqId so
            // sendQueryLineMetaReq checks _readyResponses by exact key
            // without allocating a new reqId or scanning by PA.
            wbRet = _backend->handleWriteback(
                it->linePa, it->keepAsClean,
                it->hasData ? it->data : nullptr,
                nullptr,           // no pre-resolved meta
                nullptr,           // no new outQueryReqId (reusing existing)
                it->queryReqId);   // stable cached reqId
            // If handleWriteback consumed a cached QLM response,
            // wbRet will be the writeback result (not -2).
            // If still pending, wbRet == -2 and we back off.

        } else {
            // ── First attempt: send fresh QLM ──
            uint64_t qlmReqId = 0;
            wbRet = _backend->handleWriteback(it->linePa, it->keepAsClean,
                                               it->hasData ? it->data : nullptr,
                                               nullptr,  // no cached meta
                                               &qlmReqId, // get the new reqId
                                               0);        // no cached reqId
            if (wbRet == -2 && qlmReqId > 0) {
                it->queryReqId = qlmReqId;
                it->queryInFlight = true;
                inform(
                    "[EPSNF-WB-QLM-ENQ] node=%d pa=0x%lx reqId=%lu\n",
                    _nodeId, it->linePa, qlmReqId);
            }
        }

        if (wbRet == -2) {
            // Writeback (or QLM) still pending — keep in queue with backoff
            it->retryCount++;
            if (it->retryCount > MAX_RETRIES) {
                warn("[EPSNF-WB-FATAL] node=%d pa=0x%lx max retries exceeded\n",
                     _nodeId, it->linePa);
                it = _pendingWritebacks.erase(it);
                continue;
            }
            Tick delay = BASE_BACKOFF_TICKS;
            if (it->retryCount > 4) {
                int shift = (it->retryCount - 4) / 4;
                if (shift > 10) shift = 10;
                delay = BASE_BACKOFF_TICKS * (1ULL << shift);
                if (delay > MAX_BACKOFF_TICKS) delay = MAX_BACKOFF_TICKS;
            }
            it->nextRetryTick = now + delay;
            DPRINTF(RubyEP,
                "[EPSNF-WB-RETRY] node=%d pa=0x%lx retry=%d delay=%lu\n",
                _nodeId, it->linePa, it->retryCount, delay);
            ++it;

        } else if (wbRet == -3) {
            // ── Phase 2 corrective: terminal QLM failure ──
            // found=false, owner<0, epoch=0 — do NOT silently discard.
            // Retire the entry with explicit diagnostic data.
            warn(
                "[EPSNF-WB-TERMINAL] node=%d pa=0x%lx queryReqId=%lu "
                "retries=%d — QLM metadata resolution FAILED\n",
                _nodeId, it->linePa, it->queryReqId, it->retryCount);
            it = _pendingWritebacks.erase(it);

        } else {
            // Writeback completed (or rejected) — remove from queue
            inform(
                "[EPSNF-WB-DONE] node=%d pa=0x%lx ret=%d retries=%d\n",
                _nodeId, it->linePa, wbRet, it->retryCount);
            it = _pendingWritebacks.erase(it);
        }
    }

    // Schedule next wakeup if there are still pending entries
    if (!_pendingWritebacks.empty())
        scheduleEvent(Cycles(epsnf_retry_cycles()));
}

// ---- v4: write-data completion path with dedup (§5.5) ----
// (called from recvDataMsg when a WriteNoSnp beat completes)

} // namespace ruby
} // namespace gem5
