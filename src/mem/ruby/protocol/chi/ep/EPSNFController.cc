#include "mem/ruby/protocol/chi/ep/EPSNFController.hh"

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "mem/ruby/common/DataBlock.hh"
#include "mem/ruby/protocol/CHI/CHIDataMsg.hh"
#include "mem/ruby/protocol/CHI/CHIRequestMsg.hh"
#include "mem/ruby/protocol/CHI/CHIResponseMsg.hh"
#include "mem/simple_mem.hh"
#include "mem/packet.hh"
#include "mem/request.hh"
#include "params/EPSNFController.hh"
#include <cstdlib>

namespace gem5
{

namespace ruby
{

using namespace CHI;

// Retry cycle count after BUSY grant — configurable via env var.
// Default 1,600,000 cycles = 800 µs @ 2 GHz.
static uint64_t epsnf_retry_cycles() {
    static uint64_t v = 0;
    if (v == 0) {
        const char *e = std::getenv("EP_RETRY_CYCLES");
        v = e ? std::strtoull(e, nullptr, 10) : 20000;  // default 10µs @2GHz
    }
    return v;
}

// Δ_noc: cross-socket NoC extra latency (cycles).  In dual-socket topologies
// the SN-F may sit on a different socket from the requester HN-F, adding NoC
// hops inside gem5.  This configurable delay (default 0 = single-socket)
// models that extra cross-socket routing latency.  See latency_tuning_constraints.md §6.1.
static uint64_t epsnf_delta_noc_cycles() {
    static int64_t v = -1;
    if (v < 0) {
        const char *e = std::getenv("EP_DELTA_NOC_CYCLES");
        v = e ? (int64_t)std::strtoull(e, nullptr, 10) : 0;
    }
    return (uint64_t)v;
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

    // Q3: Send deferred CompData (1-tick delay for TBE race fix)
    processDeferredData();

    // v4: Send deferred grants (§4.4.2, I10 timing invariant)
    processDeferredGrants();

    // v4: Process pending writebacks
    processPendingWritebacks();

    // Poll backend first so newly-arrived responses are available
    // for retry queue evaluation below
    if (_backend)
        _backend->wakeup();

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

                const uint8_t *gdata = _backend->lastGrantData();
                for (int i = 0; i < dataMsgsPerLine; i++) {
                    int offset = i * dataChannelSize;
                    int chunkSize = (i == dataMsgsPerLine - 1) ?
                        (cacheLineSize - offset) : dataChannelSize;
                    WriteMask wm(cacheLineSize);
                    wm.setMask(offset, chunkSize);
                    DataBlock db(cacheLineSize);
                    if (gdata != nullptr)
                        db.setData(gdata + offset, offset, chunkSize);
                    auto dat = std::make_shared<CHIDataMsg>(
                        curTick(), cacheLineSize, m_ruby_system,
                        it->linePa, dataType,
                        m_machineID, dataDest, db, wm,
                        false, 0, false, MessageSizeType_Data);
                    // v4: Set shared_hint for shared grants
                    dat->m_m_shared_hint = sharedHint;
                    sendDataMsg(dat);
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
    printf("[EPSNF-RECV] node=%d type=%d addr=0x%lx\n",
           _nodeId, msg->m_type, msg->m_addr);
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d recvRequestMsg type=%s addr=0x%lx\n",
            _nodeId, msg->m_type, msg->m_addr);
    warn("EP_SNF node_id=%d recvRequestMsg type=%d addr=0x%lx "
         "dataToFwdReq=%d\n",
         _nodeId, msg->m_type, msg->m_addr, msg->m_dataToFwdRequestor);

    // Q2: Handle WriteNoSnp / WriteNoSnpPtl — forward to DDR4 directly.
    // No UBCC directory involvement needed for writes.
    // Store pending write requestor; CompDBIDResp sent after NCBWrData
    // arrives via recvDataMsg.
    if (msg->m_type == CHIRequestType_WriteNoSnp ||
        msg->m_type == CHIRequestType_WriteNoSnpPtl) {

        if (!_backend) {
            fatal("EP_SNF node_id=%d: no backend for write\n", _nodeId);
        }
        _backend->checkDsmAddr(msg->m_addr);

        // Store pending write: addr → requestor (HN-F)
        _pendingWrites[msg->m_addr] = msg->m_requestor;

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

    // ---- M5: Read UBCC Sideband Fields ----
    int neededPerm = msg->m_ubcc_needed_perm;  // 0=Shared, 1=Unique
    bool writeIntent = msg->m_ubcc_write_intent;

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
    int grantResult = _backend->handleRemoteMiss(
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
        sendResponseMsg(rsp);
    }

    const uint8_t *gdata = _backend->lastGrantData();
    if (gdata && _backend->lastGrantDataSize() >= cacheLineSize) {
        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d: CompData populated with grant data "
                "first_byte=0x%02x\n", _nodeId, gdata[0]);
    } else {
        // F3: Data not ready — defer/retry instead of silent zero-fill.
        // Only NoData (explicit zero-fill) is allowed through without data.
        GrantDataSource ds = _backend->lastGrantDataSource();
        if (ds != GrantDataSource::NoData) {
            DPRINTF(RubyCHIGeneric,
                    "EP_SNF node_id=%d: grant data not ready (dataSource=%d), "
                    "deferring to retry queue\n",
                    _nodeId, static_cast<int>(ds));
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
        if (gdata != nullptr) {
            db.setData(gdata + offset, offset, chunkSize);
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
        _deferredCompData.push_back(dat);
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
    for (auto &dat : _deferredCompData) {
        sendDataMsg(dat);
    }
    _deferredCompData.clear();
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
            _deferredCompData.push_back(dat);
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

    // Q2: Write NCBWrData to DDR4 (SimpleMemory) via functionalAccess
    if (msg->m_type == CHIDataType_NCBWrData ||
        msg->m_type == CHIDataType_CBWrData_UC ||
        msg->m_type == CHIDataType_CBWrData_UD_PD ||
        msg->m_type == CHIDataType_CBWrData_SC ||
        msg->m_type == CHIDataType_CBWrData_SD_PD ||
        msg->m_type == CHIDataType_CBWrData_I) {

        auto *phys_mem = m_ruby_system->getPhysMem();
        if (phys_mem) {
            // ---- v4: Cross-node NCBWrData routing (§4.4.2 item 3) ----
            // Translate local PA to home PA so data goes to home node's DDR4,
            // not the local node's memory.
            uint64_t writePa = msg->m_addr;  // default: local PA
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
            uint8_t buf[64];

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

            // Write back to DDR4 (at home PA)
            Packet wrPkt(req, MemCmd::WriteReq);
            wrPkt.dataStatic(buf);
            phys_mem->functionalAccess(&wrPkt);

            // v4: Follow WriteNoSnp through EPBackend chain to UBCC,
            // same pattern as ReadNoSnp → handleRemoteMiss → UBCC.
            // Notify UBCC that home data has been written to DRAM,
            // releasing directory ownership.
            if (_backend && _backend->isDsmAddr(writePa)) {
                int wbRet = _backend->handleWriteback(writePa, false);
                if (wbRet == -2) {
                    std::fprintf(stderr,
                                 "[EPSNF-WB-PENDING] node=%d pa=0x%lx\n",
                                 _nodeId, writePa);
                    // Queue for retry in wakeup
                    _pendingWritebacks.push_back({writePa, false});
                }
            }

            DPRINTF(RubyCHIGeneric,
                    "EP_SNF node_id=%d: wrote data to DDR4 "
                    "addr=0x%lx type=%d\n",
                    _nodeId, writePa, msg->m_type);
        }

        // Send CompDBIDResp if there's a pending write for this address
        auto it = _pendingWrites.find(msg->m_addr);
        if (it != _pendingWrites.end()) {
            NetDest hnDest(m_ruby_system);
            hnDest.add(it->second);  // requestor = HN-F

            auto rsp = std::make_shared<CHIResponseMsg>(
                curTick(), cacheLineSize, m_ruby_system,
                msg->m_addr, CHIResponseType_CompDBIDResp,
                m_machineID, hnDest,
                false, false, 0, 0, MessageSizeType_Control);
            sendResponseMsg(rsp);

            DPRINTF(RubyCHIGeneric,
                    "EP_SNF node_id=%d: CompDBIDResp sent for write "
                    "addr=0x%lx\n", _nodeId, msg->m_addr);
            _pendingWrites.erase(it);
        }
        return true;
    }

    return true;
}

void
EPSNFController::processPendingWritebacks()
{
    if (_pendingWritebacks.empty()) return;
    for (auto it = _pendingWritebacks.begin(); it != _pendingWritebacks.end(); ) {
        int wbRet = _backend->handleWriteback(it->linePa, it->keepAsClean);
        if (wbRet != -2) {
            // Writeback completed (or error) — remove from queue
            it = _pendingWritebacks.erase(it);
        } else {
            ++it;
        }
    }
    if (!_pendingWritebacks.empty())
        scheduleEvent(Cycles(epsnf_retry_cycles()));
}

} // namespace ruby
} // namespace gem5
