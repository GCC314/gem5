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

namespace gem5
{

namespace ruby
{

using namespace CHI;

EPSNFController::EPSNFController(const Params &p)
  : EPController(p), _backend(p.ep_backend)
{
}

void
EPSNFController::init()
{
    EPController::init();
    fatal_if(!_backend, "EP_SNF node_id=%d: no backend attached", _nodeId);
    selfTest();
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

    // Q3: Process retry queue — request grants that were previously BUSY
    if (!_retryQueue.empty()) {
        bool needWakeup = false;
        for (auto it = _retryQueue.begin(); it != _retryQueue.end(); ) {
            int homeNode = -1;
            int grantResult = _backend->handleRemoteMiss(
                it->linePa, it->neededPerm, it->writeIntent, homeNode);
            if (grantResult >= 0) {
                // Grant succeeded — send CompData (same as normal flow)
                NetDest hnDest(m_ruby_system);
                hnDest.add(it->hnReq);
                NetDest dataDest(m_ruby_system);
                if (it->dataToFwdReq)
                    dataDest.add(it->fwdReq);
                else
                    dataDest.add(it->hnReq);

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
                        it->linePa, CHIDataType_CompData_UC,
                        m_machineID, dataDest, db, wm,
                        false, 0, MessageSizeType_Data);
                    sendDataMsg(dat);
                }
                it = _retryQueue.erase(it);
            } else {
                needWakeup = true;
                ++it;
            }
        }
        if (needWakeup)
            scheduleEvent(Cycles(1));
    }

    if (_backend)
        _backend->wakeup();
}

void
EPSNFController::print(std::ostream& out) const
{
    out << "[EP_SNF node_id=" << _nodeId << " v=" << m_version << "]";
}

bool
EPSNFController::recvRequestMsg(const CHIRequestMsg *msg)
{
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
        msg->m_addr, neededPerm, writeIntent, homeNode);

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
        scheduleEvent(Cycles(1));
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
        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d: CompData fallback to zeros "
                "(grant data not available)\n", _nodeId);
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
        if (gdata != nullptr) {
            db.setData(gdata + offset, offset, chunkSize);
        }
        // else db keeps default zeros

        auto dat = std::make_shared<CHIDataMsg>(
            curTick(), cacheLineSize, m_ruby_system,
            msg->m_addr, CHIDataType_CompData_UC,
            m_machineID, dataDest,
            db, wm,
            false, 0, MessageSizeType_Data);
        // Q3: Defer send by 1 tick to prevent same-tick TBE race
        // at HN-F (see docs/tbe-race-condition.svg for details).
        _deferredCompData.push_back(dat);
    }

    // Schedule deferred sends
    if (!_deferredCompData.empty()) {
        scheduleEvent(Cycles(1));
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
            const DataBlock &db = msg->m_dataBlk;
            const WriteMask &wm = msg->m_bitMask;
            uint8_t buf[64];

            // Read current line from DDR4, then apply write mask
            RequestPtr req = std::make_shared<Request>(
                msg->m_addr, cacheLineSize, 0, RequestorID(0));
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

            // Write back to DDR4
            Packet wrPkt(req, MemCmd::WriteReq);
            wrPkt.dataStatic(buf);
            phys_mem->functionalAccess(&wrPkt);

            DPRINTF(RubyCHIGeneric,
                    "EP_SNF node_id=%d: wrote data to DDR4 "
                    "addr=0x%lx type=%d\n",
                    _nodeId, msg->m_addr, msg->m_type);
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

} // namespace ruby
} // namespace gem5
