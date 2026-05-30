#include "mem/ruby/protocol/chi/ep/EPSNFController.hh"

#include "base/logging.hh"
#include "debug/RubyCHIGeneric.hh"
#include "mem/ruby/common/DataBlock.hh"
#include "mem/ruby/protocol/CHI/CHIDataMsg.hh"
#include "mem/ruby/protocol/CHI/CHIRequestMsg.hh"
#include "mem/ruby/protocol/CHI/CHIResponseMsg.hh"
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

    // ---- M5 R1: Request type gate ----
    // EP_SNF only services ReadNoSnp (and ReadNoSnpSep) requests.
    // All other request types (Load, Store, DVM, Snoop variants, etc.)
    // are not valid on the EP_SNF receive path and must be rejected
    // or deferred to the base class.
    if (msg->m_type != CHIRequestType_ReadNoSnp &&
        msg->m_type != CHIRequestType_ReadNoSnpSep) {
        // This is a protocol violation — only ReadNoSnp[Sep] should
        // arrive at EP_SNF.  Warn and return unhandled; production
        // code would trigger a protocol-level error response.
        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d: unsupported request type %s addr=0x%lx "
                "-- returning false (unhandled)\n",
                _nodeId, msg->m_type, msg->m_addr);
        fatal("EP_SNF node_id=%d: received non-ReadNoSnp request type "
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
    // GlobalReadShared or GlobalReadUnique
    int homeNode = -1;
    int grantResult = _backend->handleRemoteMiss(
        msg->m_addr, neededPerm, writeIntent, homeNode);

    DPRINTF(RubyCHIGeneric,
            "EP_SNF node_id=%d: grantResult=%d homeNode=%d\n",
            _nodeId, grantResult, homeNode);

    // Q2 WORKAROUND: handleRemoteMiss returns -2 for local DSM lines
    // that should be handled by dl_snf.  Skip processing silently.
    if (grantResult == -2) {
        return true;
    }

    // ---- M5: Record sideband for inspection by Python tests ----
    _backend->recordSideband(msg->m_addr, neededPerm, writeIntent,
                              (neededPerm == 0) ? 0 : 1,  // 0=GlobalReadShared, 1=GlobalReadUnique
                              grantResult, homeNode);

    // Respond to HN with RespSepData + CompData
    // Q1: Use real grant data from EPBackend/lastGrantData instead of dummy zero.
    NetDest dest(m_ruby_system);
    dest.add(msg->m_requestor);

    auto rsp = std::make_shared<CHIResponseMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        msg->m_addr, CHIResponseType_RespSepData,
        m_machineID, dest,
        false, false, 0, 0, MessageSizeType_Control);
    sendResponseMsg(rsp);

    // Build CompData with real data from the grant path.
    DataBlock db(cacheLineSize);
    const uint8_t *gdata = _backend->lastGrantData();
    if (gdata && _backend->lastGrantDataSize() >= cacheLineSize) {
        db.setData(gdata, 0, cacheLineSize);
        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d: CompData populated with grant data "
                "first_byte=0x%02x\n", _nodeId, gdata[0]);
    } else {
        DPRINTF(RubyCHIGeneric,
                "EP_SNF node_id=%d: CompData fallback to zeros "
                "(grant data not available)\n", _nodeId);
    }

    auto dat = std::make_shared<CHIDataMsg>(
        curTick(), cacheLineSize, m_ruby_system,
        msg->m_addr, CHIDataType_CompData_I,
        m_machineID, dest,
        db,
        WriteMask(cacheLineSize),
        false, 0, MessageSizeType_Data);
    sendDataMsg(dat);

    return true;
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
    DPRINTF(RubyCHIGeneric, "EP_SNF node_id=%d recvDataMsg\n", _nodeId);
    return true;
}

} // namespace ruby
} // namespace gem5
