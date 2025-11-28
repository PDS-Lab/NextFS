#include "rpc/connection.hh"
#include "common/config.hh"
#include "common/memory_pool.hh"
#include "daemon/env.hh"
#include "rpc/handler.hh"
#include "rpc/message.hh"
#include "rpc/server.hh"
#include <atomic>
#include <cstddef>
#include <cstdint>
#include <cstdlib>
#include <cstring>
#include <infiniband/verbs.h>
#include <malloc.h>
#include <mutex>
#include <rdma/rdma_cma.h>
#include <thread>
#include <unistd.h>

namespace nextfs::rpc {

Connection::Connection(ibv_qp *qp, uint32_t src_id, uint32_t dst_id,
                       rdma_cm_id *cm_id)
    : qp_(qp), src_id_(src_id), dst_node_id_(dst_id), cm_id_(cm_id) {
  qp_num_ = qp_->qp_num;
  srq_ = qp_->srq;
  info("struct Connection constructed");
}
Connection::~Connection() {
  int ret = 0;
  info("connection destroying");
}

auto Connection::post_recv(void *ctx, void *local_addr, uint32_t lkey) -> void {
  ibv_sge sge{
      (uint64_t)local_addr,
      GlobalEnv::config().block_size_,
      lkey,
  };
  ibv_recv_wr wr{
      (uintptr_t)ctx,
      nullptr,
      &sge,
      1,
  };
  ibv_recv_wr *bad = nullptr;
  // only server post recv
  auto ret = ibv_post_srq_recv(srq_, &wr, &bad);
  check(ret, "fail to post recv");
};

auto Connection::post_recv_with_data(void *ctx, void *head_addr,
                                     uint32_t head_lkey, void *data_addr,
                                     uint32_t data_lkey) -> void {
  struct ibv_sge sge_list[2];

  sge_list[0] = {
      (uint64_t)head_addr,
      GlobalEnv::config().rpc_recv_head_size_,
      head_lkey,
  };

  sge_list[1] = {
      (uint64_t)data_addr,
      GlobalEnv::config().block_size_,
      data_lkey,
  };

  ibv_recv_wr wr{
      (uintptr_t)ctx,
      nullptr,
      sge_list,
      2,
  };
  ibv_recv_wr *bad = nullptr;
  // only server post recv
  auto ret = ibv_post_srq_recv(srq_, &wr, &bad);
  check(ret, "fail to post recv");
};

auto Connection::post_send(void *ctx, void *local_addr, uint32_t length,
                           uint32_t lkey, uint32_t dst_node_id,
                           uint16_t is_last_packet, bool need_inline) -> void {
  int ret = 0;
  ibv_send_wr *bad = nullptr;
  // imm = |is_last_packet(1byte)|   src_node_id(15byte)    |
  // dst_node_id(16byte)   |

  uint32_t imm = dst_node_id;
  trace("imm:{}", imm);
  unsigned int inline_flag = need_inline || length <= 220 ? IBV_SEND_INLINE : 0;
  ibv_sge sge{
      (uint64_t)local_addr,
      length,
      lkey,
  };

  ibv_send_wr wr{(uintptr_t)ctx,
                 nullptr,
                 &sge,
                 1,
                 IBV_WR_SEND_WITH_IMM,
                 IBV_SEND_SIGNALED | inline_flag,
                 imm};
  ret = ibv_post_send(qp_, &wr, &bad);
  check(ret, "fail to post send");
}

auto Connection::post_send_with_data(void *ctx, void *head_addr,
                                     uint32_t head_size, uint32_t head_lkey,
                                     uint32_t dst_node_id, void *data_addr,
                                     uint32_t data_size, uint32_t data_lkey)
    -> void {
  int ret = 0;
  ibv_send_wr *bad = nullptr;
  // imm = |is_last_packet(1byte)|   src_node_id(15byte)    |
  // dst_node_id(16byte)   |

  uint32_t imm = dst_node_id;
  trace("imm:{}", imm);

  struct ibv_sge sge_list[2];

  sge_list[0] = {
      (uint64_t)head_addr,
      head_size,
      head_lkey,
  };

  sge_list[1] = {
      (uint64_t)data_addr,
      data_size,
      data_lkey,
  };

  ibv_send_wr wr = {(uintptr_t)ctx,       nullptr,           sge_list, 2,
                    IBV_WR_SEND_WITH_IMM, IBV_SEND_SIGNALED, imm};

  ret = ibv_post_send(qp_, &wr, &bad);
  check(ret, "fail to post send with data");
}

} // namespace nextfs::rpc