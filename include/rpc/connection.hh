#pragma once
#include "common/memory_pool.hh"
#include "common/slab.hh"
#include "common/spinlock.hh"
#include "rpc/message.hh"
#include <ankerl/unordered_dense.h>
#include <atomic>
#include <bitset>
#include <cstdint>
#include <cstdlib>
#include <functional>
#include <infiniband/verbs.h>
#include <list>
#include <queue>
#include <rdma/rdma_cma.h>
#include <sys/types.h>
#include <thread>
#include <unordered_map>
#include <utility>
#include <vector>
namespace nextfs::rpc {
static auto default_qp_init_attr() -> ibv_qp_init_attr {
  return {
      nullptr, // qp_context
      nullptr, // send_cq
      nullptr, // recv_cq
      nullptr, // srq
      {
          QUEUE_SIZE, // max_send_wr
          QUEUE_SIZE, // max_recv_wr
          2,          // max_send_sge
          2,          // max_recv_sge
          220,        // max_inline_data
      },              // cap
      IBV_QPT_RC,     // qp_type
      0,              // sq_sig_all
  };
}
struct RdmaExchangeMeta {
  // uint32_t remote_key_;
  uint32_t remote_node_id_;
};
class Connection {
  friend class ConnectionManager;
  constexpr static uint32_t BUFFER_PAGE_SIZE = 1024 * 64;

private:
public:
  Connection(ibv_qp *qp, uint32_t src_id, uint32_t dst_id, rdma_cm_id *cm_id);
  ~Connection();

  auto poll() -> void;
  auto post_recv(void *ctx, void *local_addr, uint32_t lkey) -> void;
  auto post_recv_with_data(void *ctx, void *head_addr, uint32_t head_lkey,
                           void *data_addr, uint32_t data_lkey) -> void;
  auto post_send(void *ctx, void *local_addr, uint32_t length, uint32_t lkey,
                 uint32_t dst_node_id, uint16_t is_last_packet,
                 bool need_inline = false) -> void;
  auto post_send_with_data(void *ctx, void *head_addr, uint32_t head_size,
                           uint32_t head_lkey, uint32_t dst_node_id,
                           void *data_addr, uint32_t data_size,
                           uint32_t data_lkey) -> void;

  [[nodiscard]] auto src_node_id() -> uint32_t { return src_id_; };
  [[nodiscard]] auto dst_node_id() -> uint32_t { return dst_node_id_; };
  [[nodiscard]] auto qp_num() -> uint16_t { return qp_num_; };
  [[nodiscard]] auto cm_id() -> rdma_cm_id * { return cm_id_; };
  [[nodiscard]] auto qp() -> ibv_qp * { return qp_; };

private:
  uint32_t src_id_{};
  uint32_t dst_node_id_{0};
  ibv_qp *qp_{nullptr};
  ibv_srq *srq_{nullptr};
  uint16_t qp_num_{0};
  rdma_cm_id *cm_id_{nullptr};
};

} // namespace nextfs::rpc