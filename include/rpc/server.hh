#pragma once
#include "common/config.hh"
#include "common/memory_pool.hh"
#include "common/slab.hh"
#include "common/spinlock.hh"
#include "common/util.hh"
#include "rpc/connection.hh"
#include "rpc/handler.hh"
#include "rpc/message.hh"
#include <ankerl/unordered_dense.h>
#include <atomic>
#include <cstdint>
#include <event2/event.h>
#include <event2/util.h>
#include <functional>
#include <future>
#include <infiniband/verbs.h>
#include <limits>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <string>
#include <thread>
#include <vector>
namespace nextfs::rpc {

class RpcServer {

  constexpr static uint32_t default_connection_timeout = 3000;
  constexpr static uint32_t CQ_CAPACITY = 512;

public:
  RpcServer(Config &config);
  ~RpcServer();

  [[nodiscard]] auto memory_pool() -> MemoryPool * { return memory_pool_; }
  auto set_memory_pool(MemoryPool *m) -> void;

  // start event catch thread and cq poller threads
  auto run() -> int;
  auto connect(const char *host, const char *port, uint32_t dst_node_id)
      -> void;
  auto disconnect(Connection *) -> int;
  // get a memory block where you can put your data in, the block size is equal
  // to memory_pool()->blk_size
  auto get_rpc_req_block() -> BlockInfo *;
  // send a block which you get by get_rpc_req_block to the dst_node, the size
  // of input shoud <= memory_pool()->blk_size
  // for the reason that every time you can only send a fixed size block, every
  // block you send maybe should have a header part to indicate the content
  // inside, and a protocal to pare your header
  auto send_rpc_req(uint32_t dst_node_id, BlockInfo *addr, size_t size) -> int;

  auto send_rpc_req_with_data(uint32_t dst_node_id, BlockInfo *head_blk,
                              std::pair<uint32_t, void *> data_blk,
                              uint32_t data_size) -> int;
  auto batch_post_new_recv(size_t n, int shard_id) -> void;
  auto head_mr() -> ibv_mr * { return head_mr_; };
  auto data_mr() -> ibv_mr * { return data_mr_; };

private:
  uint8_t shard_num_{1};
  uint32_t node_id_;
  uint64_t req_id_cnt_{0};
  // libevent related part
  auto handle_conn_event() -> void;
  auto handle_exit_event() -> void;
  static auto on_conn_event(evutil_socket_t fd, short what, void *arg) -> void;
  auto set_ex_meta() -> void;
  event_base *ev_base_{nullptr};
  event *conn_ev_{nullptr};
  // event *exit_ev_{nullptr};
  std::thread event_catch_thread;
  // rdma related part
  addrinfo *addr_;

  rdma_event_channel *ec_{nullptr};
  rdma_event_channel *conn_ec_{nullptr};
  // std::vector<rdma_cm_id *> cm_ids_;
  // std::vector<ibv_cq *> shared_cqs_;
  // std::vector<ibv_srq *> shared_rqs_;
  // std::vector<ibv_pd *> pds_;
  // std::vector<ibv_mr *> mrs_;
  // std::vector<ankerl::unordered_dense::map<uint16_t, Connection *>>
  //     qp_num2conns_{};
  // std::vector<ankerl::unordered_dense::map<uint16_t, uint16_t>>
  //     node_id2qp_nums_;
  // std::vector<Spinlock *> locks_;
  // std::vector<std::thread> cq_pollers_;

  rdma_cm_id *cm_id_{};
  struct Shard {
    ibv_cq *shared_cq_{};
    ibv_cq *shared_send_cq_{};
    ibv_srq *shared_rq_{};
  };
  std::vector<Shard> shards_;
  ibv_pd *pd_{};
  ibv_mr *head_mr_{};
  ibv_mr *data_mr_{};
  std::vector<std::thread> cq_pollers_;

  ankerl::unordered_dense::map<uint32_t, std::vector<Connection *>>
      node_id2conns_;
  Spinlock lock_{};

  std::atomic_bool is_running_{false};
  rdma_conn_param param_{};
  auto poll(int shard_id) -> void;
  auto get_conn(uint32_t dst_node_id) -> Connection *;
  auto on_recv_req(ibv_wc &wc, int shard) -> void;
  auto on_send_req(ibv_wc &wc) -> void;
  auto registor_conn(Connection *conn) -> void;
  auto deregistor_conn(Connection *conn) -> void;

  auto on_cm_connect_req(rdma_cm_id *listen_id, rdma_cm_id *client_cm_id,
                         RdmaExchangeMeta *ex_meta) -> void;
  auto on_cm_establish_con(rdma_cm_id *client_cm_id) -> void;
  auto on_cm_disconnect(rdma_cm_id *listen_id, rdma_cm_id *client_cm_id)
      -> void;
  MemoryPool *memory_pool_{nullptr};
  auto handle_forward_req(ibv_wc &wc) -> void;
  auto wait_event_and_ack(rdma_cm_event_type expected) -> rdma_cm_event *;
};

class RpcConnectionCtx {
public:
  RpcConnectionCtx(BlockInfo *b, BlockInfo *myself)
      : b_(b), myself_(myself),
        data_blk_(std::numeric_limits<uint32_t>::max(), nullptr) {}
  // ~RpcConnectionCtx() = default;
  ~RpcConnectionCtx() {
    delete (b_);
    delete (myself_);
    // free the data_addr ??
  }

public:
  BlockInfo *b_;
  BlockInfo *myself_;
  std::pair<uint32_t, void *> data_blk_; // used for data
  // uint64_t send_ts_;
};
} // namespace nextfs::rpc