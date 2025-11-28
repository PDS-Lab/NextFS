#include "rpc/server.hh"
#include "common/concurrent_ring.hh"
#include "common/config.hh"
#include "common/logger.hh"
#include "common/memory_pool.hh"
#include "common/slab.hh"
#include "common/spinlock.hh"
#include "common/util.hh"
#include "daemon/env.hh"
#include "rpc/connection.hh"
#include "rpc/handler.hh"
#include "rpc/message.hh"
#include <algorithm>
#include <array>
#include <atomic>
#include <cassert>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <event2/event.h>
#include <event2/thread.h>
#include <fmt/core.h>
#include <functional>
#include <infiniband/verbs.h>
#include <malloc.h>
#include <mutex>
#include <netdb.h>
#include <netinet/in.h>
#include <numa.h>
#include <rdma/rdma_cma.h>
#include <signal.h>
#include <spdlog/common.h>
#include <spdlog/spdlog.h>
#include <string>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

namespace nextfs::rpc {

RpcServer::RpcServer(Config &config) {
  assert(config.rpc_mempool_blk_size_ >= sizeof(RpcConnectionCtx));
  int ret = 0;
  shard_num_ = config.rpc_shard_num_;
  if (memory_pool_ == nullptr)
    memory_pool_ = MemoryPool::create_mempool(config.rpc_mempool_blk_size_,
                                              config.rpc_mempool_capacity_);
  checkp(memory_pool_, "failed to create memory pool");

  node_id_ = config.node_id_;
  ec_ = rdma_create_event_channel();
  checkp(ec_, "failed to create event channel");
  conn_ec_ = rdma_create_event_channel();
  checkp(ec_, "failed to create conn event channel");
  info("initializing channel and identifier");

  auto init_func = [&](std::string &server_ip) {
    ret = getaddrinfo(server_ip.c_str(), config.rpc_port_.c_str(), nullptr,
                      &addr_);
    check(ret, "failed to parse host and port for {}", server_ip);
    info("listening address at {}:{}", server_ip, config.rpc_port_);
    ret = rdma_create_id(ec_, &cm_id_, nullptr, RDMA_PS_TCP);
    check(ret, "fail to create cm id");
    ret = rdma_bind_addr(cm_id_, addr_->ai_addr);
    check(ret, "failed to bind address with cm id");

    ret = rdma_listen(cm_id_, 8);
    check(ret, "failed to listen at cm_id");

    pd_ = ibv_alloc_pd(cm_id_->verbs);
    checkp(pd_, "failed to alloc pd");

    for (int i = 0; i < shard_num_; i++) {
      auto shard_cq = ibv_create_cq(cm_id_->verbs, 1024, this, nullptr, 0);
      checkp(shard_cq, "failed to create cq");
      auto shared_send_cq =
          ibv_create_cq(cm_id_->verbs, 1024, this, nullptr, 0);
      checkp(shared_send_cq, "failed to create send cq");
      struct ibv_srq_init_attr srq_init_attr;
      memset(&srq_init_attr, 0, sizeof(srq_init_attr));
      srq_init_attr.attr.max_wr = 4096;
      srq_init_attr.attr.max_sge = 2;
      auto shared_rq = ibv_create_srq(pd_, &srq_init_attr);
      checkp(shared_rq, "failed to create srq");
      shards_.push_back(Shard{shard_cq, shared_send_cq, shared_rq});
    }

    head_mr_ =
        ibv_reg_mr(pd_, memory_pool_->addr(), memory_pool_->size(),
                   IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
                       IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ);

    data_mr_ = ibv_reg_mr(
        pd_, GlobalEnv::block_pool()->addr(), GlobalEnv::block_pool()->size(),
        IBV_ACCESS_LOCAL_WRITE | IBV_ACCESS_REMOTE_WRITE |
            IBV_ACCESS_REMOTE_ATOMIC | IBV_ACCESS_REMOTE_READ);

    for (int i = 0; i < shard_num_; i++) {
      batch_post_new_recv(512 / shard_num_, i);
    }
    freeaddrinfo(addr_);
  };

  init_func(config.self_ip_);

  evthread_use_pthreads();
  ev_base_ = event_base_new();
  checkp(ev_base_, "failed to create event base");
  conn_ev_ =
      event_new(ev_base_, ec_->fd, EV_READ | EV_PERSIST, &on_conn_event, this);
  checkp(conn_ev_, "failed to create connectionevent");

  ret = event_add(conn_ev_, nullptr);
  check(ret, "failed to registor connection event");
  set_ex_meta();
  info("initialization of RPC Server complete");
}

auto RpcServer::run() -> int {

  is_running_.store(true, std::memory_order_release);
  for (int i = 0; i < shard_num_; i++) {
    cq_pollers_.emplace_back(&RpcServer::poll, this, i);
  }

  event_catch_thread = std::thread([&]() { event_base_dispatch(ev_base_); });
  return 0;
}

auto RpcServer::poll(int shard_id) -> void {
  if (GlobalEnv::config().rpc_numa_bind_ != -1) {
    numa_run_on_node(GlobalEnv::config().rpc_numa_bind_);
    std::this_thread::yield();
    numa_hint = GlobalEnv::config().rpc_numa_bind_;
  }
  ibv_wc wc[CQ_CAPACITY];
  while (is_running_.load(std::memory_order_acquire)) {
    if (unlikely(shards_[shard_id].shared_cq_ == nullptr))
      continue;
    auto ret = ibv_poll_cq(shards_[shard_id].shared_cq_, CQ_CAPACITY, wc);
    if (ret < 0) {
      warn("poll cq error");
      return;
    }
    for (int j = 0; j < ret; j++) {
      if (unlikely(wc[j].status != IBV_WC_SUCCESS)) {
        warn("work element failed:{},{}", (int)wc[j].status, (int)wc[j].opcode);
        continue;
      }
      if (likely(wc[j].opcode == IBV_WC_RECV)) {
        on_recv_req(wc[j], shard_id);
      } else {
        warn("unexpected IBV WC type {}", (int)wc[j].opcode);
      }
    }

    if (ret > 0) {
      batch_post_new_recv(ret, shard_id);
    }

    ret = ibv_poll_cq(shards_[shard_id].shared_send_cq_, CQ_CAPACITY, wc);
    if (ret < 0) {
      warn("poll send cq error");
      return;
    }
    for (int j = 0; j < ret; j++) {
      if (unlikely(wc[j].status != IBV_WC_SUCCESS)) {
        warn("work element failed:{},{}", (int)wc[j].status, (int)wc[j].opcode);
        continue;
      }
      if (likely(wc[j].opcode == IBV_WC_SEND)) {
        on_send_req(wc[j]);
      } else {
        warn("unexpected IBV WC type {}", (int)wc[j].opcode);
      }
    }
  }
}

auto RpcServer::registor_conn(Connection *conn) -> void {
  std::lock_guard<Spinlock> l(lock_);
  node_id2conns_[conn->dst_node_id()].push_back(conn);
}

auto RpcServer::deregistor_conn(Connection *conn) -> void {
  std::lock_guard<Spinlock> l(lock_);
  auto &conns = node_id2conns_[conn->dst_node_id()];
  auto it = std::find(conns.begin(), conns.end(), conn);
  if (it != conns.end()) {
    conns.erase(it);
  }
}

auto RpcServer::on_recv_req(ibv_wc &wc, int shard) -> void {
  auto ctx = reinterpret_cast<rpc::RpcConnectionCtx *>(wc.wr_id);
  auto is_last_packet = wc.imm_data >> 31;

  // maybe should check out if I am server
  // auto conn = get_conn_by_qp_num(wc.qp_num);

  auto result_head_blk = ctx->b_;
  auto result_data_blk = ctx->data_blk_;
  memory_pool_->put_one_block(ctx->myself_);

  // maybe forward
  uint32_t dst_node_id = (uint32_t)wc.imm_data;
  if (dst_node_id != GlobalEnv::node_id()) {
    // forward
    uint16_t dst_group_id = get_group_id(dst_node_id);
    uint16_t dst_server_id = get_server_id(dst_node_id);
    if (dst_server_id != GlobalEnv::server_id()) {
      warn("rpc switch node error");
    }
    trace("forward to group {} : server {}", dst_group_id, dst_server_id);
    size_t data_size =
        wc.byte_len >= msg_head_size ? wc.byte_len - msg_head_size : 0;
    GlobalEnv::rpc()->send_rpc_req_with_data(dst_node_id, result_head_blk,
                                             result_data_blk, data_size);
  }

  // handle_recvd_result(result_blk);
  if (GlobalEnv::rpc_thread_pool(shard) == nullptr) {
    warn("the thread pool is not initialized");
    handle_recvd_result({result_head_blk, result_data_blk});
  } else {
    GlobalEnv::rpc_thread_pool(shard)->enqueue(
        {result_head_blk, result_data_blk});
  }
  // memory_pool_->put_one_block(result_blk);
}

auto RpcServer::on_send_req(ibv_wc &wc) -> void {
  auto ctx = reinterpret_cast<rpc::RpcConnectionCtx *>(wc.wr_id);
  if (ctx->data_blk_.first != std::numeric_limits<uint32_t>::max()) {
    GlobalEnv::block_pool()->free(ctx->data_blk_.first);
  }
  // static uint64_t total = 0;
  // static uint64_t cnt = 0;
  // total += get_timestamp() - ctx->send_ts_;
  // cnt++;
  // if (cnt == 20000) {
  //   warn("average rpc send time:{} us", total / cnt / 1000);
  //   cnt = 0;
  //   total = 0;
  // }
  memory_pool_->put_one_block(ctx->b_);
  memory_pool_->put_one_block(ctx->myself_);
  trace("rdma send wrs complete");
}

RpcServer::~RpcServer() {
  for (auto [_, conns] : node_id2conns_) { // copy vec
    for (auto conn : conns) {
      disconnect(conn);
    }
  }
  if (event_catch_thread.joinable()) {
    //  handle_exit_event();
    int ret = event_base_loopbreak(ev_base_); // this function is reentrant
    check(ret, "failed to break event loop");
    event_catch_thread.join();
  }
  is_running_.store(false, std::memory_order_release);
  for (auto &t : cq_pollers_) {
    if (t.joinable()) {
      t.join();
    }
  }

  event_base_free(ev_base_);
  event_free(conn_ev_);

  info("cleanup the local resources");
  for (auto &shard : shards_) {
    ibv_destroy_cq(shard.shared_cq_);
    ibv_destroy_cq(shard.shared_send_cq_);
    ibv_destroy_srq(shard.shared_rq_);
  }
  ibv_dereg_mr(head_mr_);
  ibv_dereg_mr(data_mr_);
  ibv_dealloc_pd(pd_);
  rdma_destroy_id(cm_id_);
  rdma_destroy_event_channel(ec_);
  rdma_destroy_event_channel(conn_ec_);
  if (memory_pool_)
    delete memory_pool_;
}

auto RpcServer::on_conn_event([[gnu::unused]] int fd,
                              [[gnu::unused]] short what, void *arg) -> void {
  static_cast<RpcServer *>(arg)->handle_conn_event();
}

auto RpcServer::on_cm_connect_req(rdma_cm_id *listen_id,
                                  rdma_cm_id *client_cm_id,
                                  RdmaExchangeMeta *ex_meta) -> void {
  int shard_id;
  {
    std::lock_guard<Spinlock> l(lock_);
    shard_id = node_id2conns_[ex_meta->remote_node_id_].size();
  }
  info("handle a connection request");
  client_cm_id->recv_cq = shards_[shard_id].shared_cq_;
  client_cm_id->send_cq = shards_[shard_id].shared_send_cq_;
  client_cm_id->srq = shards_[shard_id].shared_rq_;

  auto init_attr = default_qp_init_attr();
  init_attr.recv_cq = shards_[shard_id].shared_cq_;
  init_attr.send_cq = shards_[shard_id].shared_send_cq_;
  init_attr.srq = shards_[shard_id].shared_rq_;
  auto qp = ibv_create_qp(pd_, &init_attr);
  checkp(qp, "failed to create qp");
  client_cm_id->qp = qp;
  auto conn = new Connection(qp, node_id_, ex_meta->remote_node_id_, nullptr);
  client_cm_id->context = conn;

  auto ret = rdma_accept(client_cm_id, nullptr);
  check(ret, "failed to accept connection");
  info("server accepted a connection request. alloc shard {}", shard_id);
  info("on_cm_connect_req() over");
}

auto RpcServer::on_cm_establish_con(rdma_cm_id *cm_id) -> void {
  auto conn = reinterpret_cast<Connection *>(cm_id->context);
  registor_conn(conn);
  struct ibv_qp_attr qp_attr;
  struct ibv_qp_init_attr init_attr;
  int r = ibv_query_qp(conn->qp(), &qp_attr, IBV_QP_STATE, &init_attr);
  check(r, "failed to query qp state");
  warn("qp state: {}", (int)qp_attr.qp_state);
  info("connection established");
}

auto RpcServer::on_cm_disconnect(rdma_cm_id *listen_id,
                                 rdma_cm_id *client_cm_id) -> void {
  info("handle a disconnect request");

  auto conn = reinterpret_cast<Connection *>(client_cm_id->context);
  deregistor_conn(conn);
  rdma_destroy_id(client_cm_id);
  delete conn;
}

auto RpcServer::handle_conn_event() -> void {

  info("handle_conn_event");

  rdma_cm_event *tmp_ev;
  int ret;
  ret = rdma_get_cm_event(ec_, &tmp_ev);
  check(ret, "failed to get cm event from cm channel");
  if (tmp_ev->status != 0) {
    warn("handle_conn_event, got a bad event");
    warn("{}", rdma_event_str(tmp_ev->event));

    if (rdma_ack_cm_event(tmp_ev) != 0) {
      warn("failed to ack a bad event");
    }
    return;
  }
  auto client_cm_id = tmp_ev->id;
  ret = rdma_ack_cm_event(tmp_ev);
  switch (tmp_ev->event) {
  case RDMA_CM_EVENT_CONNECT_REQUEST: {
    auto ex_meta = (RdmaExchangeMeta *)(tmp_ev->param.conn.private_data);
    on_cm_connect_req(tmp_ev->listen_id, client_cm_id, ex_meta);
    break;
  }
  case RDMA_CM_EVENT_ESTABLISHED: {
    on_cm_establish_con(client_cm_id);
    check(ret, "failed to ack connection established event");
    break;
  }
  case RDMA_CM_EVENT_DISCONNECTED: {
    on_cm_disconnect(tmp_ev->listen_id, client_cm_id);
    check(ret, "failed to ack disconnect event");
    info("ack disconnect");
    break;
  }
  default: {
    info("receive unexpected rdma cm event {}", rdma_event_str(tmp_ev->event));
    break;
  }
  }
}

auto RpcServer::wait_event_and_ack(rdma_cm_event_type expected)
    -> rdma_cm_event * {
  rdma_cm_event *ev_{nullptr};
  int ret = rdma_get_cm_event(conn_ec_, &ev_);
  check(ret, "failed to get cm event");
  if (ev_->status != 0) {
    warn("wait_event_and_ack, got a bad event");
    warn("{}", rdma_event_str(ev_->event));

    if (rdma_ack_cm_event(ev_) != 0) {
      warn("failed to ack a bad event");
    }
    return nullptr;
  }
  if (ev_->event == expected) {
    rdma_ack_cm_event(ev_);
  } else {
    warn("expected to receive {} event, but got {}", rdma_event_str(expected),
         rdma_event_str(ev_->event));
    return nullptr;
  }
  return ev_;
}
auto RpcServer::set_ex_meta() -> void {
  struct RdmaExchangeMeta *ex_meta;
  ex_meta = new RdmaExchangeMeta{node_id_};
  // conn_param can be used to exchange meta data
  memset(&param_, 0, sizeof(param_));
  param_.responder_resources = 16;
  param_.initiator_depth = 16;
  param_.retry_count = 7;
  param_.rnr_retry_count = 7; // '7' indicates retry infinitely
  param_.private_data = reinterpret_cast<RdmaExchangeMeta *>(ex_meta);
  param_.private_data_len = sizeof(RdmaExchangeMeta);
  info("initialize connection ex parameters");
}

auto RpcServer::connect(const char *dst_host, const char *dst_port,
                        uint32_t dst_node_id) -> void {
  info("start connect to {}:{}", dst_host, dst_port);

  // check if the connection has already established
  {
    std::lock_guard<Spinlock> l(lock_);
    auto &conns = node_id2conns_[dst_node_id];
    if (!conns.empty()) {
      warn("connection between {} and {} has already established", node_id_,
           dst_node_id);
      return;
    }
  }
  int ret = 0;
  addrinfo *addr;
  ret = getaddrinfo(dst_host, dst_port, nullptr, &addr);
  for (int i = 0; i < shard_num_; i++) {
    rdma_cm_event *ev_{nullptr};
    rdma_cm_id *cm_id;
    ret = rdma_create_id(conn_ec_, &cm_id, nullptr, RDMA_PS_TCP);
    check(ret, "failed to create cm id when connect");

    check(ret, "failed to parse host and port");
    ret = rdma_resolve_addr(cm_id, nullptr, addr->ai_addr,
                            default_connection_timeout);

    check(ret, "fail to resolve address");
    ev_ = wait_event_and_ack(RDMA_CM_EVENT_ADDR_RESOLVED);
    checkp(ev_, "failed to RDMA resolve address event ");

    ret = rdma_resolve_route(cm_id, default_connection_timeout);
    check(ret, "failed to resolve route");
    ev_ = wait_event_and_ack(RDMA_CM_EVENT_ROUTE_RESOLVED);
    checkp(ev_, "failed to resolve route event ");

    cm_id->recv_cq = shards_[i].shared_cq_;
    cm_id->send_cq = shards_[i].shared_send_cq_;
    cm_id->srq = shards_[i].shared_rq_;

    info("allocate protection domain");

    auto init_attr = default_qp_init_attr();
    init_attr.recv_cq = shards_[i].shared_cq_;
    init_attr.send_cq = shards_[i].shared_send_cq_;
    init_attr.srq = shards_[i].shared_rq_;

    auto qp = ibv_create_qp(pd_, &init_attr);

    checkp(qp, "failed to create qp: {}", strerror(errno));
    cm_id->qp = qp;
    auto conn = new Connection(qp, node_id_, dst_node_id, cm_id);
    cm_id->context = conn;
    checkp(conn, "failed to new Connection class");
    info("Connection constructed");
    ret = rdma_connect(cm_id, &param_);

    check(ret, "failed to connect the remote side");
    ev_ = wait_event_and_ack(RDMA_CM_EVENT_ESTABLISHED);
    checkp(ev_, "failed to get event establish event ");

    info("establish the connection");
    registor_conn(conn);
    struct ibv_qp_attr qp_attr;
    struct ibv_qp_init_attr query_init_attr;
    int r = ibv_query_qp(conn->qp(), &qp_attr, IBV_QP_STATE, &query_init_attr);
    check(r, "failed to query qp state");
    warn("qp state: {}", (int)qp_attr.qp_state);
  }
  freeaddrinfo(addr);
  return;
}

auto RpcServer::disconnect(Connection *conn) -> int {
  int ret = 0;
  info("disconnecting from {}", conn->dst_node_id());
  if (conn != nullptr) {
    deregistor_conn(conn);
    auto cm_id = conn->cm_id();
    checkp(cm_id, "RPC server disconnect initiatively");
    ret = rdma_disconnect(cm_id);
    check(ret, "failed to disconnect node{}", conn->dst_node_id());
    wait_event_and_ack(RDMA_CM_EVENT_DISCONNECTED);
    auto qp = conn->qp();
    delete conn;
    ret = ibv_destroy_qp(qp);
    check(ret, "fail to destroy qp");
    rdma_destroy_id(cm_id);
    return 0;
  }
  return -1;
};

auto RpcServer::get_conn(uint32_t dst_node_id) -> Connection * {
  // ! NO LOCK HERE (UNSAFE)
  static std::atomic_uint64_t r = std::hash<uint32_t>{}(node_id_);
  int shard_id = r.fetch_add(1, std::memory_order_relaxed) % shard_num_;
  auto &conns = node_id2conns_[dst_node_id];
  auto conn =
      unlikely(conns.empty()) ? nullptr : conns[shard_id % conns.size()];
  return conn;
}

auto RpcServer::get_rpc_req_block() -> BlockInfo * {
  return memory_pool_->get_one_block();
}

auto RpcServer::send_rpc_req(uint32_t dst_node_id, BlockInfo *blk, size_t size)
    -> int {
  // maybe should check the obj
  if (size > memory_pool_->blk_size()) {
    warn("send size is larger than mempool block size");
    return 0;
  }
  auto blk_ctx = memory_pool()->get_one_block();
  // info("use:{:x}", (uintptr_t)blk->addr_);

  auto *ctx = new (blk_ctx->addr_) RpcConnectionCtx(blk, blk_ctx);

  uint32_t next_node_id = dst_node_id;

  uint16_t dst_group_id = get_group_id(dst_node_id);
  uint16_t dst_server_id = get_server_id(dst_node_id);
  if (dst_group_id != GlobalEnv::group_id() &&
      dst_server_id != GlobalEnv::server_id()) {
    // forward to the same server_id node of the target node
    next_node_id = get_node_id(GlobalEnv::group_id(), dst_server_id);
  };

  auto conn = get_conn(next_node_id);

  checkp(conn, "get_conn() failed");

  conn->post_send(ctx, blk->addr_, size, head_mr()->lkey, dst_node_id, 1);
  return 0;
}

auto RpcServer::send_rpc_req_with_data(uint32_t dst_node_id,
                                       BlockInfo *head_blk,
                                       std::pair<uint32_t, void *> data_blk,
                                       uint32_t data_size) -> int {
  auto blk_ctx = memory_pool()->get_one_block();
  // info("use:{:x}", (uintptr_t)blk->addr_);

  auto *ctx = new (blk_ctx->addr_) RpcConnectionCtx(head_blk, blk_ctx);
  ctx->data_blk_ = data_blk;

  uint32_t next_node_id = dst_node_id;

  uint16_t dst_group_id = get_group_id(dst_node_id);
  uint16_t dst_server_id = get_server_id(dst_node_id);
  if (dst_group_id != GlobalEnv::group_id() &&
      dst_server_id != GlobalEnv::server_id()) {
    // forward to the same server_id node of the target node
    next_node_id = get_node_id(GlobalEnv::group_id(), dst_server_id);
  };

  auto conn = get_conn(next_node_id);
  // info("mr:{:x}", (uintptr_t)conn->mr()->addr);
  // info("mr_end:{:x}",
  //     (uintptr_t)conn->mr()->addr + (uintptr_t)conn->mr()->length);

  checkp(conn, "get_conn() failed");

  conn->post_send_with_data(ctx, head_blk->addr_, msg_head_size,
                            head_mr()->lkey, dst_node_id, data_blk.second,
                            data_size, data_mr()->lkey);
  return 0;
}

auto RpcServer::batch_post_new_recv(size_t n, int shard_id) -> void {
  thread_local uint64_t round = 0;
  uint64_t hint = round;
  struct ibv_sge sge_lists[2 * QUEUE_SIZE];
  struct ibv_recv_wr wrs[QUEUE_SIZE];
  for (size_t i = 0; i < n; i++) {
    auto blk = memory_pool_->get_one_block(hint);
    auto head_blk = memory_pool()->get_one_block(hint);
    auto *ctx = new (blk->addr_) RpcConnectionCtx(head_blk, blk);
    ctx->data_blk_ = GlobalEnv::block_pool()->alloc<void>(hint).value();
    sge_lists[i * 2] = {
        (uint64_t)head_blk->addr_,
        GlobalEnv::config().rpc_recv_head_size_,
        head_mr_->lkey,
    };
    sge_lists[i * 2 + 1] = {
        (uint64_t)ctx->data_blk_.second,
        GlobalEnv::config().block_size_,
        data_mr_->lkey,
    };
    wrs[i] = {
        (uintptr_t)ctx,
        i == n - 1 ? nullptr : &wrs[i + 1],
        &sge_lists[i * 2],
        2,
    };
    hint++;
  }

  ibv_recv_wr *bad = nullptr;
  // only server post recv
  auto ret = ibv_post_srq_recv(shards_[shard_id].shared_rq_, &wrs[0], &bad);
  check(ret, "fail to post recv");
  round += n;
}

}; // namespace nextfs::rpc
