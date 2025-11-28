
#include "common/config.hh"
#include "daemon/env.hh"
#include "rpc/message.hh"
#include "rpc/server.hh"
#include <chrono>
#include <cstdlib>
#include <cstring>
#include <future>
#include <infiniband/verbs.h>
#include <iostream>
#include <netdb.h>
#include <rdma/rdma_cma.h>
#include <sys/socket.h>
#include <thread>
#include <unistd.h>

constexpr static auto default_qp_init_attr() -> ibv_qp_init_attr {
  return {
      nullptr, // qp_context
      nullptr, // send_cq
      nullptr, // recv_cq
      nullptr, // srq
      {
          64,     // max_send_wr
          64,     // max_recv_wr
          1,      // max_send_sge
          1,      // max_recv_sge
          0,      // max_inline_data
      },          // cap
      IBV_QPT_RC, // qp_type
      0,          // sq_sig_all
  };
}
auto main(int argc, char **argv) -> int {

  if (argc != 3) {
    std::cerr << "Usage: " << argv[0] << " <address> <provider_id>"
              << std::endl;
    exit(0);
  }
  nextfs::GlobalLogger::init(spdlog::level::trace);
  // nextfs::GlobalEnv::init();

  nextfs::Config config;
  config.node_id_ = 2;
  config.rpc_mempool_blk_size_ = 4096;
  config.rpc_mempool_capacity_ = 100 * 4096;

  config.self_ips_.push_back("192.168.200.33");
  config.self_ips_.push_back("192.168.201.33");
  config.rpc_port_ = "40000";
  nextfs::rpc::RpcServer client(config);
  // client.run_and_detach();
  client.run();
  client.connect(argv[1], argv[2], 1);
  char s[21] = "void *dataf";
  std::cout << "ready to send msg\n";
  // client.send_rpc_req(uint32_t dst_node_id, void *addr, size_t size)
  auto ss = client.get_rpc_req_block();
  memcpy(ss->addr_, "void *dataf", 20);
  client.send_rpc_req(1, ss, 20);
  std::cout << "sent\n";
  sleep(1);
  client.disconnect(1);

  return 0;
}