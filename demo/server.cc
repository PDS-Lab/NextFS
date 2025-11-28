#include "rpc/server.hh"
#include "common/logger.hh"
#include "daemon/env.hh"
#include <iostream>
#include <spdlog/common.h>

auto main(int argc, char **argv) -> int {

  // if (argc != 3) {
  //   std::cerr << "Usage: " << argv[0] << " <address> <provider_id>"
  //             << std::endl;
  //   exit(0);
  // }
  // nextfs::GlobalEnv::init();

  nextfs::GlobalLogger::init(spdlog::level::trace);
  nextfs::Config config;
  config.node_id_ = 1;
  config.rpc_mempool_blk_size_ = 512;
  config.rpc_mempool_capacity_ = 100 * 4096;
  config.self_ips_.push_back("192.168.200.33");
  config.rpc_port_ = "40000";

  nextfs::rpc::RpcServer server(config);
  server.run();
  // sleep(2);
  while (1) {
  }
  return 0;
}