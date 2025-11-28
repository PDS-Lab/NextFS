#include "daemon/daemon.hh"
#include "common/concurrent_ring.hh"
#include "common/config.hh"
#include "common/ipc_message.hh"
#include "common/logger.hh"
#include "common/thread_pool.hh"
#include "common/util.hh"
#include "daemon/aio.hh"
#include "daemon/env.hh"
#include "daemon/ipc_receiver.hh"
#include <csignal>
#include <numa.h>
#include <thread>

auto destroy(int signum) -> void {
  nextfs::info("shutdown signal {} received", strsignal(signum));
  auto &daemon_env = nextfs::GlobalEnv::instance();
  daemon_env.stop();
}

auto main(int argc, char **argv) -> int {
  auto &daemon_env = nextfs::GlobalEnv::instance();

  int ret = 0, counter = 0;
  nextfs::GlobalEnv::init();
  if (nextfs::GlobalEnv::config().hot_spot_cache_node_id_ ==
      nextfs::GlobalEnv::node_id()) {
    // TODO
    // hot-spot-cache
  }

  // start the cluster
  nextfs::GlobalEnv::rpc()->run();
  sleep(5); // wait other nodes start

  // connect to in_group_nodes
  for (auto &v : daemon_env.config().in_group_nodes_) {
    uint32_t node_id = std::get<0>(v);
    // spdlog::info("con node_id : {}", daemon_env.node_id());
    // spdlog::info("tar node_id : {}", node_id);
    string ip = std::get<1>(v);
    string port = std::get<2>(v);
    if (node_id <= daemon_env.node_id()) {
      continue;
    }
    daemon_env.rpc()->connect(ip.c_str(), port.c_str(), node_id);
  }

  // connect to out_group_nodes
  for (auto &v : daemon_env.config().out_group_peer_nodes_) {
    uint32_t node_id = std::get<0>(v);
    uint16_t group_id = nextfs::get_group_id(node_id);
    uint16_t server_id = nextfs::get_server_id(node_id);
    string ip = std::get<1>(v);
    string port = std::get<2>(v);

    if (server_id == daemon_env.server_id() &&
        group_id > daemon_env.group_id()) {
      daemon_env.rpc()->connect(ip.c_str(), port.c_str(), node_id);
    }
  }

  // listen the ipc pool
  // auto ipc_thread_pool = daemon_env.rpc_thread_pool();
  auto ipc_thread_pool = daemon_env.ipc_thread_pool();
  std::thread t([&]() {
    if (nextfs::GlobalEnv::config().ipc_numa_bind_ != -1) {
      numa_run_on_node(nextfs::GlobalEnv::config().ipc_numa_bind_);
      std::this_thread::yield();
      nextfs::numa_hint = nextfs::GlobalEnv::config().ipc_numa_bind_;
    }
    static nextfs::IpcMessageInfo infos[100];
    while (!daemon_env.is_stopped()) {
      size_t n = nextfs::poll_ipc_message(infos, 100);
      for (int i = 0; i < n; i++) {
        ipc_thread_pool->enqueue(infos[i]);
      }
      if (n == 0) {
        counter++;
        if (counter == 1000) {
          // futex此时可能是notified状态，调用park后会回到empty状态（park不成功），因此得再调用一次park才能成功睡眠。
          // daemon_env.ipc_futex()->park();
          counter = 0;
        }
      }
    }
  });
  signal(SIGTERM, destroy);
  signal(SIGINT, destroy);
  t.join();
  return ret;
}

namespace nextfs {}
