#include "daemon/env.hh"
#include "daemon/ipc_receiver.hh"
#include "interceptor/nextfs.hh"
#include "interceptor/preload.hh"
#include "iostream"
void test_openat(const char *pathname);
void test_mkdir(const char *pathname);
void test_close(const int fd);
void test_rename(const char *oldpathname, const char *newpathname);
void test_stat(const char *pathname);
void test_read(int fd, char *buf, size_t count);
void test_write(int fd, char *buf, size_t count);
void test_lseek(int fd, ssize_t offset, int whence);
void *ipc_receiver(void *args);
void test_unlink(const char *pathname);

int main() {
  // int fd = open("dummy", O_CREAT | O_RDWR, 0655)
  nextfs::info("test begin");
  std::cout << "connection begin" << std::endl;
  int ret = nextfs::GlobalEnv::init();
  if (ret != -1)
    nextfs::info("GlobalEnv::init() success");
  else
    std::cout << "GlobalEnv::init() failed" << std::endl;

  ret = nextfs::PreloadContext::init();
  if (ret != -1)
    nextfs::info("PreloadContext::init() success");
  else
    std::cout << "PreloadContext::init() failed" << std::endl;

  nextfs::GlobalEnv::instance().rpc_->run();

  sleep(3); // wait other nodes start
  std::cout << "sleep over!" << std::endl;
  if (nextfs::GlobalEnv::instance().node_id() == 1) {
    auto &env = nextfs::GlobalEnv::instance();
    for (auto &v : env.config().in_group_nodes_) {
      uint32_t id = std::get<0>(v);
      string ip = std::get<1>(v);
      string port = std::get<2>(v);
      env.rpc_->connect(ip.c_str(), port.c_str(), id);
    }
    std::cout << "connect!" << std::endl;
    pthread_t ipc_receiver_t;
    pthread_create(&ipc_receiver_t, NULL, ipc_receiver, NULL);
    string op;
    do {
      std::cout << "wait op" << std::endl;
      std::cin >> op;
      if (op == "mkdir") {
        char filepath[20];
        scanf("%s", filepath);
        test_mkdir(filepath);
      } else if (op == "open") {
        char filepath[20];
        scanf("%s", filepath);
        test_openat(filepath);
      } else if (op == "close") {
        int fd;
        std::cin >> fd;
        test_close(fd);
      } else if (op == "rename") {
        char oldfilepath[20];
        char newfilepath[20];
        scanf("%s", oldfilepath);
        scanf("%s", newfilepath);
        test_rename(oldfilepath, newfilepath);
      } else if (op == "stat") {
        char filepath[20];
        scanf("%s", filepath);
        test_stat(filepath);
      } else if (op == "lseek") {
        int fd, offset;
        std::cin >> fd >> offset;
        string whence;
        std::cin >> whence;
        std::cout << fd << "   " << offset << "    " << whence << std::endl;
        if (whence == "seek_set") {
          test_lseek(fd, offset, SEEK_SET);
        } else if (whence == "seek_cur") {
          test_lseek(fd, offset, SEEK_CUR);
        } else {
          std::cout << "unknow_lseek" << std::endl;
        }
      } else if (op == "read") {
        // int fd, offset, count;
        // std::cin >> fd >> offset >> count;
        int fd, count;
        std::cin >> fd >> count;
        char *buf = (char *)malloc(count + 1);
        test_read(fd, buf, count);
      } else if (op == "write") {
        int fd, count;
        std::cin >> fd >> count;
        char *buf = (char *)malloc(count + 1);
        test_write(fd, buf, count);
      } else if (op == "unlink") {
        char filepath[20];
        scanf("%s", filepath);
        test_unlink(filepath);
      }
    } while (op != "over");

  } else {
    pthread_t ipc_receiver_t;
    pthread_create(&ipc_receiver_t, NULL, ipc_receiver, NULL);
    string op;
    do {
      std::cout << "wait op" << std::endl;
      std::cin >> op;
      if (op == "mkdir") {
        char filepath[20];
        scanf("%s", filepath);
        test_mkdir(filepath);
      } else if (op == "open") {
        char filepath[20];
        scanf("%s", filepath);
        test_openat(filepath);
      } else if (op == "close") {
        int fd;
        std::cin >> fd;
        test_close(fd);
      } else if (op == "rename") {
        char oldfilepath[20];
        char newfilepath[20];
        scanf("%s", oldfilepath);
        scanf("%s", newfilepath);
        test_rename(oldfilepath, newfilepath);
      } else if (op == "stat") {
        char filepath[20];
        scanf("%s", filepath);
        test_stat(filepath);
      } else if (op == "lseek") {
        int fd, offset;
        std::cin >> fd >> offset;
        string whence;
        std::cin >> whence;
        std::cout << fd << "   " << offset << "    " << whence << std::endl;
        if (whence == "seek_set") {
          test_lseek(fd, offset, SEEK_SET);
        } else if (whence == "seek_cur") {
          test_lseek(fd, offset, SEEK_CUR);
        } else {
          std::cout << "unknow_lseek" << std::endl;
        }
      } else if (op == "read") {
        // int fd, offset, count;
        // std::cin >> fd >> offset >> count;
        int fd, count;
        std::cin >> fd >> count;
        char *buf = (char *)malloc(count + 1);
        test_read(fd, buf, count);
      } else if (op == "write") {
        int fd, count;
        std::cin >> fd >> count;
        char *buf = (char *)malloc(count + 1);
        test_write(fd, buf, count);
      } else if (op == "unlink") {
        char filepath[20];
        scanf("%s", filepath);
        test_unlink(filepath);
      }
    } while (op != "over");
  }
  pthread_exit(NULL);
  return 0;
}
void test_openat(const char *pathname) {
  std::cout << pathname << std::endl;
  auto ret = nextfs::nextfs_openat(pathname, 0, 0);
  if (ret == -1) {
    std::cout << "打开/创建失败!" << std::endl;
  } else {
    std::cout << "打开/创建成功!" << std::endl;
  }
}
void test_close(const int fd) {
  std::cout << fd << std::endl;
  auto ret = nextfs::nextfs_close(fd);
  if (ret == -1) {
    std::cout << "关闭失败!" << std::endl;
  } else {
    std::cout << "关闭成功!" << std::endl;
  }
}
void test_mkdir(const char *pathname) {
  std::cout << pathname << std::endl;
  auto ret = nextfs::nextfs_mkdirat(pathname, 0);
  if (ret == -1) {

    std::cout << "创建失败!" << std::endl;
  } else {

    std::cout << "创建成功/已经存在!" << std::endl;
  }
}
void test_rename(const char *oldpathname, const char *newpathname) {

  auto ret = nextfs::nextfs_rename(oldpathname, newpathname);
  if (ret == -1) {
    std::cout << "rename失败!" << std::endl;
  } else {
    std::cout << "rename成功!" << std::endl;
  }
}
void test_stat(const char *pathname) {
  auto ret = nextfs::nextfs_stat(pathname);
  if (ret == -1) {
    std::cout << "stat失败!" << std::endl;
  } else {
    std::cout << "stat成功!" << std::endl;
  }
}
void test_read(int fd, char *buf, size_t count) {
  auto ret = nextfs::nextfs_read(fd, buf, count);
  if (ret == -1) {
    std::cout << "read 失败!" << std::endl;
  } else {
    std::cout << "read 成功!" << std::endl;
    std::cout << "output read message:";
    buf[count] = '\0';
    std::cout << buf << std::endl;
  }
}
void test_write(int fd, char *buf, size_t count) {
  std::cout << "input write message:";
  std::cin >> buf;
  auto ret = nextfs::nextfs_write(fd, buf, count);
  if (ret == -1) {
    std::cout << "write 失败!" << std::endl;
  } else {
    std::cout << "write 成功!" << std::endl;
  }
}
void test_lseek(int fd, ssize_t offset, int whence) {
  auto ret = nextfs::nextfs_lseek(fd, offset, whence);
  if (ret == -1) {
    std::cout << "lseek 失败!" << std::endl;
  } else {
    std::cout << "lseek 成功!" << std::endl;
  }
}
void *ipc_receiver(void *args) {
  auto &daemon_env = nextfs::GlobalEnv::instance();
  auto ipc_thread_pool = daemon_env.ipc_thread_pool();
  static nextfs::IpcMessageInfo infos[10];
  while (1) {
    size_t n = nextfs::poll_ipc_message(infos, 10);
    for (int i = 0; i < n; i++) {
      std::cout << (int)(infos[i].type_) << std::endl;
      // ipc_thread_pool->enqueue(nextfs::ipc_msg_handler, infos[i]);
      std::cout << "pool_index_" << infos[i].pool_index_ << std::endl;
      ipc_msg_handler(infos[i]);
    }
  }
  pthread_exit(NULL);
}
void test_unlink(const char *pathname) {
  std::cout << pathname << std::endl;
  auto ret = nextfs::nextfs_unlink(pathname);
  if (ret == -1) {
    std::cout << "删除失败!" << std::endl;
  } else {
    std::cout << "删除成功!" << std::endl;
  }
}
// auto &env = nextfs::GlobalEnv::instance();
// for (auto &v : env.config().in_group_nodes_) {
//   uint32_t id = std::get<0>(v);
//   string ip = std::get<1>(v);
//   string port = std::get<2>(v);
//   env.rpc_->connect(ip.c_str(), port.c_str(), id);
// }