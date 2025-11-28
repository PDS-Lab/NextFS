#include "spdlog/sinks/basic_file_sink.h"
#include "spdlog/spdlog.h"
#include <chrono>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <fcntl.h>
#include <iostream>
#include <spdlog/common.h>
#include <spdlog/logger.h>
#include <string>
#include <sys/stat.h>
#include <thread>
#include <unistd.h>
#include <vector>

bool check_buf(char *buf, int len, char c) {
  for (int i = 0; i < len; i++) {
    if (buf[i] != c) {
      return false;
    }
  }
  return true;
}

void test1() {
  std::cout << "=============== Test 1 start ===============" << std::endl;
  char read_buf[4096];
  char write_buf[4096];
  int r = mkdir("/home/ysy/nextfs/mount/test1", 0666);
  if (r < 0) {
    std::cout << "mkdir failed" << std::endl;
    return;
  }
  int fd =
      open("/home/ysy/nextfs/mount/test1/test.txt", O_CREAT | O_RDWR, 0644);
  if (fd < 0) {
    std::cout << "open file failed" << std::endl;
    return;
  }
  struct stat st;
  r = fstat(fd, &st);
  if (r < 0) {
    std::cout << "fstat failed" << std::endl;
    return;
  }
  std::cout << "file size: " << st.st_size << std::endl;
  for (int i = 0; i < 3; i++) {
    memset(write_buf, i + '0', 1000);
    r = write(fd, write_buf, 1000);
    if (r < 0) {
      std::cout << "write failed" << std::endl;
      return;
    }
  }
  lseek(fd, 0, SEEK_SET);
  for (int i = 0; i < 3; i++) {
    r = read(fd, read_buf, 1000);
    if (r < 0) {
      std::cout << "read failed" << std::endl;
      return;
    }
    if (!check_buf(read_buf, 1000, i + '0')) {
      std::cout << "read wrong data" << std::endl;
      return;
    }
  }

  std::cout << "small file write and read success" << std::endl;

  lseek(fd, 3000, SEEK_SET);
  memset(write_buf, '3', 1000); // to normal file
  r = write(fd, write_buf, 1000);
  if (r < 0) {
    std::cout << "write failed" << std::endl;
    return;
  }
  memset(write_buf, '4', 1000); // to normal file
  r = write(fd, write_buf, 1000);
  if (r < 0) {
    std::cout << "write failed" << std::endl;
    return;
  }

  // r = pread(fd, read_buf, 4000, 0);
  // if (r < 0) {
  //   std::cout << "pread failed" << std::endl;
  //   return;
  // }

  for (int i = 0; i < 5; i++) {
    r = pread(fd, read_buf, 1000, i * 1000);
    if (r < 0) {
      std::cout << "read failed" << std::endl;
      return;
    }
    if (!check_buf(read_buf, 1000, i + '0')) {
      std::cout << "read wrong data" << std::endl;
      return;
    }
  }
  close(fd);
  std::cout << "=============== Test 1 success ===============" << std::endl;
}

void test2() {
  std::cout << "=============== Test 2 start ===============" << std::endl;
  const size_t SIZE = 16000;
  char read_buf[SIZE];
  char write_buf[SIZE];
  int r = mkdir("/home/ysy/nextfs/mount/test2", 0666);
  if (r < 0) {
    std::cout << "mkdir failed" << std::endl;
    return;
  }
  int fd =
      open("/home/ysy/nextfs/mount/test2/test.txt", O_CREAT | O_RDWR, 0644);
  if (fd < 0) {
    std::cout << "open file failed" << std::endl;
    return;
  }
  struct stat st;
  r = fstat(fd, &st);
  if (r < 0) {
    std::cout << "fstat failed" << std::endl;
    return;
  }
  std::cout << "file size: " << st.st_size << std::endl;
  for (int i = 0; i < 400; i++) {
    memset(write_buf, i + '0', SIZE);
    r = write(fd, write_buf, SIZE);
    if (r < 0) {
      std::cout << "write failed" << std::endl;
      return;
    }
  }

  std::cout << "write success" << std::endl;
  std::cout << "start read" << std::endl;

  lseek(fd, 0, SEEK_SET);
  for (int i = 0; i < 400; i++) {
    r = read(fd, read_buf, SIZE);
    if (r < 0) {
      std::cout << "read failed" << std::endl;
      return;
    }
    if (!check_buf(read_buf, SIZE, i + '0')) {
      std::cout << "read wrong data" << std::endl;
      return;
    }
  }
  close(fd);
  std::cout << "=============== Test 2 success ===============" << std::endl;
}

void test3_parallel() {
  std::cout << "=============== Test 3 start ===============" << std::endl;
  int r = mkdir("/home/ysy/nextfs/mount/test2", 0666);
  if (r < 0) {
    std::cout << "mkdir failed" << std::endl;
    return;
  }

  int thread_cnt = 20;
  std::vector<std::thread> threads;

  auto thread_func = [](int i) {
    int fd = open("/home/ysy/nextfs/mount/test2/test.txt",
                  O_CREAT | O_RDWR | O_EXCL, 0644);
    if (fd < 0) {
      std::string msg = "thread " + std::to_string(i) + " open file failed";
      std::cout << msg << std::endl;
    } else {
      std::string msg = "thread " + std::to_string(i) + " open file success";
      std::cout << msg << std::endl;
    }
    return;
  };

  for (size_t i = 0; i < thread_cnt; i++) {
    threads.push_back(std::thread(thread_func, i));
  }

  for (size_t i = 0; i < thread_cnt; i++) {
    threads[i].join();
  }

  std::cout << "=============== Test 3 end ===============" << std::endl;
}

void test4_ino_cache() {
  std::cout << "=============== Test 4 start ===============" << std::endl;
  const size_t SIZE = 16000;
  char dir_name = 'a';
  std::string dir_path = "/mnt/nextfs/ysy/mount";
  for (int i = 0; i < 8; i++) {
    dir_name++;
    dir_path.append("/");
    dir_path.append(&dir_name);
    int r = mkdir(dir_path.c_str(), 0666);
  }
  struct stat *buf;
  stat(dir_path.c_str(), buf);
  std::cout << "=============== Test 4 end ===============" << std::endl;
}

static auto get_timestamp() -> uint64_t {
  return std::chrono::duration_cast<std::chrono::nanoseconds>(
             std::chrono::system_clock::now().time_since_epoch())
      .count();
};

int main() {
  // test1();
  // test2();
  // test3_parallel();
  test4_ino_cache();
  // int fd = open("/home/ysy/nextfs/mount/test.txt", O_CREAT | O_RDWR,
  // 0644); if (fd < 0) {
  //   std::cout << "open file failed" << std::endl;
  //   return -1;
  // }
  // struct stat st;
  // int r = fstat(fd, &st);
  // if (r < 0) {
  //   std::cout << "fstat failed" << std::endl;
  //   return -1;
  // }
  // std::cout << "file size: " << st.st_size << std::endl;
  // char buf[1024] = "fuck fuck fuck";
  // r = write(fd, buf, sizeof(buf));
  // if (r < 0) {
  //   std::cout << "write failed" << std::endl;
  //   return -1;
  // }
  // close(fd);
  // fd = open("/home/ysy/nextfs/mount/test.txt", O_RDONLY);
  // if (fd < 0) {
  //   std::cout << "open file failed" << std::endl;
  //   return -1;
  // }
  // memset(buf, '1', sizeof(buf));
  // buf[1023] = '\0';
  // r = read(fd, buf, 100);
  // if (r < 0) {
  //   std::cout << "read failed" << std::endl;
  //   return -1;
  // }
  // std::cout << "read: " << buf << std::endl;
  // close(fd);
  // stat("/home/ysy/nextfs/mount/test.txt", &st);
  // std::cout << "file size: " << st.st_size << std::endl;
  return 0;
}