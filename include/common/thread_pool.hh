#pragma once
#include "common/concurrent_ring.hh"
#include "common/futex.hh"
#include "common/ipc_message.hh"
#include "common/spinlock.hh"
#include "common/util.hh"
#include "numa.h"
#include <atomic>
#include <climits>
#include <condition_variable>
#include <cstddef>
#include <functional>
#include <future>
#include <memory>
#include <mutex>
#include <optional>
#include <thread>
#include <type_traits>
#include <utility>
namespace nextfs {

template <typename T>
class ThreadPool {
public:
  using tp_fn = void (*)(T arg);

private:
  GDRing<T> *task_queue_;
  tp_fn func_;
  // ConcurrentRing<std::function<void()>, ENQ, DEQ_MC> *task_queue_;
  std::vector<std::thread> threads_;
  bool stop_{false};
  std::mutex mutex_;
  std::condition_variable cond_;

public:
  ThreadPool(const ThreadPool &) = delete;
  ThreadPool(ThreadPool &&) = delete;
  auto operator=(const ThreadPool &) -> ThreadPool & = delete;
  auto operator=(ThreadPool &&) -> ThreadPool & = delete;
  ThreadPool(size_t capacity, size_t thread_num, tp_fn func,
             void *addr = nullptr)
      : func_(func) {
    task_queue_ = std::remove_pointer_t<decltype(task_queue_)>::create_ring(
        capacity, addr);
    threads_.reserve(thread_num);
    for (int i = 0; i < thread_num; i++) {
      threads_.emplace_back(&ThreadPool::thread_func, this);
    }
  }
  ~ThreadPool() {
    stop_ = true;
    cond_.notify_all();
    for (auto &t : threads_) {
      t.join();
    }
    delete task_queue_;
  }

  auto enqueue(T arg) -> void {
    while (!task_queue_->enqueue(arg)) {
      cond_.notify_one();
    }
    cond_.notify_one();
  }

  auto thread_func() -> void {
    T arg;
    while (!stop_) {
      if (task_queue_->dequeue(&arg)) {
        func_(arg);
      } else {
        std::unique_lock<std::mutex> lock(mutex_);
        cond_.wait(lock);
      }
    }
  }
};

namespace v2 {

// template <typename T>
// class ThreadPool {
// public:
//   using tp_fn = void (*)(T arg);

// private:
//   GDRing<T> *task_queue_;
//   tp_fn func_;
//   const int numa_;
//   // ConcurrentRing<std::function<void()>, ENQ, DEQ_MC> *task_queue_;
//   std::vector<std::thread> threads_;
//   std::atomic_bool stop_{false};
//   std::atomic_int32_t sem_{0};

// public:
//   ThreadPool(const ThreadPool &) = delete;
//   ThreadPool(ThreadPool &&) = delete;
//   auto operator=(const ThreadPool &) -> ThreadPool & = delete;
//   auto operator=(ThreadPool &&) -> ThreadPool & = delete;
//   ThreadPool(size_t capacity, size_t thread_num, tp_fn func, int numa = -1,
//              void *addr = nullptr)
//       : func_(func), numa_(numa) {
//     task_queue_ = std::remove_pointer_t<decltype(task_queue_)>::create_ring(
//         capacity, addr);
//     threads_.reserve(thread_num);
//     for (int i = 0; i < thread_num; i++) {
//       threads_.emplace_back(&ThreadPool::thread_func, this);
//     }
//   }
//   ~ThreadPool() {
//     stop_ = true;
//     futex_wake(reinterpret_cast<uint32_t *>(&sem_), INT_MAX, true);
//     for (auto &t : threads_) {
//       t.join();
//     }
//     delete task_queue_;
//   }

//   auto enqueue(T arg) -> void {
//     while (!task_queue_->enqueue(arg)) {
//       cpu_relax();
//     }
//   }

//   auto signal(size_t n) -> void {
//     if (n > 0) {
//       sem_.fetch_add(n, std::memory_order_release);
//       futex_wake(reinterpret_cast<uint32_t *>(&sem_), n, true);
//     }
//   }

//   auto thread_func() -> void {
//     if (numa_ != -1) {
//       numa_run_on_node(numa_);
//       std::this_thread::yield();
//     }
//     T arg;

//     while (!stop_) {
//       // wait resources
//       acquire();
//       if (task_queue_->dequeue(&arg)) {
//         func_(arg);
//       }
//     }
//   }

// private:
//   auto acquire() -> void {
//     auto try_acquire = [&]() {
//       auto old = sem_.load(std::memory_order_acquire);
//       if (old == 0) {
//         return false;
//       }
//       return sem_.compare_exchange_strong(
//           old, old - 1, std::memory_order_acquire,
//           std::memory_order_relaxed);
//     };
//     do {
//       for (int i = 0; i < 16; i++) {
//         if (try_acquire()) {
//           return;
//         }
//         cpu_relax();
//       }
//       futex_wait(reinterpret_cast<uint32_t *>(&sem_), 0, true);
//     } while (!try_acquire());
//   }
// };

template <typename T>
class ThreadPool {
public:
  using tp_fn = void (*)(T arg);

private:
  struct Context {
    std::thread thread_{};
    int core_;
    std::optional<T> args_;
    FutexParker parker_{true};
  };
  const int numa_;
  std::vector<int> cores_;
  tp_fn func_;
  // ConcurrentRing<std::function<void()>, ENQ, DEQ_MC> *task_queue_;

  std::atomic_bool stop_{false};
  Spinlock spin_;
  std::vector<std::unique_ptr<Context>> threads_;
  std::vector<Context *> idle_threads_;

public:
  ThreadPool(const ThreadPool &) = delete;
  ThreadPool(ThreadPool &&) = delete;
  auto operator=(const ThreadPool &) -> ThreadPool & = delete;
  auto operator=(ThreadPool &&) -> ThreadPool & = delete;
  ThreadPool(size_t capacity, size_t thread_num, tp_fn func, int numa = -1)
      : func_(func), numa_(numa) {
    if (numa != -1) {
      bitmask *numa_cpu_mask = numa_allocate_cpumask();
      numa_node_to_cpus(numa, numa_cpu_mask);
      for (int i = 0; i < numa_cpu_mask->size; i++) {
        if (numa_bitmask_isbitset(numa_cpu_mask, i)) {
          cores_.push_back(i);
        }
      }
    }
    for (int i = 0; i < thread_num; i++) {
      threads_.emplace_back(std::make_unique<Context>());
      threads_.back()->core_ = numa_ == -1 ? -1 : cores_[i % cores_.size()];
      threads_.back()->thread_ =
          std::thread(&ThreadPool::thread_func, this, threads_.back().get());
      idle_threads_.push_back(threads_.back().get());
    }
  }
  ~ThreadPool() {
    stop_ = true;
    for (auto &t : threads_) {
      t->parker_.unpark();
      t->thread_.join();
    }
  }

  auto enqueue(T arg) -> void {
    Context *ctx = nullptr;
    {
      std::lock_guard<Spinlock> lock(spin_);
      if (idle_threads_.empty()) {
        threads_.emplace_back(std::make_unique<Context>());
        ctx = threads_.back().get();
        ctx->core_ = numa_ == -1 ? -1 : cores_[threads_.size() % cores_.size()];
        ctx->thread_ = std::thread(&ThreadPool::thread_func, this, ctx);
      } else {
        ctx = idle_threads_.back();
        idle_threads_.pop_back();
      }
      ctx->args_ = arg;
    }
    ctx->parker_.unpark();
  }

  auto thread_func(Context *ctx) -> void {
    if (numa_ != -1) {
      numa_run_on_node(numa_);
      std::this_thread::yield();
      numa_hint = numa_;
    }
    while (!stop_) {
      ctx->parker_.park();
      if (ctx->args_.has_value()) {
        func_(ctx->args_.value());
        ctx->args_.reset();
        std::lock_guard<Spinlock> lock(spin_);
        idle_threads_.push_back(ctx);
      }
    }
  }
};
} // namespace v2

} // namespace nextfs