#pragma once
#include "common/logger.hh"
#include <ankerl/unordered_dense.h>
#include <bits/types/struct_timespec.h>
#include <cerrno>
#include <cstddef>
#include <cstdint>
#include <cstring>
#include <libaio.h>
#include <sys/types.h>
namespace nextfs {

class AioEngine {
public:
  class IoReq {
    friend class AioEngine;

  private:
    iocb iocb_;
    long res_;
    long res2_;
    bool done_{false};

  public:
    static auto read(int fd, void *buf, size_t size, off_t offset,
                     io_callback_t) -> IoReq {
      IoReq req;
      io_prep_pread(&req.iocb_, fd, buf, size, offset);
      req.iocb_.data = (void *)IoReq::callback;
      return req;
    }
    static auto write(int fd, void *buf, size_t size, off_t offset) -> IoReq {
      IoReq req;
      io_prep_pwrite(&req.iocb_, fd, buf, size, offset);
      req.iocb_.data = (void *)IoReq::callback;
      return req;
    }
    auto done() -> bool { return done_; }
    static auto callback(io_context_t, iocb *iocb, long res, long res2)
        -> void {
      auto req = reinterpret_cast<IoReq *>(iocb);
      req->done_ = true;
      req->res_ = res;
      req->res2_ = res2;
    }
  };

private:
  static constexpr size_t MAX_DEPTH = 1024;
  // per thread context
  struct Context {
    io_context_t io_ctx_;
    uint16_t in_flight_{0};

    Context() {
      int ret = io_setup(MAX_DEPTH, &io_ctx_);
      check(ret, "io setup failed:{}", strerror(errno));
    }
    ~Context() { io_destroy(io_ctx_); }
  };
  thread_local static Context ctx_;

public:
  static auto submit(IoReq *reqs, size_t nreqs) -> void {
    if (ctx_.in_flight_ + nreqs > MAX_DEPTH) {
      warn("too many in flight io requests");
      return;
    }
    if (nreqs == 0)
      return;
    iocb *iocbs[nreqs];
    for (size_t i = 0; i < nreqs; i++) {
      iocbs[i] = &reqs[i].iocb_;
    }
    int ret = io_submit(ctx_.io_ctx_, nreqs, iocbs);
    check(ret, "io submit failed:{}", strerror(errno));
    ctx_.in_flight_ += ret;
  }
  static auto wait_ioevent() -> void {
    struct timespec timeout;
    timeout.tv_nsec = 0;
    timeout.tv_sec = 0;
    io_event events[MAX_DEPTH];
    int ret = io_getevents(ctx_.io_ctx_, 1, MAX_DEPTH, events, &timeout);
    check(ret, "io get events failed:{}", strerror(errno));
    ctx_.in_flight_ -= ret;
    for (int i = 0; i < ret; i++) {
      auto cb = (io_callback_t)events[i].data;
      cb(ctx_.io_ctx_, events[i].obj, events[i].res, events[i].res2);
    }
  }
};
} // namespace nextfs