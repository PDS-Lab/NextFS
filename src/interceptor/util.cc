#include "interceptor/util.hh"
#include "common/ipc_message.hh"
#include "common/spinwait.hh"
#include "common/util.hh"
#include "interceptor/preload.hh"
#include <atomic>
#include <thread>

namespace nextfs {

auto submit_request(const IpcMessageInfo &info,
                    PreloadContext::ipc_channel_type *channel) -> void {
  channel->enqueue(info);
  PreloadContext::instance().ipc_futex_->unpark();
}
auto wait_response(IpcMessageSync &sync) -> IpcState {
  IpcState s;
  SpinWait sw(1000);
  while ((s = sync.state_.load(std::memory_order_acquire)) ==
         IpcState::IPC_STATE_PENDING) {
    cpu_relax();
    // sw.spin_no_block(std::this_thread::yield);
  }
  return s;
}

auto resolve_path(const std::string &prefix, const char *pathname)
    -> std::string {
  if (unlikely(pathname == nullptr)) {
    return "";
  }
  std::string tmp;
  if (pathname[0] == '/') {
    tmp = pathname;
  } else {
    size_t raw_len = strlen(pathname);
    tmp.reserve(prefix.size() + raw_len + 1);
    tmp.append(prefix);
    tmp.push_back('/');
    tmp.append(pathname, raw_len);
  }
  std::string result;
  result.reserve(tmp.size());

  for (size_t n_pos = 0, last_slash_pos = 0; n_pos < tmp.size(); n_pos++) {
    size_t start = n_pos;
    // skip continuous slashes
    while (start < tmp.size() && tmp[start] == '/') {
      start++;
    }
    // next slash
    n_pos = tmp.find('/', start);
    if (n_pos == std::string::npos) {
      n_pos = tmp.size();
    }

    size_t name_len = n_pos - start;
    if (name_len == 0) {
      break;
    }
    if (name_len == 1 && tmp[start] == '.') {
      continue;
    }
    if (name_len == 2 && tmp[start] == '.' && tmp[start + 1] == '.') {
      if (!result.empty()) {
        result.erase(last_slash_pos);
        last_slash_pos = result.find_last_of('/');
      }
      continue;
    }
    last_slash_pos = result.size();
    result.push_back('/');
    result.append(tmp, start, name_len);
  }
  if (result.empty()) {
    result.push_back('/');
  }
  return result;
}
} // namespace nextfs