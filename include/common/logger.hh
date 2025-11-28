#pragma once
#include <algorithm>
#include <memory>
#include <spdlog/common.h>
#include <spdlog/logger.h>
#include <spdlog/sinks/stdout_color_sinks.h>
#include <stdexcept>
namespace nextfs {
class GlobalLogger {
public:
  static auto instance() -> GlobalLogger & {
    static GlobalLogger a;
    return a;
  }
  static auto logger() -> spdlog::logger * { return instance().logger_.get(); }
  static auto set_logger(std::shared_ptr<spdlog::logger> logger) {
    instance().logger_ = std::move(logger);
  }
  static auto init(spdlog::level::level_enum log_level) {
    if (instance().logger_ != nullptr) {
      return;
    }
    instance().logger_ = spdlog::stdout_color_mt("global");
    instance().logger_->set_level(log_level);
  }

private:
  std::shared_ptr<spdlog::logger> logger_{nullptr};
};
template <typename... Args>
static inline auto check(int rc, const char *msg, Args... args) -> void {
  if (rc != 0) {
    printf("fuck: %d\n", rc);
    GlobalLogger::logger()->error(msg, args...);
    throw std::runtime_error(fmt::format(msg, args...));
  }
}
template <typename... Args>
static inline auto checkp(void *p, const char *msg, Args... args) -> void {
  if (p == nullptr) {
    GlobalLogger::logger()->error(msg, args...);
    throw std::runtime_error(fmt::format(msg, args...));
  }
}
template <typename... Args>
static inline auto info(const char *msg, Args... args) -> void {
  GlobalLogger::logger()->info(msg, args...);
}
template <typename... Args>
static inline auto error(const char *msg, Args... args) -> void {
  GlobalLogger::logger()->error(msg, args...);
}
template <typename... Args>
static inline auto warn(const char *msg, Args... args) -> void {
  GlobalLogger::logger()->warn(msg, args...);
}
template <typename... Args>
static inline auto trace(const char *msg, Args... args) -> void {
  GlobalLogger::logger()->trace(msg, args...);
}
template <typename... Args>
static inline auto debug(const char *msg, Args... args) -> void {
  GlobalLogger::logger()->debug(msg, args...);
}
}; // namespace nextfs