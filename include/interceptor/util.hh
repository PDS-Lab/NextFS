#pragma once
#include "common/ipc_message.hh"
#include "interceptor/preload.hh"
namespace nextfs {

auto submit_request(const IpcMessageInfo &info,
                    PreloadContext::ipc_channel_type *channel) -> void;

auto wait_response(IpcMessageSync &sync) -> IpcState;

auto resolve_path(const std::string &cwd, const char *pathname) -> std::string;
} // namespace nextfs