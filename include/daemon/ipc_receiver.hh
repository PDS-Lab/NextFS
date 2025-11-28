#pragma once

#include "common/ipc_message.hh"
#include <cstddef>
namespace nextfs {

auto poll_ipc_message(IpcMessageInfo *infos, size_t n) -> size_t;

void wait_client_state(IpcMessageSync &sync, IpcState expected);

void ipc_msg_handler(IpcMessageInfo info);

void ipc_handle_mkdir(IpcMessageInfo info, IpcMessageSync &sync,
                      IpcMkdiratMsg &msg);

void ipc_handle_open(IpcMessageInfo info, IpcMessageSync &sync,
                     IpcOpenatMsg &msg);

void ipc_handle_close(IpcMessageInfo info, IpcMessageSync &sync,
                      IpcCloseMsg &msg);

void ipc_handle_rename(IpcMessageInfo info, IpcMessageSync &sync,
                       IpcRenameMsg &msg);

void ipc_handle_stat(IpcMessageInfo info, IpcMessageSync &sync,
                     IpcStatMsg &msg);

void ipc_handle_unlink(IpcMessageInfo info, IpcMessageSync &sync,
                       IpcUnlinkMsg &msg);

void ipc_handle_read(IpcMessageInfo info, IpcMessageSync &sync,
                     IpcReadMsg &msg);

void ipc_handle_write(IpcMessageInfo info, IpcMessageSync &sync,
                      IpcWriteMsg &msg);
} // namespace nextfs