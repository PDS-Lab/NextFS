add_executable(nextfs_daemon
    ${RPC_SRC}
    ${DAEMON_SRC}
    ${METADATA_SRC}
)

target_include_directories(nextfs_daemon PRIVATE
    ${PROJECT_SOURCE_DIR}/include
)

target_link_libraries(nextfs_daemon libnextfscommon rocksdb rdmacm ibverbs pthread event event_pthreads aio numa -lrt)
