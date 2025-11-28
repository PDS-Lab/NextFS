add_executable(test_metadata
    ${TEST_METADATA_SRC}
    ${RPC_SRC}
    ${PROJECT_SOURCE_DIR}/src/daemon/env.cc
    ${PROJECT_SOURCE_DIR}/src/daemon/ipc_receiver.cc
    ${METADATA_SRC}
)

target_include_directories(test_metadata PRIVATE
    ${PROJECT_SOURCE_DIR}/include 
    ${PROJECT_SOURCE_DIR}/nextfs_deps/install/include
)
target_compile_features(test_metadata PUBLIC cxx_std_20)


target_link_libraries(test_metadata libnextfscommon rdmacm ibverbs pthread event lz4 rocksdb event_pthreads aio numa -lrt)
