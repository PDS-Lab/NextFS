add_executable(rpc_server ${CMAKE_CURRENT_SOURCE_DIR}/server.cc ${RPC_SRC} ../src/daemon/env.cc )
target_include_directories(rpc_server PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_compile_features(rpc_server PUBLIC cxx_std_20)
target_link_options(rpc_server PRIVATE -fsanitize=address)
target_link_libraries(rpc_server libnextfscommon unordered_dense::unordered_dense rdmacm spdlog ibverbs pthread event  event_pthreads
)


add_executable(rpc_client ${CMAKE_CURRENT_SOURCE_DIR}/client.cc ${RPC_SRC} ../src/daemon/env.cc)
target_include_directories(rpc_client PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_compile_features(rpc_client PUBLIC cxx_std_20)
target_link_options(rpc_client PRIVATE -fsanitize=address)
target_link_libraries(rpc_client libnextfscommon unordered_dense::unordered_dense rdmacm spdlog ibverbs pthread event event_pthreads
)

