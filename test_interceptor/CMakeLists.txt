# cmake_minimum_required(VERSION 3.1...3.21)

# project(NextFS)
# list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/")
# set(CMAKE_EXPORT_COMPILE_COMMANDS ON) 
# set(CMAKE_BUILD_TYPE "Debug")

# set(CMAKE_CXX_STANDARD 20)
# set(CMAKE_CXX_STANDARD_REQUIRED ON)

# find_package(spdlog REQUIRED)
# find_package(rdmacm REQUIRED)
# find_package(unordered_dense REQUIRED)
# find_package(yaml-cpp REQUIRED)

# # message(STATUS ${RDMACM_LIBRARIES})
# # message(STATUS ${RDMACM_INCLUDE_DIR})
# file(GLOB COMMON_SRC ${PROJECT_SOURCE_DIR}/src/common/*.cc)
# file(GLOB RPC_SRC ${PROJECT_SOURCE_DIR}/src/rpc/*.cc)
# file(GLOB INTERCEPTOR_SRC ${PROJECT_SOURCE_DIR}/src/interceptor/*.cc)
# file(GLOB DAEMON_SRC ${PROJECT_SOURCE_DIR}/src/daemon/*.cc)
# file(GLOB METADATA_SRC ${PROJECT_SOURCE_DIR}/src/metadata/*.cc ${PROJECT_SOURCE_DIR}/src/metadata/util/*.cc)
# file(GLOB TEST_SRC ${PROJECT_SOURCE_DIR}/test/*.cc )

# add_subdirectory(src/common)
# #add_subdirectory(src/interceptor)
# #add_subdirectory(src/daemon)
# add_subdirectory(test)
# #add_subdirectory(demo)
################################################################


cmake_minimum_required(VERSION 3.1...3.21)
SET(CMAKE_CXX_FLAGS "${CMAKE_CXX_FLAGS} -std=c++17")
project(NextFS)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON) 
set(CMAKE_BUILD_TYPE "Debug")

find_package(spdlog REQUIRED) 
find_package(rdmacm REQUIRED)
find_package(unordered_dense REQUIRED)
find_package(yaml-cpp REQUIRED)
find_package(rocksdb REQUIRED)

# message(STATUS ${RDMACM_LIBRARIES})
# message(STATUS ${RDMACM_INCLUDE_DIR})
file(GLOB COMMON_SRC ${PROJECT_SOURCE_DIR}/src/common/*.cc)
file(GLOB RPC_SRC ${PROJECT_SOURCE_DIR}/src/rpc/*.cc)
file(GLOB INTERCEPTOR_SRC ${PROJECT_SOURCE_DIR}/src/interceptor/*.cc)
file(GLOB DAEMON_SRC ${PROJECT_SOURCE_DIR}/src/daemon/*.cc)
file(GLOB METADATA_SRC ${PROJECT_SOURCE_DIR}/src/metadata/*.cc ${PROJECT_SOURCE_DIR}/src/metadata/util/*.cc)
file(GLOB TEST_METADATA_SRC ${PROJECT_SOURCE_DIR}/test/metadata/*.cc )
INCLUDE_DIRECTORIES("./nextfs_deps/install/include")
include_directories("./include")
add_subdirectory(src/interceptor)
# add_executable(interceptor_test ./test_interceptor/main.cc)
# add_executable(interceptor_test ./test_interceptor/main.cc ${COMMON_SRC} ${INTERCEPTOR_SRC} ./src/daemon/env.cc ./src/daemon/ipc_receiver.cc  ${RPC_SRC}  ${METADATA_SRC} )
add_executable(interceptor_test ./test_interceptor/main.cc ${COMMON_SRC} ./src/interceptor/file.cc ./src/interceptor/ipc_sender.cc ./src/interceptor/nextfs.cc ./src/interceptor/preload.cc  ./src/daemon/env.cc ./src/daemon/ipc_receiver.cc  ${RPC_SRC}  ${METADATA_SRC})



# add_executable(interceptor_test ./test_interceptor/main.cc ${COMMON_SRC} )
target_include_directories(interceptor_test PRIVATE
    ${PROJECT_SOURCE_DIR}/include 
    ${PROJECT_SOURCE_DIR}/nextfs_deps/install/include
)

link_libraries("/home/yzk/mergemain/nextfs_deps/install/lib/librocksdb.so")

target_link_libraries(interceptor_test syscall_intercept )
target_link_libraries(interceptor_test yaml-cpp.a)
# target_link_libraries(interceptor_test unordered_dense::unordered_dense rdmacm spdlog ibverbs pthread event lz4 pthread -lz -lsnappy -ldl ${URING} event_pthreads)
target_link_libraries(interceptor_test unordered_dense::unordered_dense rdmacm spdlog ibverbs pthread event lz4 pthread -lz -lsnappy -ldl rocksdb.a uring event_pthreads syscall_intercept  -lrt)
# libnextfscommon

# add_library(icap SHARED ./test_interceptor/icap.c)
# target_link_libraries(icap PRIVATE syscall_intercept)

