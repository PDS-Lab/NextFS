cmake_minimum_required(VERSION 3.16...3.21)
set(CMAKE_POLICY_DEFAULT_CMP0077 NEW)

project(NextFS)
list(APPEND CMAKE_MODULE_PATH "${CMAKE_SOURCE_DIR}/cmake/modules/")
set(CMAKE_EXPORT_COMPILE_COMMANDS ON)

if(NOT CMAKE_BUILD_TYPE)
    set(CMAKE_BUILD_TYPE Release)
endif()

set(CMAKE_CXX_STANDARD 20)
set(CMAKE_CXX_STANDARD_REQUIRED ON)

set(THIRD_PARTY_DIR ${PROJECT_SOURCE_DIR}/third_party)

find_package(spdlog REQUIRED)
find_package(rdmacm REQUIRED)

# find_package(ibverbs REQUIRED)
find_package(rocksdb REQUIRED)
find_package(unordered_dense REQUIRED)
find_package(yaml-cpp REQUIRED)

# find_package(capstone REQUIRED)
find_package(syscall_intercept REQUIRED)

# message(STATUS ${RDMACM_LIBRARIES})
# message(STATUS ${RDMACM_INCLUDE_DIR})
file(GLOB COMMON_SRC ${PROJECT_SOURCE_DIR}/src/common/*.cc)
file(GLOB RPC_SRC ${PROJECT_SOURCE_DIR}/src/rpc/*.cc)
file(GLOB INTERCEPTOR_SRC ${PROJECT_SOURCE_DIR}/src/interceptor/*.cc)
file(GLOB DAEMON_SRC ${PROJECT_SOURCE_DIR}/src/daemon/*.cc)
file(GLOB METADATA_SRC ${PROJECT_SOURCE_DIR}/src/metadata/*.cc ${PROJECT_SOURCE_DIR}/src/metadata/util/*.cc)
file(GLOB TEST_METADATA_SRC ${PROJECT_SOURCE_DIR}/test_metadata/*.cc )
file(GLOB TEST_ALL_SRC ${PROJECT_SOURCE_DIR}/test_all/*.cc )

add_subdirectory(src/common)
add_subdirectory(src/interceptor)
add_subdirectory(src/daemon)
#add_subdirectory(test_metadata)
add_subdirectory(test_all)
#add_subdirectory(demo)
