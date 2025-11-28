# # find include
# find_path(ROCKSDB_INCLUDE_DIR rocksdb/db.h rocksdb/option.h ${CMAKE_SOURCE_DIR}/third_party/rocksdb/include)
# # find librocksdb.a
# find_library(ROCKSDB_LIB rocksdb ${CMAKE_SOURCE_DIR}/third_party/rocksdb)
set(WITH_LZ4 ON)
set(WITH_LIBURING OFF)
set(WITH_TOOLS OFF)
set(WITH_CORE_TOOLS OFF)
set(WITH_BENCHMARK_TOOLS OFF)
set(WITH_TESTS OFF)
set(WITH_ALL_TESTS OFF)
set(WITH_TRACE_TOOLS OFF)
add_subdirectory(${THIRD_PARTY_DIR}/rocksdb)