add_library(libnextfscommon STATIC ${COMMON_SRC})
target_include_directories(libnextfscommon
    PUBLIC ${PROJECT_SOURCE_DIR}/include)
target_link_libraries(libnextfscommon yaml-cpp::yaml-cpp spdlog::spdlog unordered_dense::unordered_dense)
target_compile_options(libnextfscommon PRIVATE -fPIC)