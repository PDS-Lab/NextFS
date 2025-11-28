add_executable(test_all 
    ${TEST_ALL_SRC}
)

target_link_libraries(test_all pthread spdlog::spdlog)