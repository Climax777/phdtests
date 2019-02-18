add_subdirectory(gtest)

include_directories(./gtest/googletest/include)

set(test_src
  run_all.cpp
  dbphd_test.cpp
)

add_executable(test_dbphd ${test_src})

target_link_libraries(test_dbphd
  gtest
  ${DBPHD_LIB_NAME}
  ${CMAKE_THREAD_LIBS_INIT}
  ${MATH_LIBS}
)

add_test(
  NAME tests
  COMMAND test_dbphd
)
