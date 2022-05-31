add_subdirectory(gtest)

include_directories(./gtest/googletest/include)

set(test_src
  run_all.cpp
  dbphd_test.cpp
  dbphd_mysql_test.cpp
  dbphd_mongo_test.cpp
  dbphd_postgres_test.cpp
  dbphd_tpcchelpers_test.cpp
)

add_executable(test_dbphd ${test_src})

target_link_libraries(test_dbphd
  gtest
  ${DBPHD_LIB_NAME}
  ${CMAKE_THREAD_LIBS_INIT}
  ${MATH_LIBS}
${PQXX_LDFLAGS} ${PQ_LDFLAGS} ${CONCPP_LIBS} ${MONGOCXX_LDFLAGS}
)

add_test(
  NAME tests
  COMMAND test_dbphd
)
