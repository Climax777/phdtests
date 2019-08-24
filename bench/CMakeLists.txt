set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Suppressing benchmark's tests" FORCE)

add_subdirectory(benchmark)
include_directories(benchmark/include)

set(bench_cpp
	dbphd_bench.cpp
	postgres_bench.cpp
	mysql_bench.cpp
	mongodb_bench.cpp
	postgres_create_bench.cpp
	mysql_create_bench.cpp
	mongodb_create_bench.cpp
	postgres_read_bench.cpp
	mysql_read_bench.cpp
	mongodb_read_bench.cpp
	postgres_update_bench.cpp
	mysql_update_bench.cpp
	mongodb_update_bench.cpp
	postgres_delete_bench.cpp
	mysql_delete_bench.cpp
	mongodb_delete_bench.cpp
	postgres_compound_bench.cpp
	mysql_compound_bench.cpp
	mongodb_compound_bench.cpp
)

add_executable(bench_dbphd ${bench_cpp})
target_include_directories(bench_dbphd PUBLIC include ${CMAKE_THREAD_LIBS_INIT} ${MATH_LIBS} ${PQXX_INCLUDE_DIRS} ${CONCPP_INCLUDE_DIR} ${MONGOCXX_INCLUDE_DIRS} ${BSONCXX_INCLUDE_DIRS})

target_link_libraries(bench_dbphd
  benchmark
  ${DBPHD_LIB_NAME}
  ${CMAKE_THREAD_LIBS_INIT}
  ${MATH_LIBS}
${PQXX_LDFLAGS} ${PQ_LDFLAGS} ${CONCPP_LIBS} ${MONGOCXX_LDFLAGS}
)
