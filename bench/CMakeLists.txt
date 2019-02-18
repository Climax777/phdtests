set(BENCHMARK_ENABLE_TESTING OFF CACHE BOOL "Suppressing benchmark's tests" FORCE)

add_subdirectory(benchmark)
include_directories(benchmark/include)

set(bench_cpp
	dbphd_bench.cpp
	postgres_bench.cpp
)

add_executable(bench_dbphd ${bench_cpp})

target_link_libraries(bench_dbphd
  benchmark
  ${DBPHD_LIB_NAME}
  ${CMAKE_THREAD_LIBS_INIT}
  ${MATH_LIBS}
)
