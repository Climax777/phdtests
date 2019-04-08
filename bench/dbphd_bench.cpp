#include "benchmark/benchmark.h"
#include "dbphd/dbphd.hpp"

static void BM_Basic(benchmark::State& state) {
  for(auto _ : state) {
	  dbphd::DBPHD dbphd;
  }
}

BENCHMARK(BM_Basic)->Iterations(2);

BENCHMARK_MAIN();
