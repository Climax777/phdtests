#include <iostream>

#include "benchmark/benchmark.h"
#include "dbphd/dbphd.hpp"
#include "precalculate.hpp"

using namespace std;

static void BM_Basic(benchmark::State& state) {
  for(auto _ : state) {
	  dbphd::DBPHD dbphd;
  }
}

//BENCHMARK(BM_Basic)->Iterations(2);

int main(int argc, char** argv) {
	cout << "Precalculating...";
	cout.flush();
	Precalculator::PreCalculate();
	cout << " Done" << endl;
	cout.flush();
	::benchmark::Initialize(&argc, argv); 
	if (::benchmark::ReportUnrecognizedArguments(argc, argv)) return 1;
	::benchmark::RunSpecifiedBenchmarks(); 
}
