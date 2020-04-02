#include "benchmark/benchmark.h"
#include "dbphd/postgresql/postgresql.hpp"

#include <pqxx/nontransaction.hxx>
#include <pqxx/result.hxx>
#include <pqxx/transaction_base.hxx>
#include <random>
#include <iostream>
#include <chrono>

using namespace std;

static void CustomArgumentsInserts(benchmark::internal::Benchmark* b) {
	for (int i = 1; i <= (1 << 12); i*=8) { // Documents
		for (int j = 1; j <= 16; j *= 2) { // Fields
			for(int k = 0; k <= j; ) { // Indexes
				b->Args({i, j, k});
				if(k == 0) {
					k = 1;
				} else {
					k *= 2;
				}
			}
		}
	}
}

static void BM_PQXX_Insert(benchmark::State& state, bool transactions) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, (1 << 16));
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		try{
			PostgreSQLDBHandler::CreateDatabase(conn, "bench");
		} catch(...) {

		}
		PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
		string createQuery = R"|(
CREATE TABLE create_bench (
	_id  serial PRIMARY KEY,
)|";
		for(int field = 0; field < state.range(1); ++field) {
			createQuery.append("a" + to_string(field) + " INT");
			if(field != state.range(1) - 1)
				createQuery.append(",\r\n");
		}
		createQuery.append(R"|(
);
)|");
		{
			pqxx::nontransaction N(*conn);
			pqxx::result R(N.exec(createQuery));
		}
		if(state.range(2) > 0) {
			for(int index = 0; index < state.range(2); ++index) {
				pqxx::nontransaction N(*conn);
				pqxx::result R(N.exec("CREATE INDEX field" + to_string(index) + " ON bench.create_bench\r\n(a" + to_string(index) + ");"));
			}
		}
	}
	for(auto _ : state) {
		state.PauseTiming();
		string query = "INSERT INTO bench.create_bench VALUES\r\n";
		for(int n = 0; n < state.range(0); ++n) {
			query.append("	(DEFAULT");
			for(int fields = 0; fields < state.range(1); ++fields) {
				query.append("," + to_string(dis(gen)));
			}
			if(n != state.range(0)-1) {
				query.append("),");
			} else {
				query.append(");"); // RETURNING _id // Technically to make it more fair, pqxx should return the id's of the inserteds
			}
		}
		state.ResumeTiming();
		auto start = std::chrono::high_resolution_clock::now();
		pqxx::transaction_base* T = nullptr;
		if(transactions) {
			pqxx::work* w = new pqxx::work(*conn);
			T = w;
		} else {
			pqxx::nontransaction* N = new pqxx::nontransaction(*conn);
			T = N;
		}
		pqxx::result R(T->exec(query));
		T->commit();
		if(transactions) {
			delete (pqxx::nontransaction*)T;
		} else {
			delete (pqxx::work*)T;
		}
		auto end = std::chrono::high_resolution_clock::now();

		auto elapsed_seconds =
			std::chrono::duration_cast<std::chrono::duration<double>>(
					end - start);

		state.SetIterationTime(elapsed_seconds.count());
	}

	if(state.thread_index == 0) {
		PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}

	//state.SetItemsProcessed(state.iterations()*state.range(0));
	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations()*state.range(0), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations()*state.range(0), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);

	state.counters.insert({{"Documents", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Fields", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK_CAPTURE(BM_PQXX_Insert, Normal, false)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_PQXX_Insert, Transact, true)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
