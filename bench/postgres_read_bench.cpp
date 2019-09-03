#include "benchmark/benchmark.h"
#include "dbphd/postgresql/postgresql.hpp"

#include <random>
#include <iostream>

using namespace std;

static void CustomArgumentsInserts(benchmark::internal::Benchmark* b) {
	for (int i = 1; i <= 5; ++i) { // fields to query
		for (int j = 0; j <= i; ++j) { //  Indexes
			b->Args({i, j});
		}
	}
}

static void CreateTable(std::shared_ptr<pqxx::connection> conn) {
	static volatile bool created = false;
	if(!created) {
		try{
			PostgreSQLDBHandler::CreateDatabase(conn, "bench");
		} catch(...) {

		}
		PostgreSQLDBHandler::DropTable(conn, "bench", "read_bench");
		string createQuery = R"|(
CREATE TABLE read_bench (
	_id  serial PRIMARY KEY,
)|";
		for(int field = 0; field < 5; ++field) {
			createQuery.append("a" + to_string(field) + " INT");
			if(field != 4)
				createQuery.append(",\r\n");
		}
		createQuery.append(R"|(
);
)|");
		{
			pqxx::nontransaction N(*conn);
			pqxx::result R(N.exec(createQuery));
		}

		int values[] = {0,1,2,3,4,5,6,7,8,9};
		for(int i = 0; i < pow(10,5); i += 100) {
			string query = "INSERT INTO bench.read_bench VALUES\r\n";
			for(int j = 0; j < 100 && i+j < pow(10,5);++j) {
				query.append("	(DEFAULT");
				int iteration = i+j;
				for(int f = 0; f < 5; ++f) {
					query.append("," + to_string(values[iteration%10]));
					iteration /= 10;
				}
				if(j != 99 && j+i != pow(10,5)-1) {
					query.append("),");
				} else {
					query.append(");");
					pqxx::nontransaction N(*conn);
					pqxx::result R(N.exec(query));
				}
			}
		}
	}
	created = true;
}

static void BM_PQXX_Read_Count(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, (1 << 16));
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			string query = "DROP INDEX IF EXISTS bench.read_bench_idx;";
			pqxx::nontransaction N(*conn);
			pqxx::result R(N.exec(query));
		}catch(...) {
		}
		if(state.range(1) > 0) {
			string indexCreate = "CREATE INDEX read_bench_idx on bench.read_bench (a0";
			for(int index = 1; index < state.range(1); ++index) {
				indexCreate += ",a" + to_string(index);
			}
			indexCreate += ");";
			pqxx::nontransaction N(*conn);
			pqxx::result R(N.exec(indexCreate));
		}
	}
	for(auto _ : state) {
		state.PauseTiming();
		string query = "SELECT COUNT(*) FROM bench.read_bench WHERE\r\n";
		query += "a0 = " + to_string(dis(gen));
		for(int n = 1; n < state.range(0); ++n) {
			query += " AND a" + to_string(n) + " = " + to_string(dis(gen));
		}
		state.ResumeTiming();
		pqxx::nontransaction N(*conn);
		pqxx::result R(N.exec(query));
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}

	state.SetItemsProcessed(pow(10,5)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Fields", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_PQXX_Read_Count)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1, 4);

static void BM_PQXX_Read_CountTransact(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, (1 << 16));
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			string query = "DROP INDEX IF EXISTS bench.read_bench_idx;";
			pqxx::nontransaction N(*conn);
			pqxx::result R(N.exec(query));
		}catch(...) {
		}
		if(state.range(1) > 0) {
			string indexCreate = "CREATE INDEX read_bench_idx on bench.read_bench (a0";
			for(int index = 1; index < state.range(1); ++index) {
				indexCreate += ",a" + to_string(index);
			}
			indexCreate += ");";
			pqxx::nontransaction N(*conn);
			pqxx::result R(N.exec(indexCreate));
		}
	}
	for(auto _ : state) {
		state.PauseTiming();
		string query = "SELECT COUNT(*) FROM bench.read_bench WHERE\r\n";
		query += "a0 = " + to_string(dis(gen));
		for(int n = 1; n < state.range(0); ++n) {
			query += " AND a" + to_string(n) + " = " + to_string(dis(gen));
		}
		state.ResumeTiming();
		pqxx::work w(*conn);
		w.exec(query);
		w.commit();
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}

	state.SetItemsProcessed(pow(10,5)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Fields", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_PQXX_Read_CountTransact)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1, 4);
