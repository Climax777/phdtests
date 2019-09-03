#include "benchmark/benchmark.h"
#include "dbphd/mysqldb/mysqldb.hpp"

#include <random>

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

static void BM_MYSQL_Insert(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, (1 << 16));
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		MySQLDBHandler::CreateDatabase(conn, "bench");
		MySQLDBHandler::DropTable(conn, "bench", "create_bench");
		string createQuery = R"|(
CREATE TABLE create_bench (
	_id  INT AUTO_INCREMENT,
	a0 INT,
)|";
		for(int field = 1; field < state.range(1); ++field) {
			createQuery.append("a" + to_string(field) + " INT,");
		}
		createQuery.append(R"|(
	PRIMARY KEY(_id)
) ENGINE=INNODB;
)|");
		conn.sql(createQuery).execute();
		if(state.range(2) > 0) {
			for(int index = 0; index < state.range(2); ++index) {
				conn.sql("CREATE INDEX field" + to_string(index) + " ON bench.create_bench(a" + to_string(index) + ");").execute();
			}
		}
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("create_bench");
	for(auto _ : state) {
		state.PauseTiming();
		auto tableInsert = table.insert();
		for(int n = 0; n < state.range(0); ++n) {
			mysqlx::Row row;
				row.set(0, mysqlx::nullvalue);
			for(int fields = 0; fields < state.range(1); ++fields) {
				row.set(fields+1, dis(gen));
			}
			tableInsert.rows(row);
		}
		state.ResumeTiming();
		auto result = tableInsert.execute();
	}

	if(state.thread_index == 0) {
		MySQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_MYSQL_Insert)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_InsertTransact(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, (1 << 16));
	const std::string query = "select 500";
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		MySQLDBHandler::CreateDatabase(conn, "bench");
		MySQLDBHandler::DropTable(conn, "bench", "create_bench");
		string createQuery = R"|(
CREATE TABLE create_bench (
	_id  INT AUTO_INCREMENT,
	a0 INT,
)|";
		for(int field = 1; field < state.range(1); ++field) {
			createQuery.append("a" + to_string(field) + " INT,");
		}
		createQuery.append(R"|(
	PRIMARY KEY(_id)
) ENGINE=INNODB;
)|");
		conn.sql(createQuery).execute();
		if(state.range(2) > 0) {
			for(int index = 0; index < state.range(2); ++index) {
				conn.sql("CREATE INDEX field" + to_string(index) + " ON bench.create_bench(a" + to_string(index) + ");").execute();
			}
		}
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("create_bench");
	for(auto _ : state) {
		state.PauseTiming();
		auto tableInsert = table.insert();
		for(int n = 0; n < state.range(0); ++n) {
			mysqlx::Row row;
				row.set(0, mysqlx::nullvalue);
			for(int fields = 0; fields < state.range(1); ++fields) {
				row.set(fields+1, dis(gen));
			}
			tableInsert.rows(row);
		}
		state.ResumeTiming();
		conn.startTransaction();
		auto result = tableInsert.execute();
		conn.commit();
	}

	if(state.thread_index == 0) {
		MySQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_MYSQL_InsertTransact)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1,4);
