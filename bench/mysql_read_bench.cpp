#include "benchmark/benchmark.h"
#include "dbphd/mysqldb/mysqldb.hpp"

#include <random>

using namespace std;

static void CustomArgumentsInserts(benchmark::internal::Benchmark* b) {
	for (int i = 1; i <= 5; ++i) { // fields to query
		for (int j = 0; j <= i; ++j) { //  Indexes
			b->Args({i, j});
		}
	}
}

static void CreateTable(mysqlx::Session &conn) {
	static volatile bool created = false;
	if(!created) {
		MySQLDBHandler::CreateDatabase(conn, "bench");
		MySQLDBHandler::DropTable(conn, "bench", "read_bench");
		string createQuery = R"|(
CREATE TABLE read_bench (
	_id  INT AUTO_INCREMENT,
	a0 INT,
)|";
		for(int field = 1; field < 5; ++field) {
			createQuery.append("a" + to_string(field) + " INT,");
		}
		createQuery.append(R"|(
	PRIMARY KEY(_id)
) ENGINE=INNODB;
)|");
		conn.sql(createQuery).execute();
		auto db = conn.getSchema("bench");
		auto table = db.getTable("read_bench");

		int values[] = {0,1,2,3,4,5,6,7,8,9};
		for(int i = 0; i < pow(10,5); i += 100) {
			auto tableInsert = table.insert();
			for(int j = 0; j < 100 && i+j < pow(10,5);++j) {
				mysqlx::Row row;
				row.set(0, mysqlx::nullvalue);
				int iteration = i+j;
				for(int f = 1; f <= 5; ++f) {
					row.set(f, values[iteration%10]);
					iteration /= 10;
				}
				tableInsert.rows(row);
			}
			auto result = tableInsert.execute();
		}
	}
	created = true;
}

static void BM_MYSQL_Read_Count(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, (1 << 16));
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
		if(state.range(1) > 0) {
			string indexCreate = "CREATE INDEX idx on bench.read_bench (a0";
			for(int index = 1; index < state.range(1); ++index) {
				indexCreate += ",a" + to_string(index);
			}
			indexCreate += ") ALGORITHM INPLACE;";
			conn.sql(indexCreate).execute();
		}
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("read_bench");
	for(auto _ : state) {
		state.PauseTiming();
		auto tableSelect = table.select("COUNT(*)");
		string whereclause = "a0 = " + to_string(dis(gen));
		for(int n = 1; n < state.range(0); ++n) {
			whereclause += " AND a" + to_string(n) + " = " + to_string(dis(gen));
		}
		tableSelect.where(whereclause);
		state.ResumeTiming();
		auto result = tableSelect.execute().fetchAll();
	}

	if(state.thread_index == 0) {
		//MySQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_MYSQL_Read_Count)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_CountTransact(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, (1 << 16));
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
		if(state.range(1) > 0) {
			string indexCreate = "CREATE INDEX idx on bench.read_bench (a0";
			for(int index = 1; index < state.range(1); ++index) {
				indexCreate += ",a" + to_string(index);
			}
			indexCreate += ") ALGORITHM INPLACE;";
			conn.sql(indexCreate).execute();
		}
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("read_bench");
	for(auto _ : state) {
		state.PauseTiming();
		auto tableSelect = table.select("COUNT(*)");
		string whereclause = "a0 = " + to_string(dis(gen));
		for(int n = 1; n < state.range(0); ++n) {
			whereclause += " AND a" + to_string(n) + " = " + to_string(dis(gen));
		}
		tableSelect.where(whereclause);
		state.ResumeTiming();
		conn.startTransaction();
		auto result = tableSelect.execute().fetchAll();
		conn.commit();
	}

	if(state.thread_index == 0) {
		//MySQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_MYSQL_Read_CountTransact)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1,4);
