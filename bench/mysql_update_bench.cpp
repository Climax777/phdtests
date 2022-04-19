#include "benchmark/benchmark.h"
#include "dbphd/mysqldb/mysqldb.hpp"
#include "mysqlx/devapi/document.h"
#include "precalculate.hpp"

#include <random>
#include <iostream>
#include <chrono>

using namespace std;

static void CustomArgumentsUpdates(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= Precalculator::Columns; ++i) { // fields to query
		for (int j = 1; j <= Precalculator::Columns; ++j) { // fields to write
			for (int k = 0; k <= i; ++k) { //  Indexes
				b->Args({i, j, k});
			}
		}
	}
}

static void CustomArgumentsUpdates2(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= Precalculator::Columns; ++i) { // fields to query
		for (int j = 1; j <= Precalculator::Columns; ++j) { // fields to write
			for (int k = 0; k <= j; ++k) { //  Indexes
				b->Args({i, j, k});
			}
		}
	}
}

static void CreateTable(mysqlx::Session &conn, bool doublefields = false) {
	static volatile bool created = false;
	if(!created) {
		cout << endl << "Creating Mysql Update Table...";
		cout.flush();
		auto start = chrono::steady_clock::now();
		MySQLDBHandler::CreateDatabase(conn, "bench");
		MySQLDBHandler::DropTable(conn, "bench", "update_bench");
		string createQuery = R"|(
CREATE TABLE update_bench (
	_id  INT AUTO_INCREMENT,
	a0 INT,
)|";
		for(int field = 1; field < Precalculator::Columns; ++field) {
			createQuery.append("a" + to_string(field) + " INT,");
		}
		if(doublefields) {
			for(int field = 0; field < Precalculator::Columns; ++field) {
				createQuery.append("b" + to_string(field) + " INT,");
			}
		}
		createQuery.append(R"|(
	PRIMARY KEY(_id)
) ENGINE=INNODB;
)|");
		conn.sql(createQuery).execute();
		auto db = conn.getSchema("bench");
		auto table = db.getTable("update_bench");

		int batchsize = 10000;
		for(int i = 0; i < Precalculator::Rows; i += batchsize) {
			auto tableInsert = table.insert();
			for(int j = 0; j < batchsize && i+j < Precalculator::Rows;++j) {
				mysqlx::Row row;
				row.set(0, mysqlx::nullvalue);
				int iteration = i+j;
				auto& rowval = Precalculator::PrecalcValues[iteration];
				for(int f = 1; f <= Precalculator::Columns; ++f) {
					row.set(f, rowval[f-1]);
					if(doublefields)
						row.set(f+Precalculator::Columns, rowval[f-1]);
				}
				tableInsert.rows(row);
			}
			auto result = tableInsert.execute();
		}
		auto end = chrono::steady_clock::now();
		cout<< " Done in " << chrono::duration <double, milli> (end-start).count() << " ms" << endl << endl;
		cout.flush();
	}
	created = true;
}

static void BM_MYSQL_Update(benchmark::State& state, bool transactions, bool testwriteindexes) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	std::uniform_int_distribution<> dis2(1,100);
	// Per thread settings...
	if(state.thread_index() == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn, true);
		try {
			conn.sql("DROP INDEX idx ON bench.update_bench;").execute();
		}catch(...) {
		}
		try {
			conn.sql("DROP INDEX idx2 ON bench.update_bench;").execute();
		}catch(...) {
		}
		if(testwriteindexes) {
			if(state.range(0) > 0) {
				string indexCreate = "CREATE INDEX idx on bench.update_bench (a0";
				for(int index = 1; index < state.range(0); ++index) {
					indexCreate += ",a" + to_string(index);
				}
				indexCreate += ") ALGORITHM INPLACE;";
				conn.sql(indexCreate).execute();
			}
			if(state.range(2) > 0) {
				string indexCreate = "CREATE INDEX idx2 on bench.update_bench (b0";
				for(int index = 1; index < state.range(2); ++index) {
					indexCreate += ",b" + to_string(index);
				}
				indexCreate += ") ALGORITHM INPLACE;";
				conn.sql(indexCreate).execute();
			}
		} else {
			if(state.range(2) > 0) {
				string indexCreate = "CREATE INDEX idx on bench.update_bench (a0";
				for(int index = 1; index < state.range(2); ++index) {
					indexCreate += ",a" + to_string(index);
				}
				indexCreate += ") ALGORITHM INPLACE;";
				conn.sql(indexCreate).execute();
			}
		}
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("update_bench");
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();

		auto tableUpdate = table.update();
		if(state.range(0) > 0) {
			string whereclause = "a0 = " + to_string(dis(gen));
			for(int n = 1; n < state.range(0); ++n) {
				whereclause += " AND a" + to_string(n) + " = " + to_string(dis(gen));
			}
			tableUpdate.where(whereclause);
		}
		for(int i = 0; i < state.range(1); ++i) {
			auto field = "b" + to_string(i);
			tableUpdate.set(field, mysqlx::expr(field + " + " + to_string(dis2(gen))));
		}
		state.ResumeTiming();
		auto start = std::chrono::high_resolution_clock::now();
		if(transactions) {
			conn.startTransaction();
		}
		count += tableUpdate.execute().getAffectedItemsCount();
		if(transactions) {
			conn.commit();
		}
		auto end = std::chrono::high_resolution_clock::now();

		auto elapsed_seconds =
			std::chrono::duration_cast<std::chrono::duration<double>>(
					end - start);

		state.SetIterationTime(elapsed_seconds.count());
	}

	if(state.thread_index() == 0) {
		//MySQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}

	state.SetItemsProcessed(count);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"FieldsQueried", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"FieldsChanged", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK_CAPTURE(BM_MYSQL_Update, Normal, false, false)->Apply(CustomArgumentsUpdates)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_MYSQL_Update, Transact, true, false)->Apply(CustomArgumentsUpdates)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_MYSQL_Update, NormalWriteIdx, false, true)->Apply(CustomArgumentsUpdates2)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_MYSQL_Update, TransactWriteIdx, true, true)->Apply(CustomArgumentsUpdates2)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
