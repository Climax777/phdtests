#include "benchmark/benchmark.h"
#include "dbphd/mysqldb/mysqldb.hpp"
#include "precalculate.hpp"

#include <random>
#include <iostream>
#include <chrono>

using namespace std;

static void CustomArgumentsDeletes(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= Precalculator::Columns; ++i) { // fields to query
		for (int j = 0; j <= i; ++j) { //  Indexes
			b->Args({i, j});
		}
	}
}

static void CreateTable(mysqlx::Session &conn, std::string postfix = "") {
	static unordered_map<string, bool> created;
	if(!created.count(postfix)) {
		cout << endl << "Creating Mysql Delete Table " << postfix << "...";
		cout.flush();
		auto start = chrono::steady_clock::now();
		MySQLDBHandler::CreateDatabase(conn, "bench");
		MySQLDBHandler::DropTable(conn, "bench", "delete_bench" + postfix);
		string createQuery = R"|(
CREATE TABLE )|" + string("delete_bench") + postfix +R"|( (
	_id  INT AUTO_INCREMENT,
	a0 INT,
)|";
		for(int field = 1; field < Precalculator::Columns; ++field) {
			createQuery.append("a" + to_string(field) + " INT,");
		}
		createQuery.append(R"|(
	PRIMARY KEY(_id)
) ENGINE=INNODB;
)|");
		conn.sql(createQuery).execute();
		auto db = conn.getSchema("bench");
		auto table = db.getTable("delete_bench"+postfix);

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
				}
				tableInsert.rows(row);
			}
			auto result = tableInsert.execute();
		}
		auto end = chrono::steady_clock::now();
		cout<< " Done in " << chrono::duration <double, milli> (end-start).count() << " ms" << endl << endl;
		cout.flush();
		created[postfix] = true;
	}
}

static void BM_MYSQL_Delete(benchmark::State& state, bool transactions) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::string postfix = std::to_string(state.thread_index());
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
		// This is the first thread, so do initialization here, build indexes etc...
	CreateTable(conn, postfix);
	try {
		conn.sql("DROP INDEX idx ON bench.delete_bench"+postfix+";").execute();
	}catch(...) {
	}
	if(state.range(1) > 0) {
		string indexCreate = "CREATE INDEX idx on bench.delete_bench" + postfix +" (a0";
		for(int index = 1; index < state.range(1); ++index) {
			indexCreate += ",a" + to_string(index);
		}
		indexCreate += ") ALGORITHM INPLACE;";
		conn.sql(indexCreate).execute();
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("delete_bench"+postfix);
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		auto tablePrequery = table.select("*");
		auto tableDelete = table.remove();
		if(state.range(0) > 0) {
			string whereclause = "a0 = " + to_string(dis(gen));
			for(int n = 1; n < state.range(0); ++n) {
				whereclause += " AND a" + to_string(n) + " = " + to_string(dis(gen));
			}
			tableDelete.where(whereclause);
			tablePrequery.where(whereclause);
		}
		std::deque<mysqlx::Row> deleters = tablePrequery.execute().fetchAll();
		
		state.ResumeTiming();
		auto start = std::chrono::high_resolution_clock::now();
		if(transactions) {
			conn.startTransaction();
		}
		auto result = tableDelete.execute().getAffectedItemsCount();
		count += result;
		if(transactions) {
			conn.commit();
		}
		auto end = std::chrono::high_resolution_clock::now();

		auto elapsed_seconds =
			std::chrono::duration_cast<std::chrono::duration<double>>(
					end - start);

		state.SetIterationTime(elapsed_seconds.count());
		// TODO This is SLOW
		for(auto i = 0; i< deleters.size(); i+=10000) {
			auto tableReinserter = table.insert();
			for(int j = 0; j < 10000 && i+j < deleters.size(); ++j) {
				tableReinserter.rows(deleters[i+j]);
			}
			tableReinserter.execute();
		}
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
	state.counters.insert({{"Fields", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK_CAPTURE(BM_MYSQL_Delete, Normal, false)->Apply(CustomArgumentsDeletes)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_MYSQL_Delete, Transact, true)->Apply(CustomArgumentsDeletes)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
