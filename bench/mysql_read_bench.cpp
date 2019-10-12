#include "benchmark/benchmark.h"
#include "dbphd/mysqldb/mysqldb.hpp"
#include "precalculate.hpp"

#include <random>
#include <iostream>
#include <chrono>

using namespace std;

static void CustomArgumentsInserts(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= Precalculator::Columns; ++i) { // fields to query
		for (int j = 0; j <= i; ++j) { //  Indexes
			b->Args({i, j});
		}
	}
}

static void CustomArgumentsInserts2(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= Precalculator::Columns; ++i) { // fields to query
		for (int j = 0; j <= Precalculator::Columns; ++j) { // fields to return
			for (int k = 0; k <= i; ++k) { //  Indexes
				for(int l = 1; l <= (int)pow(Precalculator::Values,Precalculator::Columns-i); l *= 2) { // Documents to return
					b->Args({i, j, k, l});
				}
			}
		}
	}
}

static void CustomArgumentsInserts3(benchmark::internal::Benchmark* b) {
	for (int i = 1; i <= Precalculator::Rows; i *= 2) { // documents to process
		b->Args({i});
	}
}

static void CustomArgumentsInserts4(benchmark::internal::Benchmark* b) {
	for (int i = 1; i <= Precalculator::Columns; ++i) { // fields to sort
		for (int k = 0; k <= i; ++k) { //  Indexes
			for(int l = 1; l <= (int)pow(Precalculator::Values,Precalculator::Columns); l *= 2) { // Documents to return
				b->Args({i, k, l});
			}
		}
	}
}

static void CustomArgumentsInserts5(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= 2; ++i) { // fields to index
		for(int l = 1; l <= (int)pow(Precalculator::Values,Precalculator::Columns); l *= 2) { // Documents to return
			b->Args({i, l});
		}
	}
}

static void CustomArgumentsInserts6(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= 2; ++i) { // fields to index
		for(int j = 1; j <= (int)pow(Precalculator::Values,Precalculator::Columns); j *= 2) { // Documents to return
			for(int k = 1; k <= j; k *= 2) { // Documents to query for batch
				b->Args({i, j, k});
			}
		}
	}
}

static void CreateTable(mysqlx::Session &conn) {
	static volatile bool created = false;
	if(!created) {
		cout << endl << "Creating Mysql Read Table...";
		cout.flush();
		auto start = chrono::steady_clock::now();
		MySQLDBHandler::CreateDatabase(conn, "bench");
		MySQLDBHandler::DropTable(conn, "bench", "read_bench");
		string createQuery = R"|(
CREATE TABLE read_bench (
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
		auto table = db.getTable("read_bench");

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
	}
	created = true;
}

static void BM_MYSQL_Read_Count(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		auto tableSelect = table.select("COUNT(*)");
		if(state.range(0) > 0) {
			string whereclause = "a0 = " + to_string(dis(gen));
			for(int n = 1; n < state.range(0); ++n) {
				whereclause += " AND a" + to_string(n) + " = " + to_string(dis(gen));
			}
			tableSelect.where(whereclause);
		}
		state.ResumeTiming();
		auto result = tableSelect.execute().fetchOne();
		count += result.get(0).get<uint64_t>();
	}

	if(state.thread_index == 0) {
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

BENCHMARK(BM_MYSQL_Read_Count)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_Count_Transact(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		auto tableSelect = table.select("COUNT(*)");
		if(state.range(0) > 0) {
			string whereclause = "a0 = " + to_string(dis(gen));
			for(int n = 1; n < state.range(0); ++n) {
				whereclause += " AND a" + to_string(n) + " = " + to_string(dis(gen));
			}
			tableSelect.where(whereclause);
		}
		state.ResumeTiming();
		conn.startTransaction();
		auto result = tableSelect.execute().fetchOne();
		count += result.get(0).get<uint64_t>();
		conn.commit();
	}

	if(state.thread_index == 0) {
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

BENCHMARK(BM_MYSQL_Read_Count_Transact)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Reads(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
		if(state.range(2) > 0) {
			string indexCreate = "CREATE INDEX idx on bench.read_bench (a0";
			for(int index = 1; index < state.range(2); ++index) {
				indexCreate += ",a" + to_string(index);
			}
			indexCreate += ") ALGORITHM INPLACE;";
			conn.sql(indexCreate).execute();
		}
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("read_bench");
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		std::list<string> selectclause = {"_id"};
		for(int i = 0; i < state.range(1); ++i) {
			selectclause.push_back("a" + to_string(i));
		}

		auto tableSelect = table.select(selectclause);
		if(state.range(0) > 0) {
			string whereclause = "a0 = " + to_string(dis(gen));
			for(int n = 1; n < state.range(0); ++n) {
				whereclause += " AND a" + to_string(n) + " = " + to_string(dis(gen));
			}
			tableSelect.where(whereclause);
		}
		tableSelect.limit(state.range(3));
		state.ResumeTiming();
		std::list<mysqlx::Row> result = tableSelect.execute().fetchAll();
		for(auto res: result) {
			++count;
		}
	}

	if(state.thread_index == 0) {
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
	state.counters.insert({{"Fields", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"FieldsProj", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(3), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MYSQL_Reads)->Apply(CustomArgumentsInserts2)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Reads_Transact(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
		if(state.range(2) > 0) {
			string indexCreate = "CREATE INDEX idx on bench.read_bench (a0";
			for(int index = 1; index < state.range(2); ++index) {
				indexCreate += ",a" + to_string(index);
			}
			indexCreate += ") ALGORITHM INPLACE;";
			conn.sql(indexCreate).execute();
		}
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("read_bench");
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		std::list<string> selectclause = {"_id"};
		for(int i = 0; i < state.range(1); ++i) {
			selectclause.push_back("a" + to_string(i));
		}
		auto tableSelect = table.select(selectclause);
		if(state.range(0) > 0) {
			string whereclause = "a0 = " + to_string(dis(gen));
			for(int n = 1; n < state.range(0); ++n) {
				whereclause += " AND a" + to_string(n) + " = " + to_string(dis(gen));
			}
			tableSelect.where(whereclause);
		}
		tableSelect.limit(state.range(3));
		state.ResumeTiming();
		conn.startTransaction();
		std::list<mysqlx::Row> result = tableSelect.execute().fetchAll();
		for(auto res: result) {
			++count;
		}
		conn.commit();
	}

	if(state.thread_index == 0) {
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
	state.counters.insert({{"Fields", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"FieldsProj", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(3), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MYSQL_Reads_Transact)->Apply(CustomArgumentsInserts2)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_Sum(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("read_bench");
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT SUM(new.a0)";
		string query = selectclause + " FROM (SELECT * FROM bench.read_bench";
		query += " LIMIT " + to_string(state.range(0));
		query += ") new;";
		state.ResumeTiming();
		std::list<mysqlx::Row> result = conn.sql(query).execute().fetchAll();
		for(auto res: result) {
		}
	}

	if(state.thread_index == 0) {
		//MySQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}
	state.SetComplexityN(state.range(0));

	state.SetItemsProcessed(state.range(0)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_MYSQL_Read_Sum)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_Sum_Transact(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("read_bench");
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT SUM(new.a0)";
		string query = selectclause + " FROM (SELECT * FROM bench.read_bench";
		query += " LIMIT " + to_string(state.range(0));
		query += ") new;";
		state.ResumeTiming();
		conn.startTransaction();
		std::list<mysqlx::Row> result = conn.sql(query).execute().fetchAll();
		for(auto res: result) {
		}
		conn.commit();
	}

	if(state.thread_index == 0) {
		//MySQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}
	state.SetComplexityN(state.range(0));

	state.SetItemsProcessed(state.range(0)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_MYSQL_Read_Sum_Transact)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_Avg(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("read_bench");
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT AVG(new.a0)";
		string query = selectclause + " FROM (SELECT * FROM bench.read_bench";
		query += " LIMIT " + to_string(state.range(0));
		query += ") new;";
		state.ResumeTiming();
		std::list<mysqlx::Row> result = conn.sql(query).execute().fetchAll();
		for(auto res: result) {
		}
	}

	if(state.thread_index == 0) {
		//MySQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}
	state.SetComplexityN(state.range(0));

	state.SetItemsProcessed(state.range(0)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_MYSQL_Read_Avg)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_Avg_Transact(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("read_bench");
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT AVG(new.a0)";
		string query = selectclause + " FROM (SELECT * FROM bench.read_bench";
		query += " LIMIT " + to_string(state.range(0));
		query += ") new;";
		state.ResumeTiming();
		conn.startTransaction();
		std::list<mysqlx::Row> result = conn.sql(query).execute().fetchAll();
		for(auto res: result) {
		}
		conn.commit();
	}

	if(state.thread_index == 0) {
		//MySQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}

	state.SetComplexityN(state.range(0));
	state.SetItemsProcessed(state.range(0)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_MYSQL_Read_Avg_Transact)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_Mul(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("read_bench");
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "a0*2";
		auto tableSelect = table.select(selectclause);
		tableSelect.limit(state.range(0));
		state.ResumeTiming();
		std::list<mysqlx::Row> result = tableSelect.execute().fetchAll();
		for(auto res: result) {
		}
	}

	if(state.thread_index == 0) {
		//MySQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}

	state.SetComplexityN(state.range(0));
	state.SetItemsProcessed(state.range(0)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_MYSQL_Read_Mul)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_Mul_Transact(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
	}
	auto db = conn.getSchema("bench");
	auto table = db.getTable("read_bench");
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "a0*2";
		auto tableSelect = table.select(selectclause);
		tableSelect.limit(state.range(0));
		state.ResumeTiming();
		conn.startTransaction();
		std::list<mysqlx::Row> result = tableSelect.execute().fetchAll();
		for(auto res: result) {
		}
		conn.commit();
	}

	if(state.thread_index == 0) {
		//MySQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}

	state.SetComplexityN(state.range(0));
	state.SetItemsProcessed(state.range(0)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_MYSQL_Read_Mul_Transact)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_Sort(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		std::list<string> selectclause = {"*"};

		auto tableSelect = table.select(selectclause);
		if(state.range(0) > 0) {
			std::list<string> orderclause = {"a0 DESC"};
			for(int n = 1; n < state.range(0); ++n) {
				orderclause.push_back("a" + to_string(n) + " DESC");
			}
			tableSelect.orderBy(orderclause);
		}
		tableSelect.limit(state.range(2));
		state.ResumeTiming();
		std::list<mysqlx::Row> result = tableSelect.execute().fetchAll();
		for(auto res: result) {
			++count;
		}
	}

	if(state.thread_index == 0) {
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
	state.counters.insert({{"Fields", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MYSQL_Read_Sort)->Apply(CustomArgumentsInserts4)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_Sort_Transact(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		std::list<string> selectclause = {"*"};

		auto tableSelect = table.select(selectclause);
		if(state.range(0) > 0) {
			std::list<string> orderclause = {"a0 DESC"};
			for(int n = 1; n < state.range(0); ++n) {
				orderclause.push_back("a" + to_string(n) + " DESC");
			}
			tableSelect.orderBy(orderclause);
		}
		tableSelect.limit(state.range(2));
		state.ResumeTiming();
		conn.startTransaction();
		std::list<mysqlx::Row> result = tableSelect.execute().fetchAll();
		for(auto res: result) {
			++count;
		}
		conn.commit();
	}

	if(state.thread_index == 0) {
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
	state.counters.insert({{"Fields", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MYSQL_Read_Sort_Transact)->Apply(CustomArgumentsInserts4)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_Join(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
		for(int index = 0; index < state.range(0); ++index) {
			string indexCreate = "CREATE INDEX idx on bench.read_bench a" + to_string(index) + " ALGORITHM INPLACE;";
			conn.sql(indexCreate).execute();
		}
	}
	auto db = conn.getSchema("bench");
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT *";
		string query = selectclause + " FROM bench.read_bench b1 INNER JOIN bench.read_bench b2 ON b1.a0 = b2.a1 AND b1._id != b2._id ";
		query += " LIMIT " + to_string(state.range(1));
		query += ";";
		state.ResumeTiming();
		std::list<mysqlx::Row> result = conn.sql(query).execute().fetchAll();
		for(auto res: result) {
			++count;
		}
	}

	if(state.thread_index == 0) {
		//MySQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}

	state.SetComplexityN(state.range(1));
	state.SetItemsProcessed(state.range(1)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Indexes", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MYSQL_Read_Join)->Apply(CustomArgumentsInserts5)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_Join_Transact(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
		for(int index = 0; index < state.range(0); ++index) {
			string indexCreate = "CREATE INDEX idx on bench.read_bench a" + to_string(index) + " ALGORITHM INPLACE;";
			conn.sql(indexCreate).execute();
		}
	}
	auto db = conn.getSchema("bench");
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT *";
		string query = selectclause + " FROM bench.read_bench b1 INNER JOIN bench.read_bench b2 ON b1.a0 = b2.a1 AND b1._id != b2._id ";
		query += " LIMIT " + to_string(state.range(1));
		query += ";";
		state.ResumeTiming();
		conn.startTransaction();
		std::list<mysqlx::Row> result = conn.sql(query).execute().fetchAll();
		for(auto res: result) {
			++count;
		}
		conn.commit();
	}

	if(state.thread_index == 0) {
		//MySQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}

	state.SetComplexityN(state.range(1));
	state.SetItemsProcessed(state.range(1)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?;
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Indexes", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MYSQL_Read_Join_Transact)->Apply(CustomArgumentsInserts5)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_Join_Manual(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
		for(int index = 0; index < state.range(0); ++index) {
			string indexCreate = "CREATE INDEX idx on bench.read_bench a" + to_string(index) + " ALGORITHM INPLACE;";
			conn.sql(indexCreate).execute();
		}
	}
	auto db = conn.getSchema("bench");
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT *";
		string query = selectclause + " FROM bench.read_bench";
		query += " LIMIT " + to_string(state.range(1));
		query += ";";
		vector<mysqlx::Row> batch;
		batch.reserve(state.range(2));
		vector<pair<mysqlx::Row, vector<mysqlx::Row>>> results;
		results.reserve(state.range(1));
		state.ResumeTiming();

		auto res = conn.sql(query).execute();
		for(auto i: res) {
			batch.push_back(i);
			// Our batch is ready...
			if(batch.size() == state.range(2)) {
				string selectclause = "SELECT *";
				string query = selectclause + " FROM bench.read_bench WHERE	";
				bool first = true;
				for(auto querydoc: batch) {
					vector<mysqlx::Row> a;
					results.push_back(make_pair(querydoc, a));
					if(!first)
						query += " OR ";
					query += " (a1 = " + to_string(querydoc.get(1).get<uint32_t>()) + " AND _id != " + to_string(querydoc.get(0).get<uint32_t>());
					query += ") ";
					first = false;
				}
				query += " LIMIT " + to_string(state.range(1));
				query += ";";
				auto cursorinternal = conn.sql(query).execute();
				for(auto row: cursorinternal) {
					for(auto res: results) {
						if(res.first.get(1).get<uint32_t>() == row.get(0).get<uint32_t>()) {
							res.second.push_back(row);
							++count;
							break;
						}
					}
					if(count >= state.range(1))
						break;
				}
				batch.clear();
			}
			if(count >= state.range(1))
				break;
		}
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}
	state.SetComplexityN(state.range(1));

	state.SetItemsProcessed(state.range(1)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Indexes", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Batch", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MYSQL_Read_Join_Manual)->Apply(CustomArgumentsInserts6)->Complexity()->DenseThreadRange(1,4);

static void BM_MYSQL_Read_Join_Manual_Transact(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn);
		try {
			conn.sql("DROP INDEX idx ON bench.read_bench;").execute();
		}catch(...) {
		}
		for(int index = 0; index < state.range(0); ++index) {
			string indexCreate = "CREATE INDEX idx on bench.read_bench a" + to_string(index) + " ALGORITHM INPLACE;";
			conn.sql(indexCreate).execute();
		}
	}
	auto db = conn.getSchema("bench");
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT *";
		string query = selectclause + " FROM bench.read_bench";
		query += " LIMIT " + to_string(state.range(1));
		query += ";";
		vector<mysqlx::Row> batch;
		batch.reserve(state.range(2));
		vector<pair<mysqlx::Row, vector<mysqlx::Row>>> results;
		results.reserve(state.range(1));
		state.ResumeTiming();
		conn.startTransaction();
		auto res = conn.sql(query).execute();
		for(auto i: res) {
			batch.push_back(i);
			// Our batch is ready...
			if(batch.size() == state.range(2)) {
				string selectclause = "SELECT *";
				string query = selectclause + " FROM bench.read_bench WHERE	";
				bool first = true;
				for(auto querydoc: batch) {
					vector<mysqlx::Row> a;
					results.push_back(make_pair(querydoc, a));
					if(!first)
						query += " OR ";
					query += " (a1 = " + to_string(querydoc.get(1).get<uint32_t>()) + " AND _id != " + to_string(querydoc.get(0).get<uint32_t>());
					query += ") ";
					first = false;
				}
				query += " LIMIT " + to_string(state.range(1));
				query += ";";
				auto cursorinternal = conn.sql(query).execute();
				for(auto row: cursorinternal) {
					for(auto res: results) {
						if(res.first.get(1).get<uint32_t>() == row.get(0).get<uint32_t>()) {
							res.second.push_back(row);
							++count;
							break;
						}
					}
					if(count >= state.range(1))
						break;
				}
				batch.clear();
			}
			if(count >= state.range(1))
				break;
		}
		conn.commit();
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
		// This is the first thread, so do destruction here (delete documents etc..)
	}
	state.SetComplexityN(state.range(1));

	state.SetItemsProcessed(state.range(1)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Indexes", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Batch", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MYSQL_Read_Join_Manual_Transact)->Apply(CustomArgumentsInserts6)->Complexity()->DenseThreadRange(1,4);
