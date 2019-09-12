#include "benchmark/benchmark.h"
#include "dbphd/postgresql/postgresql.hpp"
#include "precalculate.hpp"

#include <random>
#include <iostream>

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

static void CreateTable(std::shared_ptr<pqxx::connection> conn) {
	static volatile bool created = false;
	if(!created) {
		cout << "Creating Postgres Read Collection...";
		cout.flush();
		try{
			PostgreSQLDBHandler::CreateDatabase(conn, "bench");
		} catch(...) {

		}
		PostgreSQLDBHandler::DropTable(conn, "bench", "read_bench");
		string createQuery = R"|(
CREATE TABLE read_bench (
	_id  serial PRIMARY KEY,
)|";
		for(int field = 0; field < Precalculator::Columns; ++field) {
			createQuery.append("a" + to_string(field) + " INT");
			if(field != Precalculator::Columns-1)
				createQuery.append(",\r\n");
		}
		createQuery.append(R"|(
);
)|");
		{
			pqxx::nontransaction N(*conn);
			pqxx::result R(N.exec(createQuery));
		}
		
		int batchsize = 10000;

		for(int i = 0; i < Precalculator::Rows; i += batchsize) {
			string query = "INSERT INTO bench.read_bench VALUES\r\n";
			for(int j = 0; j < batchsize && i+j < Precalculator::Rows;++j) {
				query.append("	(DEFAULT");
				int iteration = i+j;
				auto& rowval = Precalculator::PrecalcValues[iteration];
				for(int f = 0; f < Precalculator::Columns; ++f) {
					query.append("," + to_string(rowval[f]));
				}
				if((j != batchsize - 1) && j+i != Precalculator::Rows-1) {
					query.append("),");
				} else {
					query.append(");");
					pqxx::nontransaction N(*conn);
					pqxx::result R(N.exec(query));
				}
			}
		}
		cout << " Done" << endl;
		cout.flush();
	}
	created = true;
}

static void BM_PQXX_Read_Count(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		string query = "SELECT COUNT(*) FROM bench.read_bench";
		if(state.range(0) > 0)  {
			query += " WHERE\r\n a0 = " + to_string(dis(gen));
			for(int n = 1; n < state.range(0); ++n) {
				query += " AND a" + to_string(n) + " = " + to_string(dis(gen));
			}
		}
		state.ResumeTiming();
		pqxx::nontransaction N(*conn);
		auto res = N.exec1(query);
		count += res.at(0).as<uint64_t>();
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_PQXX_Read_Count)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1, 4);

static void BM_PQXX_Read_Count_Transact(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		string query = "SELECT COUNT(*) FROM bench.read_bench";
		if(state.range(0) > 0)  {
			query += " WHERE\r\n a0 = " + to_string(dis(gen));
			for(int n = 1; n < state.range(0); ++n) {
				query += " AND a" + to_string(n) + " = " + to_string(dis(gen));
			}
		}
		state.ResumeTiming();
		pqxx::work w(*conn);
		auto res = w.exec1(query);
		count += res.at(0).as<uint64_t>();
		w.commit();
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_PQXX_Read_Count_Transact)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1, 4);

static void BM_PQXX_Reads(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
		if(state.range(2) > 0) {
			string indexCreate = "CREATE INDEX read_bench_idx on bench.read_bench (a0";
			for(int index = 1; index < state.range(2); ++index) {
				indexCreate += ",a" + to_string(index);
			}
			indexCreate += ");";
			pqxx::nontransaction N(*conn);
			pqxx::result R(N.exec(indexCreate));
		}
	}
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT _id";
		for(int i = 0; i < state.range(1); ++i) {
			selectclause += ",a" + to_string(i);
		}
		string query = selectclause + " FROM bench.read_bench";
		if(state.range(0) > 0)  {
			query += " WHERE\r\n a0 = " + to_string(dis(gen));
			for(int n = 1; n < state.range(0); ++n) {
				query += " AND a" + to_string(n) + " = " + to_string(dis(gen));
			}
		}
		query += " LIMIT " + to_string(state.range(3));
		query += ";";
		state.ResumeTiming();
		pqxx::nontransaction N(*conn);
		auto res = N.exec(query);
		for(auto i: res)
			++count;
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_PQXX_Reads)->Apply(CustomArgumentsInserts2)->Complexity()->DenseThreadRange(1, 4);

static void BM_PQXX_Reads_Transact(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
		if(state.range(2) > 0) {
			string indexCreate = "CREATE INDEX read_bench_idx on bench.read_bench (a0";
			for(int index = 1; index < state.range(2); ++index) {
				indexCreate += ",a" + to_string(index);
			}
			indexCreate += ");";
			pqxx::nontransaction N(*conn);
			pqxx::result R(N.exec(indexCreate));
		}
	}
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT _id";
		for(int i = 0; i < state.range(1); ++i) {
			selectclause += ",a" + to_string(i);
		}
		string query = selectclause + " FROM bench.read_bench";
		if(state.range(0) > 0)  {
			query += " WHERE\r\n a0 = " + to_string(dis(gen));
			for(int n = 1; n < state.range(0); ++n) {
				query += " AND a" + to_string(n) + " = " + to_string(dis(gen));
			}
		}
		query += " LIMIT " + to_string(state.range(3));
		query += ";";
		state.ResumeTiming();
		pqxx::work w(*conn);
		auto res = w.exec(query);
		for(auto i: res)
			++count;
		w.commit();
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_PQXX_Reads_Transact)->Apply(CustomArgumentsInserts2)->Complexity()->DenseThreadRange(1, 4);

static void BM_PQXX_Read_Sum(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
	}
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT SUM(new.a0)";
		string query = selectclause + " FROM (SELECT * FROM bench.read_bench";
		query += " LIMIT " + to_string(state.range(0));
		query += ") new;";
		state.ResumeTiming();
		pqxx::nontransaction N(*conn);
		auto res = N.exec(query);
		for(auto i: res) {}
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_PQXX_Read_Sum)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1, 4);

static void BM_PQXX_Read_Sum_Transact(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
	}
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT SUM(new.a0)";
		string query = selectclause + " FROM (SELECT * FROM bench.read_bench";
		query += " LIMIT " + to_string(state.range(0));
		query += ") new;";
		state.ResumeTiming();
		pqxx::work w(*conn);
		auto res = w.exec(query);
		for(auto i: res) {}
		w.commit();
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_PQXX_Read_Sum_Transact)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1, 4);

static void BM_PQXX_Read_Avg(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
	}
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT AVG(new.a0)";
		string query = selectclause + " FROM (SELECT * FROM bench.read_bench";
		query += " LIMIT " + to_string(state.range(0));
		query += ") new;";
		state.ResumeTiming();
		pqxx::nontransaction N(*conn);
		auto res = N.exec(query);
		for(auto i: res) {}
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_PQXX_Read_Avg)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1, 4);

static void BM_PQXX_Read_Avg_Transact(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
	}
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT AVG(new.a0)";
		string query = selectclause + " FROM (SELECT * FROM bench.read_bench";
		query += " LIMIT " + to_string(state.range(0));
		query += ") new;";
		state.ResumeTiming();
		pqxx::work w(*conn);
		auto res = w.exec(query);
		for(auto i: res) {}
		w.commit();
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_PQXX_Read_Avg_Transact)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1, 4);

static void BM_PQXX_Read_Mul(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
	}
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT a0*2";
		string query = selectclause + " FROM bench.read_bench";
		query += " LIMIT " + to_string(state.range(0));
		query += ";";
		state.ResumeTiming();
		pqxx::nontransaction N(*conn);
		auto res = N.exec(query);
		for(auto i: res) {}
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_PQXX_Read_Mul)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1, 4);

static void BM_PQXX_Read_Mul_Transact(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
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
	}
	for(auto _ : state) {
		state.PauseTiming();
		string selectclause = "SELECT a0*2";
		string query = selectclause + " FROM bench.read_bench";
		query += " LIMIT " + to_string(state.range(0));
		query += ";";
		state.ResumeTiming();
		pqxx::work w(*conn);
		auto res = w.exec(query);
		for(auto i: res) {}
		w.commit();
	}

	if(state.thread_index == 0) {
		//PostgreSQLDBHandler::DropTable(conn, "bench", "create_bench");
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

BENCHMARK(BM_PQXX_Read_Mul_Transact)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1, 4);
