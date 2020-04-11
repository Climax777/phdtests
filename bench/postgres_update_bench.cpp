#include "benchmark/benchmark.h"
#include "dbphd/postgresql/postgresql.hpp"
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

static void CreateTable(std::shared_ptr<pqxx::connection> conn, bool doublefields = false) {
	static volatile bool created = false;
	if(!created) {
		cout << endl << "Creating Postgres Update Collection...";
		cout.flush();
		auto start = chrono::steady_clock::now();
		try{
			PostgreSQLDBHandler::CreateDatabase(conn, "bench");
		} catch(...) {

		}
		PostgreSQLDBHandler::DropTable(conn, "bench", "update_bench");
		string createQuery = R"|(
CREATE TABLE update_bench (
	_id  serial PRIMARY KEY,
)|";
		for(int field = 0; field < Precalculator::Columns; ++field) {
			createQuery.append("a" + to_string(field) + " INT");
			if(field != Precalculator::Columns-1)
				createQuery.append(",\r\n");
		}
		if(doublefields) {
			createQuery.append(",\r\n");
			for(int field = 0; field < Precalculator::Columns; ++field) {
				createQuery.append("b" + to_string(field) + " INT");
				if(field != Precalculator::Columns-1)
					createQuery.append(",\r\n");
			}
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
			string query = "INSERT INTO bench.update_bench VALUES\r\n";
			for(int j = 0; j < batchsize && i+j < Precalculator::Rows;++j) {
				query.append("	(DEFAULT");
				int iteration = i+j;
				auto& rowval = Precalculator::PrecalcValues[iteration];
				for(int f = 0; f < Precalculator::Columns; ++f) {
					query.append("," + to_string(rowval[f]));
				}
				if(doublefields) {
					for(int f = 0; f < Precalculator::Columns; ++f) {
						query.append("," + to_string(rowval[f]));
					}
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
		auto end = chrono::steady_clock::now();
		cout<< " Done in " << chrono::duration <double, milli> (end-start).count() << " ms" << endl << endl;
		cout.flush();
	}
	created = true;
}

static void BM_PQXX_Update(benchmark::State& state, bool transactions, bool testwriteindexes) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	std::uniform_int_distribution<> dis2(1, 100);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateTable(conn, true);
		try {
			string query = "DROP INDEX IF EXISTS bench.update_bench_idx;";
			pqxx::nontransaction N(*conn);
			pqxx::result R(N.exec(query));
		}catch(...) {
		}
		try {
			string query = "DROP INDEX IF EXISTS bench.update_bench_idx2;";
			pqxx::nontransaction N(*conn);
			pqxx::result R(N.exec(query));
		}catch(...) {
		}
		if(testwriteindexes) {
			if(state.range(0) > 0) {
				string indexCreate = "CREATE INDEX update_bench_idx on bench.update_bench (a0";
				for(int index = 1; index < state.range(0); ++index) {
					indexCreate += ",a" + to_string(index);
				}
				indexCreate += ");";
				pqxx::nontransaction N(*conn);
				pqxx::result R(N.exec(indexCreate));
			}
			if(state.range(2) > 0) {
				string indexCreate = "CREATE INDEX update_bench_idx2 on bench.update_bench (b0";
				for(int index = 1; index < state.range(2); ++index) {
					indexCreate += ",b" + to_string(index);
				}
				indexCreate += ");";
				pqxx::nontransaction N(*conn);
				pqxx::result R(N.exec(indexCreate));
			}
		} else {
			if(state.range(2) > 0) {
				string indexCreate = "CREATE INDEX update_bench_idx on bench.update_bench (a0";
				for(int index = 1; index < state.range(2); ++index) {
					indexCreate += ",a" + to_string(index);
				}
				indexCreate += ");";
				pqxx::nontransaction N(*conn);
				pqxx::result R(N.exec(indexCreate));
			}
		}
	}
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		string query = "UPDATE bench.update_bench SET b0 = b0 + " + to_string(dis2(gen));
		for(int i = 1; i < state.range(1); ++i) {
			query += ",b" + to_string(i) + " = b" + to_string(i) + " + " + to_string(dis2(gen));
		}
		if(state.range(0) > 0)  {
			query += " WHERE\r\n a0 = " + to_string(dis(gen));
			for(int n = 1; n < state.range(0); ++n) {
				query += " AND a" + to_string(n) + " = " + to_string(dis(gen));
			}
		}
		query += ";";
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
		auto res = T->exec(query);
		count += res.affected_rows();
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
	state.counters.insert({{"FieldsQueried", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"FieldsChanged", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK_CAPTURE(BM_PQXX_Update, Normal, false, false)->Apply(CustomArgumentsUpdates)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_PQXX_Update, Transact, true, false)->Apply(CustomArgumentsUpdates)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_PQXX_Update, NormalWriteIdx, false, true)->Apply(CustomArgumentsUpdates2)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_PQXX_Update, TransactWriteIdx, true, true)->Apply(CustomArgumentsUpdates2)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
