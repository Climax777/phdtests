#include "benchmark/benchmark.h"
#include "dbphd/postgresql/postgresql.hpp"
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

static void CreateTable(std::shared_ptr<pqxx::connection> conn, std::string postfix = "") {
	static unordered_map<string, bool> created;
	if(!created.count(postfix)) {
		cout << endl << "Creating Postgres Delete Table " << postfix << "...";
		cout.flush();
		auto start = chrono::steady_clock::now();
		try{
			PostgreSQLDBHandler::CreateDatabase(conn, "bench");
		} catch(...) {

		}
		PostgreSQLDBHandler::DropTable(conn, "bench", "delete_bench"+postfix);
		string createQuery = R"|(
CREATE TABLE )|" + string("delete_bench")+postfix + R"|( (
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
			string query = "INSERT INTO bench.delete_bench" + postfix + " VALUES\r\n";
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
		auto end = chrono::steady_clock::now();
		cout<< " Done in " << chrono::duration <double, milli> (end-start).count() << " ms" << endl << endl;
		cout.flush();
		created[postfix] = true;
	}
}

static void BM_PQXX_Delete(benchmark::State& state, bool transactions) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	string postfix = std::to_string(state.thread_index);
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
		// This is the first thread, so do initialization here, build indexes etc...
	CreateTable(conn, postfix);
	try {
		string query = "DROP INDEX IF EXISTS bench.delete_bench"+postfix+"_idx;";
		pqxx::nontransaction N(*conn);
		pqxx::result R(N.exec(query));
	}catch(...) {
	}
	if(state.range(1) > 0) {
		string indexCreate = "CREATE INDEX delete_bench"+postfix+"_idx on bench.delete_bench" + postfix + " (a0";
		for(int index = 1; index < state.range(1); ++index) {
			indexCreate += ",a" + to_string(index);
		}
		indexCreate += ");";
		pqxx::nontransaction N(*conn);
		pqxx::result R(N.exec(indexCreate));
	}
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		string query = "SELECT * FROM bench.delete_bench"+postfix;
		string whereclause = "";
		if(state.range(0) > 0)  {
			whereclause += " WHERE\r\n a0 = " + to_string(dis(gen));
			for(int n = 1; n < state.range(0); ++n) {
				whereclause += " AND a" + to_string(n) + " = " + to_string(dis(gen));
			}
		}
		pqxx::result results;
		{
			pqxx::nontransaction N(*conn);
			results = N.exec(query+whereclause);
		}

		query = "DELETE FROM bench.delete_bench"+postfix;
		query += whereclause;
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
		{
			pqxx::nontransaction N(*conn);
			string query = "INSERT INTO bench.delete_bench"+postfix+" VALUES\r\n";
			for(int n = 0; n < results.size(); ++n) {
				query.append("	(");
				for(int fields = 0; fields < results[n].size(); ++fields) {
					query.append(results[n][fields].c_str());
					if(fields != results[n].size()-1) 
						query.append(",");
				}
				if(n != results.size()-1) {
					query.append("),");
				} else {
					query.append(");"); // RETURNING _id // Technically to make it more fair, pqxx should return the id's of the inserteds
				}
			}
		}
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

BENCHMARK_CAPTURE(BM_PQXX_Delete, Normal, false)->Apply(CustomArgumentsDeletes)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_PQXX_Delete, Transact, true)->Apply(CustomArgumentsDeletes)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();

