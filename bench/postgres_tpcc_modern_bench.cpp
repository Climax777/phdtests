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
/*static void BM_PQXX_Select(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	  const std::string query = "select 500";
	  for(auto _ : state) {
		  pqxx::nontransaction N(*conn);
		  pqxx::result R(N.exec(query));
	  }
}

BENCHMARK(BM_PQXX_Select);

static void BM_PQXX_SelectTransact(benchmark::State& state) {
	auto conn = PostgreSQLDBHandler::GetConnection();
	  const std::string query = "select 500";
	  for(auto _ : state) {
		  pqxx::work w(*conn);
		  // work::exec1() executes a query returning a single row of data.
		  // We'll just ask the database to return the number 1 to us.
		  pqxx::row r = w.exec1("SELECT 500");
		  // Commit your transaction.  If an exception occurred before this
		  // point, execution will have left the block, and the transaction will
		  // have been destroyed along the way.  In that case, the failed
		  // transaction would implicitly abort instead of getting to this point.
		  w.commit();
	  }
}

BENCHMARK(BM_PQXX_SelectTransact);*/

/*
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
}*/