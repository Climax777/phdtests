#include "benchmark/benchmark.h"
#include "dbphd/postgresql/postgresql.hpp"

static void BM_PQXX_Select(benchmark::State& state) {
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

BENCHMARK(BM_PQXX_SelectTransact);
