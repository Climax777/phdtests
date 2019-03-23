#include "benchmark/benchmark.h"
#include "dbphd/mysqldb/mysqldb.hpp"

static void BM_MYSQL_Select(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	const std::string query = "select 500";
	for(auto _ : state) {
		auto querystatement = conn.sql(query);
		auto result = querystatement.execute();
	}
}

BENCHMARK(BM_MYSQL_Select);

static void BM_MYSQL_SelectTransact(benchmark::State& state) {
	auto conn = MySQLDBHandler::GetConnection();	
	const std::string query = "select 500";
	for(auto _ : state) {
		conn.startTransaction();
		auto querystatement = conn.sql(query);
		auto result = querystatement.execute();
		conn.commit();
	}
}

BENCHMARK(BM_MYSQL_SelectTransact);
