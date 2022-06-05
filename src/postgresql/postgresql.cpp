#include "dbphd/postgresql/postgresql.hpp"

using namespace std;
PostgreSQLDBHandler::PostgreSQLDBHandler() {

}
PostgreSQLDBHandler::~PostgreSQLDBHandler() {

}

std::shared_ptr<pqxx::connection> PostgreSQLDBHandler::GetConnection(std::string connstr) {
	return shared_ptr<pqxx::connection>(new pqxx::connection(connstr));
}

bool PostgreSQLDBHandler::CreateDatabase(std::shared_ptr<pqxx::connection> conn, std::string dbname) {
	pqxx::nontransaction N(*conn);
	// Using schemas as databases
	N.exec0("create schema " + N.esc(dbname));
	N.exec0("set search_path to " + N.esc(dbname) + ", \"$user\", public");
	pqxx::row r = N.exec1("select 1 as result from pg_namespace where nspname=" + N.quote(dbname));
	return r[0].as<int>() == 1;
}

bool PostgreSQLDBHandler::DropDatabase(std::shared_ptr<pqxx::connection> conn, std::string dbname) {
	pqxx::nontransaction N(*conn);
	// Using schemas as databases
	N.exec0("drop schema if exists " + N.esc(dbname) + " cascade");
	pqxx::result r = N.exec("select 1 as result from pg_namespace where nspname=" + N.quote(dbname));
	return r.empty();
}

bool PostgreSQLDBHandler::DropTable(std::shared_ptr<pqxx::connection> conn, std::string dbname, std::string tablename) {
	pqxx::nontransaction N(*conn);
	// Using schemas as databases
    N.exec0("set search_path to " + N.esc(dbname) + ", \"$user\", public");
    try {
        N.exec0("drop table if exists " + N.esc(tablename) + " cascade");
    } catch (...) {
    }
    try {
        N.exec0("drop type if exists " + N.esc(tablename) + " cascade");
    } catch (...) {
    }
    pqxx::row r = N.exec1("select exists ( select 1 from pg_tables where schemaname=" + N.quote(dbname) + " and tablename=" + N.quote(tablename) + ")");
	return r[0].as<bool>() == false;
}

bool PostgreSQLDBHandler::TruncateTable(std::shared_ptr<pqxx::connection> conn, std::string dbname, std::string tablename) {
	pqxx::nontransaction N(*conn);
	// Using schemas as databases
	N.exec0("set search_path to " + N.esc(dbname) + ", \"$user\", public");
	N.exec0("truncate table " + N.esc(tablename));
	pqxx::row r = N.exec1("select count(*) from " + N.esc(tablename));
	return r[0].as<int>() == 0;
}
