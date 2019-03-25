#include <iostream>
#include "gtest/gtest.h"

#include "dbphd/postgresql/postgresql.hpp"
using namespace std;

TEST(PostgreSQL, Connection) {
	auto conn = PostgreSQLDBHandler::GetConnection();	
	ASSERT_TRUE(conn);
	conn->disconnect();
}

TEST(PostgreSQL, CreateDatabase) {
	string db = "dbphd_test_psql";
	auto conn = PostgreSQLDBHandler::GetConnection();	
	ASSERT_TRUE(conn);
	{
		pqxx::nontransaction N(*conn);
		N.exec0("drop schema if exists " + N.esc(db) + " cascade");
	}
	ASSERT_TRUE(PostgreSQLDBHandler::CreateDatabase(conn, db));
	{
		pqxx::nontransaction N(*conn);
		pqxx::row r = N.exec1("select 1 as result from pg_namespace where nspname='" + N.esc(db)+"'");
		ASSERT_EQ(r[0].as<int>(), 1);
		N.exec0("drop schema if exists " + N.esc(db) + " cascade");
	}
	conn->disconnect();
}

TEST(PostgreSQL, DropDatabase) {
	string db = "dbphd_test_psql";
	auto conn = PostgreSQLDBHandler::GetConnection();	
	ASSERT_TRUE(conn);
	{
		pqxx::nontransaction N(*conn);
		N.exec0("drop schema if exists " + N.esc(db) + " cascade");
	}
	ASSERT_TRUE(PostgreSQLDBHandler::CreateDatabase(conn, db));
	ASSERT_TRUE(PostgreSQLDBHandler::DropDatabase(conn, db));
	{
		pqxx::nontransaction N(*conn);
		pqxx::result r = N.exec("select 1 as result from pg_namespace where nspname='" + N.esc(db)+"'");
		ASSERT_TRUE(r.empty());
	}
	conn->disconnect();
}

TEST(PostgreSQL, DropTable) {
	string db = "dbphd_test_psql";
	auto conn = PostgreSQLDBHandler::GetConnection();	
	ASSERT_TRUE(conn);
	{
		pqxx::nontransaction N(*conn);
		N.exec0("drop schema if exists " + N.esc(db) + " cascade");
	}
	ASSERT_TRUE(PostgreSQLDBHandler::CreateDatabase(conn, db));
	{
		pqxx::nontransaction N(*conn);
		N.exec0("set search_path to " + N.esc(db) + ", \"$user\", public");
		N.exec0("create table if not exists DropTable (test int) ");
	}
	ASSERT_TRUE(PostgreSQLDBHandler::DropTable(conn, db, "DropTable"));
	{
		pqxx::nontransaction N(*conn);
		pqxx::row r = N.exec1("select exists ( select 1 from pg_tables where schemaname = " + N.quote(db) + " and tablename = " + N.quote("DropTable") + " )");
		ASSERT_FALSE(r[0].as<bool>());
	}

	ASSERT_TRUE(PostgreSQLDBHandler::DropDatabase(conn, db));
	conn->disconnect();
}

TEST(PostgreSQL, TruncateTable) {
	string db = "dbphd_test_psql";
	auto conn = PostgreSQLDBHandler::GetConnection();	
	ASSERT_TRUE(conn);
	{
		pqxx::nontransaction N(*conn);
		N.exec0("drop database if exists " + N.esc(db));
	}
	ASSERT_TRUE(PostgreSQLDBHandler::CreateDatabase(conn, db));
	{
		pqxx::nontransaction N(*conn);
		N.exec0("set search_path to " + N.esc(db) + ", \"$user\", public");
		N.exec0("create table if not exists TruncateTable (test int) ");
		pqxx::result r = N.exec("insert into TruncateTable values (1),(2),(3),(4)");
		ASSERT_EQ(r.affected_rows(), 4);
	}
	ASSERT_TRUE(PostgreSQLDBHandler::TruncateTable(conn, db, "TruncateTable"));
	{
		pqxx::nontransaction N(*conn);
		N.exec0("set search_path to " + N.esc(db) + ", \"$user\", public");
		N.exec0("create table if not exists TruncateTable (test int) ");
		pqxx::row r = N.exec1("select count(*) from " + N.esc("TruncateTable"));
		ASSERT_EQ(r[0].as<int>(), 0);
	}

	ASSERT_TRUE(PostgreSQLDBHandler::DropDatabase(conn, db));
	conn->disconnect();
}
