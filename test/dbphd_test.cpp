#include <iostream>
#include "gtest/gtest.h"

#include "dbphd/dbphd.hpp"
#include "dbphd/postgresql/postgresql.hpp"
#include "dbphd/mysqldb/mysqldb.hpp"
#include "dbphd/mongodb/mongodb.hpp"

TEST(DBPHDTests, Basic) {
	dbphd::DBPHD testObj;
}

TEST(PostgreSQL, Connection) {
	auto conn = PostgreSQLDBHandler::GetConnection();	
	ASSERT_TRUE(conn);
	conn->disconnect();
}

TEST(MySQLX, Connection) {
	auto conn = MySQLDBHandler::GetConnection();	
	conn.close();
}

TEST(MongoDB, Connection) {
	auto conn = MongoDBHandler::GetConnection();
	ASSERT_TRUE(conn);
}
