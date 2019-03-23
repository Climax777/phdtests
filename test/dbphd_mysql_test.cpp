#include <iostream>
#include "gtest/gtest.h"

#include "dbphd/mysqldb/mysqldb.hpp"
using namespace std;

TEST(MySQLX, Connection) {
	auto conn = MySQLDBHandler::GetConnection();	
	conn.close();
}

TEST(MySQLX, CreateDatabase) {
	string db = "dbphd_test_mysql";
	auto conn = MySQLDBHandler::GetConnection();	
	auto schematest = conn.getSchema(db, false);
	if(schematest.existsInDatabase())
		conn.dropSchema(db);
	auto schema = MySQLDBHandler::CreateDatabase(conn, db);
	ASSERT_TRUE(schema.existsInDatabase());
	conn.dropSchema(db);
	auto schematest2 = conn.getSchema(db, false);
	ASSERT_FALSE(schematest2.existsInDatabase());
	conn.close();
}

TEST(MySQLX, DropDatabase) {
	string db = "dbphd_test_mysql";
	auto conn = MySQLDBHandler::GetConnection();	
	auto schema = conn.createSchema(db, true);
	ASSERT_TRUE(MySQLDBHandler::DropDatabase(conn, db));
	auto schematest = conn.getSchema(db, false);
	ASSERT_FALSE(schematest.existsInDatabase());
	conn.close();
}

TEST(MySQLX, DropTable) {
	string db = "dbphd_test_mysql";
	auto conn = MySQLDBHandler::GetConnection();	
	ASSERT_TRUE(MySQLDBHandler::DropDatabase(conn, db));
	auto schema = MySQLDBHandler::CreateDatabase(conn, db);
	ASSERT_TRUE(schema.existsInDatabase());
	conn.sql("USE " + db).execute();
	auto result = conn.sql("create table if not exists testdrop (test int)").execute();
	ASSERT_TRUE(result.getWarningsCount() == 0);
	ASSERT_TRUE(MySQLDBHandler::DropTable(conn, db, "testdrop"));
	ASSERT_TRUE(MySQLDBHandler::DropDatabase(conn, db));
	conn.close();
}
