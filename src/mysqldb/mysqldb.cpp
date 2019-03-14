#include "dbphd/mysqldb/mysqldb.hpp"

using namespace std;

mysqlx::Session MySQLDBHandler::GetConnection(string connstr) {
	return mysqlx::Session(connstr);
}

