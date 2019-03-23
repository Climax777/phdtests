#include "dbphd/mysqldb/mysqldb.hpp"

using namespace std;

std::optional<mysqlx::Client> MySQLDBHandler::Client;

mysqlx::Session MySQLDBHandler::GetConnection(string connstr) {
	if(!Client) {
		Client.emplace(mysqlx::getClient(connstr));
	}
	return Client->getSession();
}

bool MySQLDBHandler::DropDatabase(mysqlx::Session& session, std::string dbname) {
	session.dropSchema(dbname);
	auto schema = session.getSchema(dbname, false);
	return !schema.existsInDatabase();
}

mysqlx::Schema MySQLDBHandler::CreateDatabase(mysqlx::Session& session, std::string dbname) {
	auto schema = session.createSchema(dbname, true);
	return schema;
}

bool MySQLDBHandler::DropTable(mysqlx::Session& session, std::string dbname, std::string tablename) {	
	session.sql("use " + dbname).execute();
	auto sqlstatement = session.sql("drop table if exists " + tablename);
	mysqlx::SqlResult result = sqlstatement.execute();
	return result.getWarningsCount() == 0;
}
