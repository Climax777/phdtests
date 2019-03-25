#ifndef POSTGRESQL_HPP
#define POSTGRESQL_HPP

#include <memory>
#include <pqxx/pqxx>
class PostgreSQLDBHandler
{
private:
	

public:
	PostgreSQLDBHandler();

	static std::shared_ptr<pqxx::connection> GetConnection(std::string connstr = "host=localhost dbname=phdtests user=phdtests password=881007");
	static bool CreateDatabase(std::shared_ptr<pqxx::connection> conn, std::string dbname);
	static bool DropDatabase(std::shared_ptr<pqxx::connection> conn, std::string dbname);
	static bool DropTable(std::shared_ptr<pqxx::connection> conn, std::string dbname, std::string tablename);
	static bool TruncateTable(std::shared_ptr<pqxx::connection> conn, std::string dbname, std::string tablename);
	virtual ~PostgreSQLDBHandler();
};

#endif /* POSTGRESQL_HPP */
