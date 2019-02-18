#ifndef POSTGRESQL_HPP
#define POSTGRESQL_HPP

#include <memory>
#include <pqxx/pqxx>
class PostgreSQLDBHandler
{
private:
	

public:
	PostgreSQLDBHandler();

	static std::shared_ptr<pqxx::connection> GetConnection(std::string connstr = "host=localhost user=phdtests password=881007");
	virtual ~PostgreSQLDBHandler();
};

#endif /* POSTGRESQL_HPP */
