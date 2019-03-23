#ifndef MYSQLDB_HPP
#define MYSQLDB_HPP

#include <memory>
#include <optional>
#include <mysqlx/xdevapi.h>

/*using ::std::cout;
using ::std::endl;
using namespace ::mysqlx;
#undef throw
#define throw() noexcept
#include <mysql_connection.h>

#include <cppconn/driver.h>
#include <cppconn/exception.h>
#include <cppconn/resultset.h>
#include <cppconn/statement.h>*/

class MySQLDBHandler
{
private:
	

public:
	MySQLDBHandler();
	virtual ~MySQLDBHandler();
	static std::optional<mysqlx::Client> Client;
	static mysqlx::Session GetConnection(std::string connstr = "mysqlx://root:password@127.0.0.1");
	static mysqlx::Schema CreateDatabase(mysqlx::Session& session, std::string dbname);
	static bool DropDatabase(mysqlx::Session& session, std::string dbname);
	static bool DropTable(mysqlx::Session& session, std::string dbname, std::string tablename);
};

#endif /* MYSQLDB_HPP */
