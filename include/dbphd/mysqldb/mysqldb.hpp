#ifndef MYSQLDB_HPP
#define MYSQLDB_HPP

#include <memory>
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

	static mysqlx::Session GetConnection(std::string connstr = "mysqlx://root:ABB03075023084a@127.0.0.1");
};

#endif /* MYSQLDB_HPP */
