#include "dbphd/postgresql/postgresql.hpp"

using namespace std;
PostgreSQLDBHandler::PostgreSQLDBHandler() {

}
PostgreSQLDBHandler::~PostgreSQLDBHandler() {

}

std::shared_ptr<pqxx::connection> PostgreSQLDBHandler::GetConnection(std::string connstr) {
	return shared_ptr<pqxx::connection>(new pqxx::connection(connstr));
}
