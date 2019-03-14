#include "dbphd/mongodb/mongodb.hpp"

std::shared_ptr<mongocxx::pool> MongoDBHandler::m_Pool;
mongocxx::pool::entry MongoDBHandler::GetConnection(std::string connstr) {
	if(!m_Pool) {
		mongocxx::instance instance{};
		m_Pool.reset(new mongocxx::pool(mongocxx::uri(connstr)));
	}

	return m_Pool->acquire();
}
