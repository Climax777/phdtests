#ifndef MONGODB_HPP
#define MONGODB_HPP

#include <memory>

#include <bsoncxx/builder/stream/array.hpp>
#include <bsoncxx/builder/stream/document.hpp>
#include <bsoncxx/builder/stream/helpers.hpp>
#include <bsoncxx/builder/core.hpp>
#include <bsoncxx/json.hpp>
#include <bsoncxx/types.hpp>
#include <mongocxx/client.hpp>
#include <mongocxx/instance.hpp>
#include <mongocxx/pool.hpp>
#include <mongocxx/uri.hpp>
#include <bsoncxx/builder/basic/document.hpp>
#include <bsoncxx/builder/basic/kvp.hpp>
#include <bsoncxx/builder/list.hpp>
#include <bsoncxx/document/value.hpp>
#include <bsoncxx/document/view.hpp>
#include <bsoncxx/document/view_or_value.hpp>

#define MDVV(...)	bsoncxx::document::view { bsoncxx::builder::document  { __VA_ARGS__ }.view().get_document() }
#define MDV(...)	bsoncxx::document::value { MDVV( __VA_ARGS__ ) }
#define MDV2(...)	bsoncxx::document::value { __VA_ARGS__ }

class MongoDBHandler
{
private:
	static std::shared_ptr<mongocxx::pool> m_Pool;

public:
	MongoDBHandler();
	virtual ~MongoDBHandler();
	
	static mongocxx::pool::entry GetConnection(std::string connstr = "mongodb://localhost:27017/?maxPoolSize=100&minPoolSize=8&compressors=zstd,snappy,zlib");
};

#endif /* MONGODB_HPP */
