#include <iostream>
#include "gtest/gtest.h"

#include "dbphd/mongodb/mongodb.hpp"

TEST(MongoDB, Connection) {
	auto conn = MongoDBHandler::GetConnection();
	ASSERT_TRUE(conn);
}
