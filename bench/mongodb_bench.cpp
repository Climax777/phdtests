#include "benchmark/benchmark.h"
#include "dbphd/mongodb/mongodb.hpp"
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

static void BM_MONGO_Select(benchmark::State& state) {
	auto conn = MongoDBHandler::GetConnection();
	auto db = conn->database("bench");
	auto collection = db.collection("select_bench");
	mongocxx::pipeline stages;
	stages.limit(1);
	stages.project(make_document(kvp("_id", 0), kvp("result", make_document(kvp("$literal", 500)))));
	for(auto _ : state) {
		auto cursor = collection.aggregate(stages);
	}
}

//BENCHMARK(BM_MONGO_Select);

static void BM_MONGO_SelectTransact(benchmark::State& state) {
	auto conn = MongoDBHandler::GetConnection();
	auto db = conn->database("bench");
	auto collection = db.collection("select_bench");
	mongocxx::pipeline stages;
	stages.limit(1);
	stages.project(make_document(kvp("_id", 0), kvp("result", make_document(kvp("$literal", 500)))));
	auto session = conn->start_session();
	for(auto _ : state) {
		session.start_transaction();
		auto cursor = collection.aggregate(stages);
		session.commit_transaction();
	}
}

//BENCHMARK(BM_MONGO_SelectTransact);
