#include "benchmark/benchmark.h"
#include "dbphd/mongodb/mongodb.hpp"
#include <random>
#include <string>

using namespace std;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

static void CustomArgumentsInserts(benchmark::internal::Benchmark* b) {
	for (int i = 1; i <= (1 << 12); i*=8) { // Documents
		for (int j = 1; j <= 16; j *= 2) { // Fields
			for(int k = 0; k <= j; ) { // Indexes
				b->Args({i, j, k});
				if(k == 0) {
					k = 1;
				} else {
					k *= 2;
				}
			}
		}
	}
}

static void BM_MONGO_Insert(benchmark::State& state) {
	auto conn = MongoDBHandler::GetConnection();
	auto db = conn->database("bench");
	auto collection = db.collection("create_bench");
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, (1 << 16));
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		collection.drop();
		collection.create_index(make_document(kvp("_id", 1)));
		if(state.range(2) > 0) {
			for(int index = 0; index < state.range(2); ++index) {
				collection.create_index(make_document(kvp(("a" + to_string(index)), 1)));
			}
		}
	}
	for(auto _ : state) {
		state.PauseTiming();
		std::vector<bsoncxx::document::value> documents;
		for(int n = 0; n < state.range(0); ++n) {
			auto builder = bsoncxx::builder::stream::document{};
			auto doc = builder << "a0" << dis(gen);
			for(int fields = 1; fields < state.range(1); ++fields) {
				doc << ("a" + to_string(fields)) << to_string(dis(gen));
			}

			documents.push_back(doc << bsoncxx::builder::stream::finalize);
		}
		state.ResumeTiming();
		collection.insert_many(documents);
	}

	if(state.thread_index == 0) {
		collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
	}

	//state.SetItemsProcessed(state.iterations()*state.range(0));
	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations()*state.range(0), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations()*state.range(0), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Documents", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Fields", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MONGO_Insert)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_InsertTransact(benchmark::State& state) {
	auto conn = MongoDBHandler::GetConnection();
	auto db = conn->database("bench");
	auto collection = db.collection("create_bench");
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, (1 << 16));
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		collection.drop();
		collection.create_index(make_document(kvp("_id", 1)));
		if(state.range(2) > 0) {
			for(int index = 0; index < state.range(2); ++index) {
				collection.create_index(make_document(kvp(("a" + to_string(index)), 1)));
			}
		}
	}
	auto session = conn->start_session();
	for(auto _ : state) {
		state.PauseTiming();
		std::vector<bsoncxx::document::value> documents;
		for(int n = 0; n < state.range(0); ++n) {
			auto builder = bsoncxx::builder::stream::document{};
			auto doc = builder << "a0" << dis(gen);
			for(int fields = 1; fields < state.range(1); ++fields) {
				doc << ("a" + to_string(fields)) << to_string(dis(gen));
			}

			documents.push_back(doc << bsoncxx::builder::stream::finalize);
		}
		state.ResumeTiming();
		session.start_transaction();
		collection.insert_many(documents);
		session.commit_transaction();
	}

	if(state.thread_index == 0) {
		collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
	}

//	state.SetItemsProcessed(state.iterations()*state.range(0));
	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations()*state.range(0), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations()*state.range(0), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Documents", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Fields", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MONGO_InsertTransact)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1,4);
/*static void BM_MONGO_SelectTransact(benchmark::State& state) {
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

BENCHMARK(BM_MONGO_SelectTransact);*/
