#include "benchmark/benchmark.h"
#include "dbphd/mongodb/mongodb.hpp"
#include <random>
#include <string>
#include <iostream>
#include <algorithm>

// Read tests read performance based on:
// *	Number of fields in query Done
// *	Number of indexes used (compounded scan vs indexed basically) Done
// *	Number of fields returned per document
// *	Range queries 
// *	Join?
// *	Skip, sort, limit
// *	Simple aggregations: avg, sum, min, max, *2
// *	Transaction/non-transaction
// *	Number of threads (concurrency)

using namespace std;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

static void CustomArgumentsInserts(benchmark::internal::Benchmark* b) {
	for (int i = 1; i <= 5; ++i) { // fields to query
		for (int j = 0; j <= i; ++j) { //  Indexes
			b->Args({i, j});
		}
	}
}

static void CustomArgumentsInserts2(benchmark::internal::Benchmark* b) {
	for (int i = 1; i <= 5; ++i) { // documents to return
		for (int j = 1; j <= 5; ++j) { // fields to query
			for (int k = 0; k <= 5; ++k) { //  fields to return
				b->Args({i, j, k});
			}
		}
	}
}

static void CreateCollection(mongocxx::pool::entry& conn) {
	static volatile bool created = false;
	if(!created) {
		auto db = conn->database("bench");
		auto collection = db.collection("read_bench");

		collection.drop();
		collection = db.create_collection("read_bench");
		collection.create_index(make_document(kvp("_id", 1)));

		int values[] = {0,1,2,3,4,5,6,7,8,9};
		for(int i = 0; i < pow(10,5); i += 100) {
			std::vector<bsoncxx::document::value> documents;
			for(int j = 0; j < 100 && i+j < pow(10,5);++j) {
				auto builder = bsoncxx::builder::stream::document{};
				auto doc = builder << "_id" << j+i;
				int iteration = i+j;
				for(int f = 0; f < 5; ++f) {
					doc << ("a" + to_string(f)) << values[iteration%10];
					iteration /= 10;
				}

				documents.push_back(doc << bsoncxx::builder::stream::finalize);
			}
			collection.insert_many(documents);
		}
	}
	created = true;
}

// This does not test #docs returned
static void BM_MONGO_Read_Count(benchmark::State& state) {
	auto conn = MongoDBHandler::GetConnection();
	auto db = conn->database("bench");
	auto collection = db.collection("read_bench");
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateCollection(conn);
		collection = db.collection("read_bench");
		collection.indexes().drop_all();
		mongocxx::options::index index_options{};
		index_options.background(false);
		if(state.range(1) > 0) {
			auto idxbuilder = bsoncxx::builder::stream::document{};
			auto idx = idxbuilder << "a0" << 1;
			for(int index = 1; index < state.range(1); ++index) {
				idx << ("a" + to_string(index)) << 1;
			}
			// One compounded index (basically just many indexes)
			collection.create_index(idx << bsoncxx::builder::stream::finalize, index_options);
		}
	}
	auto session = conn->start_session();
	for(auto _ : state) {
		state.PauseTiming();
		auto builder = bsoncxx::builder::stream::document{};
		auto doc = builder << "a0" << dis(gen);
		for(int n = 1; n < state.range(0); ++n) {
			doc << ("a" + to_string(n)) << dis(gen);
		}
		state.ResumeTiming();
		collection.count_documents(session, doc << bsoncxx::builder::stream::finalize);
	}

	if(state.thread_index == 0) {
		//collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}

	state.SetItemsProcessed(pow(10,5)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Fields", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MONGO_Read_Count)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_Read_Count_Transact(benchmark::State& state) {
	auto conn = MongoDBHandler::GetConnection();
	auto db = conn->database("bench");
	auto collection = db.collection("read_bench");
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateCollection(conn);
		collection = db.collection("read_bench");
		collection.indexes().drop_all();
		mongocxx::options::index index_options{};
		index_options.background(false);
		if(state.range(1) > 0) {
			auto idxbuilder = bsoncxx::builder::stream::document{};
			auto idx = idxbuilder << "a0" << 1;
			for(int index = 1; index < state.range(1); ++index) {
				idx << ("a" + to_string(index)) << 1;
			}
			// One compounded index (basically just many indexes)
			collection.create_index(idx << bsoncxx::builder::stream::finalize, index_options);
		}
	}
	auto session = conn->start_session();
	for(auto _ : state) {
		state.PauseTiming();
		auto builder = bsoncxx::builder::stream::document{};
		auto doc = builder << "a0" << dis(gen);
		for(int n = 1; n < state.range(0); ++n) {
			doc << ("a" + to_string(n)) << dis(gen);
		}

		state.ResumeTiming();
		session.start_transaction();
		collection.count_documents(session, doc << bsoncxx::builder::stream::finalize);
		session.commit_transaction();
	}

	if(state.thread_index == 0) {
//		collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}

	state.SetItemsProcessed(pow(10,5)*state.iterations()); // TODO check assumption

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Fields", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MONGO_Read_Count_Transact)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1,4);
