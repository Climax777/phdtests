#include "benchmark/benchmark.h"
#include "dbphd/mongodb/mongodb.hpp"
#include "mongocxx/bulk_write.hpp"
#include "mongocxx/model/insert_one.hpp"
#include "mongocxx/options/bulk_write.hpp"
#include <random>
#include <string>
#include <iostream>

// Insert tests simply tests insert performance based on:
// *	Number of fields
// *	Number of indexes
// *	Transaction/non-transaction
// *	Number of threads (concurrency)

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

static void BM_MONGO_Insert(benchmark::State& state, bool transactions) {
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
		collection.create_index(make_document(kvp("_id", 1))); // This creates the collection too
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
			auto doc = builder << "a0" << to_string(dis(gen));
			for(int fields = 1; fields < state.range(1); ++fields) {
				doc << ("a" + to_string(fields)) << dis(gen);
			}

			documents.push_back(doc << bsoncxx::builder::stream::finalize);
		}
		state.ResumeTiming();
		auto start = std::chrono::high_resolution_clock::now();
		if(transactions) {
			session.start_transaction();
		}
		collection.insert_many(session, documents);
		if(transactions) {
			session.commit_transaction();
		}
		auto end = std::chrono::high_resolution_clock::now();

		auto elapsed_seconds =
			std::chrono::duration_cast<std::chrono::duration<double>>(
					end - start);

		state.SetIterationTime(elapsed_seconds.count());
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

BENCHMARK_CAPTURE(BM_MONGO_Insert, Normal, false)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_MONGO_Insert, Transact, true)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();

static void BM_MONGO_Insert_Bulk(benchmark::State& state, bool transactions) {
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
		collection.create_index(make_document(kvp("_id", 1))); // This creates the collection too
		if(state.range(2) > 0) {
			for(int index = 0; index < state.range(2); ++index) {
				collection.create_index(make_document(kvp(("a" + to_string(index)), 1)));
			}
		}
	}
	auto session = conn->start_session();
	for(auto _ : state) {
		state.PauseTiming();
		mongocxx::options::bulk_write bulkOptions;
		bulkOptions.bypass_document_validation(true);
		bulkOptions.ordered(false);
		auto writer = collection.create_bulk_write(session, bulkOptions);
		for(int n = 0; n < state.range(0); ++n) {
			auto builder = bsoncxx::builder::stream::document{};
			auto doc = builder << "a0" << to_string(dis(gen));
			for(int fields = 1; fields < state.range(1); ++fields) {
				doc << ("a" + to_string(fields)) << dis(gen);
			}

			mongocxx::model::insert_one inserter(doc << bsoncxx::builder::stream::finalize);
			writer.append(inserter);
		}
		state.ResumeTiming();
		auto start = std::chrono::high_resolution_clock::now();
		if(transactions) {
			session.start_transaction();
		}
		writer.execute();

		if(transactions) {
			session.commit_transaction();
		}

		auto end = std::chrono::high_resolution_clock::now();

		auto elapsed_seconds =
			std::chrono::duration_cast<std::chrono::duration<double>>(
					end - start);

		state.SetIterationTime(elapsed_seconds.count());
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

BENCHMARK_CAPTURE(BM_MONGO_Insert_Bulk, Normal, false)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_MONGO_Insert_Bulk, Transact, true)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
