#include "benchmark/benchmark.h"
#include "dbphd/mongodb/mongodb.hpp"
#include "precalculate.hpp"

#include <random>
#include <string>
#include <iostream>
#include <algorithm>
#include <chrono>
//	1	Delete testing based on number of indexes
//	2	Delete testing based on number of indexes vs number of fields queried (single) (should correlate to find/count + delete)

using namespace std;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

static void CustomArgumentsInserts(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= Precalculator::Columns; ++i) { // fields to query
		for (int j = 0; j <= i; ++j) { //  Indexes
			b->Args({i, j});
		}
	}
}

static void CreateCollection(mongocxx::pool::entry& conn) {
	cout << endl << "Creating Mongo Delete Collection...";
	cout.flush();
	auto start = chrono::steady_clock::now();
	auto db = conn->database("bench");
	auto collection = db.collection("delete_bench");

	collection.drop();
	collection.create_index(make_document(kvp("_id", 1)));

	auto writer = collection.create_bulk_write();
	for(int i = 0; i < Precalculator::Rows; i += 100) {
		for(int j = 0; j < 100 && i+j < Precalculator::Rows;++j) {
			auto builder = bsoncxx::builder::stream::document{};
			auto doc = builder << "_id" << j+i;
			int iteration = i+j;
			auto& rowval = Precalculator::PrecalcValues[iteration];
			for(int f = 0; f < Precalculator::Columns; ++f) {
				doc << ("a" + to_string(f)) << rowval[f];
			}

			mongocxx::model::insert_one inserter(doc << bsoncxx::builder::stream::finalize);
			writer.append(inserter);
		}
		writer.execute();
	}
	auto end = chrono::steady_clock::now();
	cout<< " Done in " << chrono::duration <double, milli> (end-start).count() << " ms" << endl << endl;
	cout.flush();
}

// This does not test #docs returned
static void BM_MONGO_Delete(benchmark::State& state, bool transactions) {
	auto conn = MongoDBHandler::GetConnection();
	auto db = conn->database("bench");
	auto collection = db.collection("delete_bench");
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateCollection(conn);
		collection = db.collection("delete_bench");
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
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		auto builder = bsoncxx::builder::stream::document{};
		for(int n = 0; n < state.range(0); ++n) {
			builder << ("a" + to_string(n)) << dis(gen);
		}
		state.ResumeTiming();
		auto start = std::chrono::high_resolution_clock::now();
		if(transactions) {
			session.start_transaction();
		}
		count += collection.delete_many(session, builder << bsoncxx::builder::stream::finalize)->deleted_count();
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
		//collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}

	state.SetItemsProcessed(count);

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

BENCHMARK_CAPTURE(BM_MONGO_Delete, Normal, false)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_MONGO_Delete, Transact, true)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();

/*
using namespace std;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

static void CustomArgumentsInserts(benchmark::internal::Benchmark* b) {
	for (int i = 1; i <= (1 << 12); i*=8) { // Documents
		for (int j = 1; j <= 16; j *= 2) { // IndexedFields
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

static void BM_MONGO_Delete(benchmark::State& state) {
	auto conn = MongoDBHandler::GetConnection();
	auto db = conn->database("bench");
	auto collection = db.collection("delete_bench");
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

BENCHMARK(BM_MONGO_Delete)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_DeleteTransact(benchmark::State& state) {
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

BENCHMARK(BM_MONGO_DeleteTransact)->Apply(CustomArgumentsInserts)->Complexity()->DenseThreadRange(1,4);*/
