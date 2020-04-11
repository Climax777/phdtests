#include "benchmark/benchmark.h"
#include "bsoncxx/builder/stream/helpers.hpp"
#include "dbphd/mongodb/mongodb.hpp"
#include "precalculate.hpp"

#include <random>
#include <string>
#include <iostream>
#include <algorithm>
#include <chrono>
//TODO
//  1	Update testing based on number of fields to query // basically boils down to #documents
//  2	Update testing based on number of fields to change(indexed fields and unindexed fields - query speed vs write speed)
//  3	Update testing based on number of indexes used
//  4	Update testing based on subquery // TODO what was this again?? Probably a where clause of sorts?
//  5	Update testing based on adding fields // Do we have to do this? Probably not operational in essence
//  6	Update testing based on removing fields // same as above
//
using namespace std;
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

static void CustomArgumentsUpdates(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= Precalculator::Columns; ++i) { // fields to query
		for (int j = 1; j <= Precalculator::Columns; ++j) { // fields to write
			for (int k = 0; k <= i; ++k) { //  Indexes
				b->Args({i, j, k});
			}
		}
	}
}

static void CustomArgumentsUpdates2(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= Precalculator::Columns; ++i) { // fields to query
		for (int j = 1; j <= Precalculator::Columns; ++j) { // fields to write
			for (int k = 0; k <= j; ++k) { //  Indexes
				b->Args({i, j, k});
			}
		}
	}
}


static void CreateCollection(mongocxx::pool::entry& conn, bool doublefields = false) {
	static volatile bool created = false;
	if(!created) {
		cout << endl << "Creating Mongo Update Collection...";
		cout.flush();
		auto start = chrono::steady_clock::now();
		auto db = conn->database("bench");
		auto collection = db.collection("update_bench");

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
					doc << ("a" + to_string(f)) << rowval[f]; // values for query
					if(doublefields)
						doc << ("b" + to_string(f)) << rowval[f]; // values for writing
				}

				mongocxx::model::insert_one inserter(doc << bsoncxx::builder::stream::finalize);
				writer.append(inserter);
			}
		}
		writer.execute();
		auto end = chrono::steady_clock::now();
		cout<< " Done in " << chrono::duration <double, milli> (end-start).count() << " ms" << endl << endl;
		cout.flush();
	}
	created = true;
}

static void BM_MONGO_Update(benchmark::State& state, bool transactions, bool testwriteindexes) {
	auto conn = MongoDBHandler::GetConnection();
	auto db = conn->database("bench");
	auto collection = db.collection("update_bench");
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	std::uniform_int_distribution<> dis2(1,100);
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
		CreateCollection(conn, true); // add double fields, as to measure write perforance without indexes playing a role
		collection = db.collection("update_bench");
		collection.indexes().drop_all();
		if(testwriteindexes) {
			if(state.range(0) > 0) {
				auto idxbuilder = bsoncxx::builder::stream::document{};
				auto idx = idxbuilder << ("a0") << 1;
				for(int index = 1; index < state.range(0); ++index) {
					idx << ("a" + to_string(index)) << 1;
				}
				// One compounded index (basically just many indexes)
				collection.create_index(idx << bsoncxx::builder::stream::finalize);
			}
			if(state.range(2) > 0) { // Create individual indexes for write indexes
				for(int index = 0; index < state.range(2); ++index) {
					auto idxbuilder = bsoncxx::builder::stream::document{};
					auto idx = idxbuilder << ("b"+to_string(index)) << 1;
					collection.create_index(idx << bsoncxx::builder::stream::finalize);
				}
				// One compounded index (basically just many indexes)
			}
		} else {
			if(state.range(2) > 0) {
				auto idxbuilder = bsoncxx::builder::stream::document{};
				auto idx = idxbuilder << "a0" << 1;
				for(int index = 1; index < state.range(2); ++index) {
					idx << ("a" + to_string(index)) << 1;
				}
				// One compounded index (basically just many indexes)
				collection.create_index(idx << bsoncxx::builder::stream::finalize);
			}
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
		auto builderupdate = bsoncxx::builder::stream::document{};
		builderupdate << "$inc" << bsoncxx::builder::stream::open_document;
		for(int n = 0; n < state.range(1); ++n) {
			builderupdate << ("b" + to_string(n)) << dis2(gen);
		}
		builderupdate << bsoncxx::builder::stream::close_document;
		auto query = builder << bsoncxx::builder::stream::finalize;
		auto update = builderupdate << bsoncxx::builder::stream::finalize;
		state.ResumeTiming();
		auto start = std::chrono::high_resolution_clock::now();
		if(transactions) {
			session.start_transaction();
		}
		auto res = collection.update_many(session, query.view(), update.view());
		count += res->matched_count();
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
	state.counters.insert({{"FieldsQueried", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"FieldsChanged", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK_CAPTURE(BM_MONGO_Update, Normal, false, false)->Apply(CustomArgumentsUpdates)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_MONGO_Update, Transact, true, false)->Apply(CustomArgumentsUpdates)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_MONGO_Update, NormalWriteIdx, false, true)->Apply(CustomArgumentsUpdates2)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_MONGO_Update, TransactWriteIdx, true, true)->Apply(CustomArgumentsUpdates2)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();

/*static void BM_MONGO_Select(benchmark::State& state) {
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

BENCHMARK(BM_MONGO_Select);

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

BENCHMARK(BM_MONGO_SelectTransact);*/
