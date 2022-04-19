#include "benchmark/benchmark.h"
#include "dbphd/mongodb/mongodb.hpp"
#include <mongocxx/bulk_write.hpp>
#include <mongocxx/exception/bulk_write_exception.hpp>
#include "mongocxx/model/insert_one.hpp"
#include <map>
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

static void CustomArgumentsDeletes(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= Precalculator::Columns; ++i) { // fields to query
		for (int j = 0; j <= i; ++j) { //  Indexes
			b->Args({i, j});
		}
	}
}

static void CreateCollection(mongocxx::pool::entry& conn, std::string postfix = "") {
	static unordered_map<string, bool> created;
	if(!created.count(postfix)) {
		cout << endl << "Creating Mongo Delete Collection " << postfix << "...";
		cout.flush();
		auto start = chrono::steady_clock::now();
		auto db = conn->database("bench");
		auto collection = db.collection("delete_bench" + postfix);

		collection.drop();
		collection.create_index(make_document(kvp("_id", 1)));

		mongocxx::options::bulk_write writeOptions;
		writeOptions.ordered(false);
		auto writer = collection.create_bulk_write(writeOptions);
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
		}
		try {
			auto res = writer.execute();
			auto end = chrono::steady_clock::now();
			cout<< " Done " << res->inserted_count() << " in " << chrono::duration <double, milli> (end-start).count() << " ms" << endl << endl;
		}catch(mongocxx::bulk_write_exception e) {
			auto end = chrono::steady_clock::now();
			cout<< " Error  " << e.what() << endl << " in " << chrono::duration <double, milli> (end-start).count() << " ms" << endl << endl;
		}
		cout.flush();
		created[postfix] = true;
	}
}

// This does not test #docs returned
static void BM_MONGO_Delete(benchmark::State& state, bool transactions) {
	auto conn = MongoDBHandler::GetConnection();
	auto db = conn->database("bench");
	std::string postfix = std::to_string(state.thread_index());
	auto collection = db.collection("delete_bench" + postfix);
	std::random_device rd;  //Will be used to obtain a seed for the random number engine
	std::mt19937 gen(rd()); //Standard mersenne_twister_engine seeded with rd()
	std::uniform_int_distribution<> dis(0, 4);
	// Per thread settings...
	CreateCollection(conn, postfix);
	collection = db.collection("delete_bench" + postfix);
	collection.indexes().drop_all();
	if(state.range(1) > 0) {
		auto idxbuilder = bsoncxx::builder::stream::document{};
		auto idx = idxbuilder << "a0" << 1;
		for(int index = 1; index < state.range(1); ++index) {
			idx << ("a" + to_string(index)) << 1;
		}
		// One compounded index (basically just many indexes)
		collection.create_index(idx << bsoncxx::builder::stream::finalize);
	}
	auto session = conn->start_session();
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		auto builder = bsoncxx::builder::stream::document{};
		for(int n = 0; n < state.range(0); ++n) {
			builder << ("a" + to_string(n)) << dis(gen);
		}
		auto doc = builder << bsoncxx::builder::stream::finalize;
		mongocxx::options::bulk_write writeOptions;
		writeOptions.ordered(false);
		auto bulkReinserter = collection.create_bulk_write(writeOptions);
		for(auto deldoc : collection.find(session, doc.view())) {
			mongocxx::model::insert_one inserter(deldoc);
			bulkReinserter.append(inserter);
		}
		
		state.ResumeTiming();
		auto start = std::chrono::high_resolution_clock::now();
		if(transactions) {
			session.start_transaction();
		}
		auto res = collection.delete_many(session, doc.view());
		count +=  res->deleted_count();
		if(transactions) {
			session.commit_transaction();
		}
		auto end = std::chrono::high_resolution_clock::now();

		auto elapsed_seconds =
			std::chrono::duration_cast<std::chrono::duration<double>>(
					end - start);

		state.SetIterationTime(elapsed_seconds.count());
		state.PauseTiming();
		bulkReinserter.execute();
		state.ResumeTiming();
	}

	if(state.thread_index() == 0) {
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


BENCHMARK_CAPTURE(BM_MONGO_Delete, Normal, false)->Apply(CustomArgumentsDeletes)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
BENCHMARK_CAPTURE(BM_MONGO_Delete, Transact, true)->Apply(CustomArgumentsDeletes)->Complexity()->DenseThreadRange(1, 8, 2)->UseManualTime();
