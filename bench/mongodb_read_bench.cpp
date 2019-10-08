#include "benchmark/benchmark.h"
#include "dbphd/mongodb/mongodb.hpp"
#include "precalculate.hpp"

#include <random>
#include <string>
#include <iostream>
#include <algorithm>
#include <chrono>

// Read tests read performance based on:
// *	Number of fields in query Done
// *	Number of indexes used (compounded scan vs indexed basically) Done
// *	Number of fields returned per document Done
// *	limit Done
// *	Simple aggregations: avg, sum, *2 Done
// *	sort // Done
// *	Join Done
// *	Manual Join Done
// *	Skip Not really more impactful than just returning the document
// *	Range queries // This is implicitly the same as fixing one and leaving the rest of the columns >= 0 ???

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

static void CustomArgumentsInserts2(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= Precalculator::Columns; ++i) { // fields to query
		for (int j = 0; j <= Precalculator::Columns; ++j) { // fields to return
			for (int k = 0; k <= i; ++k) { //  Indexes
				for(int l = 1; l <= (int)pow(Precalculator::Values,Precalculator::Columns-i); l *= 2) { // Documents to return
					b->Args({i, j, k, l});
				}
			}
		}
	}
}

static void CustomArgumentsInserts3(benchmark::internal::Benchmark* b) {
	for (int i = 1; i <= Precalculator::Rows; i *= 2) { // documents to process
		b->Args({i});
	}
}

static void CustomArgumentsInserts4(benchmark::internal::Benchmark* b) {
	for (int i = 1; i <= Precalculator::Columns; ++i) { // fields to sort
		for (int k = 0; k <= i; ++k) { //  Indexes
			for(int l = 1; l <= (int)pow(Precalculator::Values,Precalculator::Columns); l *= 2) { // Documents to return
				b->Args({i, k, l});
			}
		}
	}
}

static void CustomArgumentsInserts5(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= 2; ++i) { // fields to index
		for(int l = 1; l <= (int)pow(Precalculator::Values,Precalculator::Columns); l *= 2) { // Documents to return
			b->Args({i, l});
		}
	}
}

static void CustomArgumentsInserts6(benchmark::internal::Benchmark* b) {
	for (int i = 0; i <= 2; ++i) { // fields to index
		for(int j = 1; j <= (int)pow(Precalculator::Values,Precalculator::Columns); j *= 2) { // Documents to return
			for(int k = 1; k <= j; k *= 2) { // Documents to query for batch
				b->Args({i, j, k});
			}
		}
	}
}

static void CreateCollection(mongocxx::pool::entry& conn) {
	static volatile bool created = false;
	if(!created) {
		cout << endl << "Creating Mongo Read Collection...";
		cout.flush();
		auto start = chrono::steady_clock::now();
		auto db = conn->database("bench");
		auto collection = db.collection("read_bench");

		collection.drop();
		collection = db.create_collection("read_bench");
		collection.create_index(make_document(kvp("_id", 1)));

		for(int i = 0; i < Precalculator::Rows; i += 100) {
			std::vector<bsoncxx::document::value> documents;
			for(int j = 0; j < 100 && i+j < Precalculator::Rows;++j) {
				auto builder = bsoncxx::builder::stream::document{};
				auto doc = builder << "_id" << j+i;
				int iteration = i+j;
				auto& rowval = Precalculator::PrecalcValues[iteration];
				for(int f = 0; f < Precalculator::Columns; ++f) {
					doc << ("a" + to_string(f)) << rowval[f];
				}

				documents.push_back(doc << bsoncxx::builder::stream::finalize);
			}
			collection.insert_many(documents);
		}
		auto end = chrono::steady_clock::now();
		cout<< " Done in " << chrono::duration <double, milli> (end-start).count() << " ms" << endl << endl;
		cout.flush();
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
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		auto builder = bsoncxx::builder::stream::document{};
		for(int n = 0; n < state.range(0); ++n) {
			builder << ("a" + to_string(n)) << dis(gen);
		}
		state.ResumeTiming();
		count += collection.count_documents(session, builder << bsoncxx::builder::stream::finalize);
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
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		auto builder = bsoncxx::builder::stream::document{};
		for(int n = 0; n < state.range(0); ++n) {
			builder << ("a" + to_string(n)) << dis(gen);
		}

		state.ResumeTiming();
		session.start_transaction();
		count += collection.count_documents(session, builder << bsoncxx::builder::stream::finalize);
		session.commit_transaction();
	}

	if(state.thread_index == 0) {
//		collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}

	state.SetItemsProcessed(count); // TODO check assumption

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

static void BM_MONGO_Reads(benchmark::State& state) {
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
		if(state.range(2) > 0) {
			auto idxbuilder = bsoncxx::builder::stream::document{};
			auto idx = idxbuilder << "a0" << 1;
			for(int index = 1; index < state.range(2); ++index) {
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
		mongocxx::options::find options;
		options.batch_size(INT32_MAX).limit(state.range(3));
		if(state.range(1) > 0) {
			auto builder = bsoncxx::builder::stream::document{};
			for(int i = 0; i < state.range(1); ++i) {
				builder << "a" + to_string(i) << 1;
			}
			options.projection(builder << bsoncxx::builder::stream::finalize);
		} else {
			options.projection(bsoncxx::builder::stream::document{} << "_id " << 1 << bsoncxx::builder::stream::finalize);
		}
		state.ResumeTiming();
		for(auto i : collection.find(session, builder << bsoncxx::builder::stream::finalize, options)) {
			++count;
		}
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
	state.counters.insert({{"Fields", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"FieldsProj", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(3), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MONGO_Reads)->Apply(CustomArgumentsInserts2)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_Reads_Transact(benchmark::State& state) {
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
		if(state.range(2) > 0) {
			auto idxbuilder = bsoncxx::builder::stream::document{};
			auto idx = idxbuilder << "a0" << 1;
			for(int index = 1; index < state.range(2); ++index) {
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
		mongocxx::options::find options;
		options.batch_size(INT32_MAX).limit(state.range(3));
		if(state.range(1) > 0) {
			auto builder = bsoncxx::builder::stream::document{};
			for(int i = 0; i < state.range(1); ++i) {
				builder << "a" + to_string(i) << 1;
			}
			options.projection(builder << bsoncxx::builder::stream::finalize);
		} else {
			options.projection(bsoncxx::builder::stream::document{} << "_id " << 1 << bsoncxx::builder::stream::finalize);
		}
		state.ResumeTiming();
		session.start_transaction();
		auto cursor = collection.find(session, builder << bsoncxx::builder::stream::finalize, options);
		for(auto i : cursor) {
			++count;
		}
		session.commit_transaction();
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
	state.counters.insert({{"Fields", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"FieldsProj", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(3), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MONGO_Reads_Transact)->Apply(CustomArgumentsInserts2)->Complexity()->DenseThreadRange(1,4);


static void BM_MONGO_Read_Sum(benchmark::State& state) {
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
	}
	auto session = conn->start_session();
	for(auto _ : state) {
		state.PauseTiming();
		mongocxx::pipeline pipe;
		pipe.limit(state.range(0));
		pipe.group(bsoncxx::builder::stream::document{} << "_id"  << 1 << "count" << bsoncxx::builder::stream::open_document << "$sum" << "$a0" <<  bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::finalize);
		state.ResumeTiming();
		auto result = collection.aggregate(session, pipe);

		for(auto i : result) {
		}
	}

	if(state.thread_index == 0) {
		//collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}
	state.SetComplexityN(state.range(0));

	state.SetItemsProcessed(state.range(0)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_MONGO_Read_Sum)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_Read_Sum_Transact(benchmark::State& state) {
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
	}
	auto session = conn->start_session();
	for(auto _ : state) {
		state.PauseTiming();
		mongocxx::pipeline pipe;
		pipe.limit(state.range(0));
		pipe.group(bsoncxx::builder::stream::document{} << "_id"  << 1 << "count" << bsoncxx::builder::stream::open_document << "$sum" << "$a0" <<  bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::finalize);
		state.ResumeTiming();
		session.start_transaction();
		auto result = collection.aggregate(session, pipe);
		for(auto i : result) {
		}
		session.commit_transaction();
	}

	if(state.thread_index == 0) {
		//collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}
	state.SetComplexityN(state.range(0));

	state.SetItemsProcessed(state.range(0)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_MONGO_Read_Sum_Transact)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_Read_Avg(benchmark::State& state) {
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
	}
	auto session = conn->start_session();
	for(auto _ : state) {
		state.PauseTiming();
		mongocxx::pipeline pipe;
		pipe.limit(state.range(0));
		pipe.group(bsoncxx::builder::stream::document{} << "_id"  << 1 << "count" << bsoncxx::builder::stream::open_document << "$avg" << "$a0" <<  bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::finalize);
		state.ResumeTiming();
		auto result = collection.aggregate(session, pipe);

		for(auto i : result) {
		}
	}

	if(state.thread_index == 0) {
		//collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}
	state.SetComplexityN(state.range(0));

	state.SetItemsProcessed(state.range(0)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_MONGO_Read_Avg)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_Read_Avg_Transact(benchmark::State& state) {
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
	}
	auto session = conn->start_session();
	for(auto _ : state) {
		state.PauseTiming();
		mongocxx::pipeline pipe;
		pipe.limit(state.range(0));
		pipe.group(bsoncxx::builder::stream::document{} << "_id"  << 1 << "count" << bsoncxx::builder::stream::open_document << "$avg" << "$a0" <<  bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::finalize);
		state.ResumeTiming();
		session.start_transaction();
		auto result = collection.aggregate(session, pipe);
		for(auto i : result) {
		}
		session.commit_transaction();
	}

	if(state.thread_index == 0) {
		//collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}
	state.SetComplexityN(state.range(0));

	state.SetItemsProcessed(state.range(0)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_MONGO_Read_Avg_Transact)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_Read_Mul(benchmark::State& state) {
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
	}
	auto session = conn->start_session();
	for(auto _ : state) {
		state.PauseTiming();
		mongocxx::pipeline pipe;
		pipe.limit(state.range(0));
		pipe.project(bsoncxx::builder::stream::document{} << "_id"  << 1 << "a0" << bsoncxx::builder::stream::open_document << "$multiply" << bsoncxx::builder::stream::open_array << "$a0" << 2 << bsoncxx::builder::stream::close_array <<  bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::finalize);
		state.ResumeTiming();
		auto result = collection.aggregate(session, pipe);

		for(auto i : result) {
		}
	}

	if(state.thread_index == 0) {
		//collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}
	state.SetComplexityN(state.range(0));

	state.SetItemsProcessed(state.range(0)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_MONGO_Read_Mul)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_Read_Mul_Transact(benchmark::State& state) {
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
	}
	auto session = conn->start_session();
	for(auto _ : state) {
		state.PauseTiming();
		mongocxx::pipeline pipe;
		pipe.limit(state.range(0));
		pipe.project(bsoncxx::builder::stream::document{} << "_id"  << 1 << "a0" << bsoncxx::builder::stream::open_document << "$multiply" << bsoncxx::builder::stream::open_array << "$a0" << 2 << bsoncxx::builder::stream::close_array <<  bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::finalize);
		state.ResumeTiming();
		session.start_transaction();
		auto result = collection.aggregate(session, pipe);
		for(auto i : result) {
		}
		session.commit_transaction();
	}

	if(state.thread_index == 0) {
		//collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}
	state.SetComplexityN(state.range(0));

	state.SetItemsProcessed(state.range(0)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
}

BENCHMARK(BM_MONGO_Read_Mul_Transact)->Apply(CustomArgumentsInserts3)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_Read_Sort(benchmark::State& state) {
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
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		auto builder = bsoncxx::builder::stream::document{};
		for(int n = 0; n < state.range(0); ++n) {
			builder << ("a" + to_string(n)) << -1;
		}
		mongocxx::options::find options;
		options.sort(builder << bsoncxx::builder::stream::finalize);
		options.batch_size(INT32_MAX).limit(state.range(2));
		state.ResumeTiming();
		for(auto i : collection.find(session, {}, options)) {
			++count;
		}
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
	state.counters.insert({{"Fields", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MONGO_Read_Sort)->Apply(CustomArgumentsInserts4)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_Read_Sort_Transact(benchmark::State& state) {
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
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		auto builder = bsoncxx::builder::stream::document{};
		for(int n = 0; n < state.range(0); ++n) {
			builder << ("a" + to_string(n)) << -1;
		}
		mongocxx::options::find options;
		options.sort(builder << bsoncxx::builder::stream::finalize);
		options.batch_size(INT32_MAX).limit(state.range(2));
		state.ResumeTiming();
		session.start_transaction();
		for(auto i : collection.find(session, {}, options)) {
			++count;
		}
		session.commit_transaction();
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
	state.counters.insert({{"Fields", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MONGO_Read_Sort_Transact)->Apply(CustomArgumentsInserts4)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_Read_Join(benchmark::State& state) {
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
		for(int index = 0; index < state.range(0); ++index) {
			auto idxbuilder = bsoncxx::builder::stream::document{};
			idxbuilder << ("a" + to_string(index)) << 1;
			// One index (basically just many indexes)
			collection.create_index(idxbuilder << bsoncxx::builder::stream::finalize, index_options);
		}
	}
	auto session = conn->start_session();
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		mongocxx::pipeline pipe;
		pipe.lookup(bsoncxx::builder::stream::document{} << 
				"from" << "read_bench" << 
				"let" << bsoncxx::builder::stream::open_document << "a0" << "$a0" << "id" << "$_id" << bsoncxx::builder::stream::close_document << 
				"as" << "match" << 
				"pipeline" << bsoncxx::builder::stream::open_array << 
								bsoncxx::builder::stream::open_document << 
								"$match" << bsoncxx::builder::stream::open_document <<  
												"$expr" << bsoncxx::builder::stream::open_document <<  
															"$and" << bsoncxx::builder::stream::open_array <<
																		bsoncxx::builder::stream::open_document << "$eq" << bsoncxx::builder::stream::open_array << "$a1" << "$$a0" << bsoncxx::builder::stream::close_array << bsoncxx::builder::stream::close_document <<
																		bsoncxx::builder::stream::open_document << "$ne" << bsoncxx::builder::stream::open_array << "$_id" << "$$id" << bsoncxx::builder::stream::close_array << bsoncxx::builder::stream::close_document <<
																	  bsoncxx::builder::stream::close_array <<
														   bsoncxx::builder::stream::close_document << 
											bsoncxx::builder::stream::close_document << 
								bsoncxx::builder::stream::close_document << 
								bsoncxx::builder::stream::open_document << 
								"$limit" << state.range(1) <<
								bsoncxx::builder::stream::close_document << 
								bsoncxx::builder::stream::close_array << bsoncxx::builder::stream::finalize);
		pipe.match(bsoncxx::builder::stream::document{} << "match" << bsoncxx::builder::stream::open_document << "$ne" << bsoncxx::builder::basic::array{}  << bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::finalize);
		pipe.unwind("$match");
		pipe.limit(state.range(1));

		mongocxx::options::aggregate options;
		options.batch_size(INT32_MAX);
		options.allow_disk_use(true);
		options.bypass_document_validation(true);
		state.ResumeTiming();
		auto result = collection.aggregate(session, pipe, options);

		for(auto i : result) {
			++count;
		}
	}

	if(state.thread_index == 0) {
		//collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}
	state.SetComplexityN(state.range(1));

	state.SetItemsProcessed(state.range(1)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Indexes", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MONGO_Read_Join)->Apply(CustomArgumentsInserts5)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_Read_Join_Transact(benchmark::State& state) {
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
		for(int index = 0; index < state.range(0); ++index) {
			auto idxbuilder = bsoncxx::builder::stream::document{};
			idxbuilder << ("a" + to_string(index)) << 1;
			// One index (basically just many indexes)
			collection.create_index(idxbuilder << bsoncxx::builder::stream::finalize, index_options);
		}
	}
	auto session = conn->start_session();
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		mongocxx::pipeline pipe;
		pipe.lookup(bsoncxx::builder::stream::document{} << 
				"from" << "read_bench" << 
				"let" << bsoncxx::builder::stream::open_document << "a0" << "$a0" << "id" << "$_id" << bsoncxx::builder::stream::close_document << 
				"as" << "match" << 
				"pipeline" << bsoncxx::builder::stream::open_array << 
								bsoncxx::builder::stream::open_document << 
								"$match" << bsoncxx::builder::stream::open_document <<  
												"$expr" << bsoncxx::builder::stream::open_document <<  
															"$and" << bsoncxx::builder::stream::open_array <<
																		bsoncxx::builder::stream::open_document << "$eq" << bsoncxx::builder::stream::open_array << "$a1" << "$$a0" << bsoncxx::builder::stream::close_array << bsoncxx::builder::stream::close_document <<
																		bsoncxx::builder::stream::open_document << "$ne" << bsoncxx::builder::stream::open_array << "$_id" << "$$id" << bsoncxx::builder::stream::close_array << bsoncxx::builder::stream::close_document <<
																	  bsoncxx::builder::stream::close_array <<
														   bsoncxx::builder::stream::close_document << 
											bsoncxx::builder::stream::close_document << 
								bsoncxx::builder::stream::close_document << 
								bsoncxx::builder::stream::open_document << 
								"$limit" << state.range(1) <<
								bsoncxx::builder::stream::close_document << 
								bsoncxx::builder::stream::close_array << bsoncxx::builder::stream::finalize);
		pipe.match(bsoncxx::builder::stream::document{} << "match" << bsoncxx::builder::stream::open_document << "$ne" << bsoncxx::builder::basic::array{}  << bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::finalize);
		pipe.unwind("$match");
		pipe.limit(state.range(1));

		mongocxx::options::aggregate options;
		options.batch_size(INT32_MAX);
		options.allow_disk_use(true);
		options.bypass_document_validation(true);
		state.ResumeTiming();
		session.start_transaction();
		auto result = collection.aggregate(session, pipe, options);

		for(auto i : result) {
			++count;
		}
		session.commit_transaction();
	}

	if(state.thread_index == 0) {
		//collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}
	state.SetComplexityN(state.range(1));

	state.SetItemsProcessed(state.range(1)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Indexes", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MONGO_Read_Join_Transact)->Apply(CustomArgumentsInserts5)->Complexity()->DenseThreadRange(1,4);

static void BM_MONGO_Read_Join_Manual(benchmark::State& state) {
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
		for(int index = 0; index < state.range(0); ++index) {
			auto idxbuilder = bsoncxx::builder::stream::document{};
			idxbuilder << ("a" + to_string(index)) << 1;
			// One index (basically just many indexes)
			collection.create_index(idxbuilder << bsoncxx::builder::stream::finalize, index_options);
		}
	}
	auto session = conn->start_session();
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		vector<bsoncxx::document::view> batch(state.range(2));
		vector<pair<bsoncxx::document::view, vector<bsoncxx::document::view>>> results(state.range(1));
		state.ResumeTiming();
		mongocxx::options::find opts;
		opts.limit(state.range(1));
		auto cursor = collection.find(session, {}, opts);
		for(auto doc : cursor) {
			batch.push_back(doc);
			// Our batch is ready...
			if(batch.size() == state.range(2)) {
				auto query = bsoncxx::builder::stream::document{};
				auto partial = query << "$or" << bsoncxx::builder::stream::open_array;
				for(auto querydoc : batch) {
					vector<bsoncxx::document::view> a;
					results.push_back(make_pair(querydoc, a));
					partial = partial << bsoncxx::builder::stream::open_document << "$and" << bsoncxx::builder::stream::open_array << bsoncxx::builder::stream::open_document << "a1" << querydoc["a0"].get_int64() << bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::open_document << "_id"  << bsoncxx::builder::stream::open_document << "$ne" << querydoc["_id"].get_oid() << bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::close_array << bsoncxx::builder::stream::close_document;
				} 
				mongocxx::options::find opts;
				opts.limit(state.range(1));
				auto afterin = partial << bsoncxx::builder::stream::close_array;
				auto cursorinternal = collection.find(session, afterin << bsoncxx::builder::stream::finalize, opts);
				for(auto doc : cursor) {
					for(auto res: results) {
						if(res.first["a0"].get_int64() == doc["a0"].get_int64()) {
							res.second.push_back(doc);
							++count;
							break;
						}
					}
					if(count >= state.range(1))
						break;
				}
				batch.clear();
			}
			if(count >= state.range(1))
				break;
		}
	}

	if(state.thread_index == 0) {
		//collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}
	state.SetComplexityN(state.range(1));

	state.SetItemsProcessed(state.range(1)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Indexes", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Batch", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MONGO_Read_Join_Manual)->Apply(CustomArgumentsInserts6)->Complexity()->DenseThreadRange(1,4);
 
static void BM_MONGO_Read_Join_Manual_Transact(benchmark::State& state) {
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
		for(int index = 0; index < state.range(0); ++index) {
			auto idxbuilder = bsoncxx::builder::stream::document{};
			idxbuilder << ("a" + to_string(index)) << 1;
			// One index (basically just many indexes)
			collection.create_index(idxbuilder << bsoncxx::builder::stream::finalize, index_options);
		}
	}
	auto session = conn->start_session();
	uint64_t count = 0;
	for(auto _ : state) {
		state.PauseTiming();
		vector<bsoncxx::document::view> batch(state.range(2));
		vector<pair<bsoncxx::document::view, vector<bsoncxx::document::view>>> results(state.range(1));
		state.ResumeTiming();
		session.start_transaction();
		mongocxx::options::find opts;
		opts.limit(state.range(1));
		auto cursor = collection.find(session, {}, opts);
		for(auto doc : cursor) {
			batch.push_back(doc);
			// Our batch is ready...
			if(batch.size() == state.range(2)) {
				auto query = bsoncxx::builder::stream::document{};
				auto partial = query << "$or" << bsoncxx::builder::stream::open_array;
				for(auto querydoc : batch) {
					vector<bsoncxx::document::view> a;
					results.push_back(make_pair(querydoc, a));
					partial = partial << bsoncxx::builder::stream::open_document << "$and" << bsoncxx::builder::stream::open_array << bsoncxx::builder::stream::open_document << "a1" << querydoc["a0"].get_int64() << bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::open_document << "_id"  << bsoncxx::builder::stream::open_document << "$ne" << querydoc["_id"].get_oid() << bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::close_document << bsoncxx::builder::stream::close_array << bsoncxx::builder::stream::close_document;
				} 
				mongocxx::options::find opts;
				opts.limit(state.range(1));
				auto afterin = partial << bsoncxx::builder::stream::close_array;
				auto cursorinternal = collection.find(session, afterin << bsoncxx::builder::stream::finalize, opts);
				for(auto doc : cursor) {
					for(auto res: results) {
						if(res.first["a0"].get_int64() == doc["a0"].get_int64()) {
							res.second.push_back(doc);
							++count;
							break;
						}
					}
					if(count >= state.range(1))
						break;
				}
				batch.clear();
			}
			if(count >= state.range(1))
				break;
		}
		session.commit_transaction();
	}

	if(state.thread_index == 0) {
		//collection.drop();
		// This is the first thread, so do destruction here (delete documents etc..)
		// TODO Figure out way to kill collection after all tests of suite is done
	}
	state.SetComplexityN(state.range(1));

	state.SetItemsProcessed(state.range(1)*state.iterations());

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark.
	// Meaning: per one second, how many 'foo's are processed?
	state.counters["Ops"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate);

	// Set the counter as a rate. It will be presented divided
	// by the duration of the benchmark, and the result inverted.
	// Meaning: how many seconds it takes to process one 'foo'?
	state.counters["OpsInv"] = benchmark::Counter(state.iterations(), benchmark::Counter::kIsRate | benchmark::Counter::kInvert);
	state.counters.insert({{"Indexes", benchmark::Counter(state.range(0), benchmark::Counter::kAvgThreads)}, {"Limit", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Batch", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MONGO_Read_Join_Manual_Transact)->Apply(CustomArgumentsInserts6)->Complexity()->DenseThreadRange(1,4);
