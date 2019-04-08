#include "benchmark/benchmark.h"
#include "dbphd/mongodb/mongodb.hpp"
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;

static void CustomArgumentsInserts(benchmark::internal::Benchmark* b) {
	for (int i = 1; i <= (1 << 16); i*=8) {
		for (int j = 1; j <= 16; j *= 2) {
			for(int k = 0; k <= j; ) {
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
	auto collection = db.collection("select_bench");
	mongocxx::pipeline stages;
	stages.limit(1);
	stages.project(make_document(kvp("_id", 0), kvp("result", make_document(kvp("$literal", 500)))));
	// Per thread settings...
	if(state.thread_index == 0) {
		// This is the first thread, so do initialization here, build indexes etc...
	}

	for(auto _ : state) {
		for(int x = 0; x < state.range(0); ++x) {
			auto cursor = collection.aggregate(stages);
		}
	}
	state.SetItemsProcessed(state.iterations()*state.range(0));
	state.counters.insert({{"Fields", benchmark::Counter(state.range(1), benchmark::Counter::kAvgThreads)}, {"Indexes", benchmark::Counter(state.range(2), benchmark::Counter::kAvgThreads)}});
}

BENCHMARK(BM_MONGO_Insert)->Apply(CustomArgumentsInserts)->UseRealTime()->Complexity();
BENCHMARK(BM_MONGO_Insert)->Apply(CustomArgumentsInserts)->UseRealTime()->Complexity()->Threads(2);
BENCHMARK(BM_MONGO_Insert)->Apply(CustomArgumentsInserts)->UseRealTime()->Complexity()->Threads(3);
BENCHMARK(BM_MONGO_Insert)->Apply(CustomArgumentsInserts)->UseRealTime()->Complexity()->Threads(4);

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
