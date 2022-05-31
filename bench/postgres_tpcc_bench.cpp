#include "benchmark/benchmark.h"
#include "dbphd/postgresql/postgresql.hpp"
#include "dbphd/tpc/tpchelpers.hpp"

using namespace tpcc;

#include <omp.h>
#include <chrono>
#include <iostream>
#include <pqxx/nontransaction.hxx>
#include <pqxx/result.hxx>
#include <pqxx/transaction_base.hxx>
#include <random>

using namespace std;

static void CustomArgumentsInserts(benchmark::internal::Benchmark *b) {
    for (int i = 1; i <= (1 << 12); i *= 8) { // Documents
        for (int j = 1; j <= 16; j *= 2) {    // Fields
            for (int k = 0; k <= j;) {        // Indexes
                b->Args({i, j, k});
                if (k == 0) {
                    k = 1;
                } else {
                    k *= 2;
                }
            }
        }
    }
}
/*static void BM_PQXX_Select(benchmark::State& state) {
        auto conn = PostgreSQLDBHandler::GetConnection();
          const std::string query = "select 500";
          for(auto _ : state) {
                  pqxx::nontransaction N(*conn);
                  pqxx::result R(N.exec(query));
          }
}

BENCHMARK(BM_PQXX_Select);

static void BM_PQXX_SelectTransact(benchmark::State& state) {
        auto conn = PostgreSQLDBHandler::GetConnection();
          const std::string query = "select 500";
          for(auto _ : state) {
                  pqxx::work w(*conn);
                  // work::exec1() executes a query returning a single row of
data.
                  // We'll just ask the database to return the number 1 to us.
                  pqxx::row r = w.exec1("SELECT 500");
                  // Commit your transaction.  If an exception occurred before
this
                  // point, execution will have left the block, and the
transaction will
                  // have been destroyed along the way.  In that case, the
failed
                  // transaction would implicitly abort instead of getting to
this point. w.commit();
          }
}

BENCHMARK(BM_PQXX_SelectTransact);*/

static void LoadBenchmark(std::shared_ptr<pqxx::connection> conn,
                          const ScaleParameters &params, int clients) {
    static volatile bool created = false;
    if (created)
        return;
    cout << endl << "Creating Postgres old TPC-C Tables with " << clients << " threads ..." << endl;
    cout.flush();
    auto start = chrono::steady_clock::now();
    try {
        PostgreSQLDBHandler::CreateDatabase(conn, "bench");
    } catch (...) {
    }
    // Use clients to scale loading too
    vector<vector<int>> w_ids;
    w_ids.resize(clients);

    cout << "Warehouses: " << params.warehouses << endl;
    for (int w_id = params.startingWarehouse; w_id <= params.endingWarehouse;
         ++w_id) {
        cout << w_id << endl;
        w_ids[w_id % clients].push_back(w_id);
    }

    int threadId;
    const static int BATCH_SIZE = 500;
	#pragma omp parallel private(threadId) num_threads(clients)
    {
        threadId = omp_get_thread_num();
        cout << "Thread: " << threadId << endl;
        // Need to load items...
        if(threadId == 0) {
            auto originalRows = randomHelper.uniqueIds(params.items/10, 1, params.items);
            std::vector<Item> items;
            items.reserve(BATCH_SIZE);

            for(int iId = 0; iId < params.items; ++iId) {
                items.push_back(Item());
                randomHelper.generateItem(iId+1, find(originalRows.begin(), originalRows.end(), iId+1) != originalRows.end(), items[items.size()-1]);
                //cout << items[items.size()-1].toString();
                if(items.size() == BATCH_SIZE) {
                    // TODO drain items into DB and clear
                    items.clear();
                }
            }

            if (items.size() > 0) {
                // TODO drain items into DB and clear
                items.clear();
            }
        }
        for (int wId : w_ids[threadId]) {
            Warehouse w;
            randomHelper.generateWarehouse(wId, w);
                cout << w.toString();
            // TODO insert into DB

            for(int dId = 1; dId <= params.districtsPerWarehouse; ++dId) {
                int dNextOid = params.customersPerDistrict+1;
                District dist;
                randomHelper.generateDistrict(dId, wId, dNextOid, dist);
                cout << dist.toString();

                // TODO insert into DB

                auto selectedBadCredits = randomHelper.uniqueIds(params.customersPerDistrict/10, 1, params.customersPerDistrict);
            }
        }
    }

    created = true;
    /*
    static volatile bool created = false;
        if(!created) {
                cout << endl << "Creating Postgres Delete Table " << postfix <<
  "..."; cout.flush(); auto start = chrono::steady_clock::now(); try{
                        PostgreSQLDBHandler::CreateDatabase(conn, "bench");
                } catch(...) {

                }
                PostgreSQLDBHandler::DropTable(conn, "bench",
  "delete_bench"+postfix); string createQuery = R"|( CREATE TABLE )|" +
  string("delete_bench")+postfix + R"|( ( _id  serial PRIMARY KEY,
  )|";
                for(int field = 0; field < Precalculator::Columns; ++field) {
                        createQuery.append("a" + to_string(field) + " INT");
                        if(field != Precalculator::Columns-1)
                                createQuery.append(",\r\n");
                }
                createQuery.append(R"|(
  );
  )|");
                {
                        pqxx::nontransaction N(*conn);
                        pqxx::result R(N.exec(createQuery));
                }

                int batchsize = 10000;

                for(int i = 0; i < Precalculator::Rows; i += batchsize) {
                        string query = "INSERT INTO bench.delete_bench" +
  postfix
  + " VALUES\r\n"; for(int j = 0; j < batchsize && i+j <
  Precalculator::Rows;++j) { query.append("	(DEFAULT"); int iteration = i+j;
  auto& rowval = Precalculator::PrecalcValues[iteration]; for(int f = 0; f <
  Precalculator::Columns; ++f) { query.append("," + to_string(rowval[f]));
                                }
                                if((j != batchsize - 1) && j+i !=
  Precalculator::Rows-1) { query.append("),"); } else { query.append(");");
                                        pqxx::nontransaction N(*conn);
                                        pqxx::result R(N.exec(query));
                                }
                        }
                }
                auto end = chrono::steady_clock::now();
                cout<< " Done in " << chrono::duration <double, milli>
  (end-start).count() << " ms" << endl << endl; cout.flush(); created[postfix] =
  true;
        }
  */
}

static void BM_PQXX_TPCC_OLD(benchmark::State &state) {
    auto conn = PostgreSQLDBHandler::GetConnection();
    ScaleParameters params = ScaleParameters::makeDefault(4);
    LoadBenchmark(conn, params, 4);
    const std::string query = "select 500";
    for (auto _ : state) {
        pqxx::nontransaction N(*conn);
        pqxx::result R(N.exec(query));
    }
}

BENCHMARK(BM_PQXX_TPCC_OLD);
