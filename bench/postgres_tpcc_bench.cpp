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
#include <vector>
#include <deque>

//#define PRINT_BENCH_GEN
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
    PostgreSQLDBHandler::DropTable(conn, "bench", "warehouse");
    PostgreSQLDBHandler::DropTable(conn, "bench", "district");
    PostgreSQLDBHandler::DropTable(conn, "bench", "history");
    PostgreSQLDBHandler::DropTable(conn, "bench", "customer");
    PostgreSQLDBHandler::DropTable(conn, "bench", "item");
    PostgreSQLDBHandler::DropTable(conn, "bench", "new_order");
    PostgreSQLDBHandler::DropTable(conn, "bench", "order_line");
    PostgreSQLDBHandler::DropTable(conn, "bench", "\"order\"");
    PostgreSQLDBHandler::DropTable(conn, "bench", "stock");
    string createQuery = R"|(
CREATE TABLE warehouse (
	w_id SMALLINT PRIMARY KEY DEFAULT '1' NOT NULL,
	w_name VARCHAR(16) NOT NULL,
	w_street_1 VARCHAR(32) NOT NULL,
	w_street_2 VARCHAR(32) NOT NULL,
	w_city VARCHAR(32) NOT NULL,
	w_state VARCHAR(2) NOT NULL,
	w_zip VARCHAR(9) NOT NULL,
	w_tax FLOAT NOT NULL,
	w_ytd FLOAT NOT NULL
);

CREATE TABLE "district" (
  "d_w_id" SMALLINT NOT NULL,
  "d_next_o_id" integer NOT NULL,
  "d_id" SMALLINT NOT NULL,
  "d_ytd" FLOAT NOT NULL,
  "d_tax" FLOAT NOT NULL,
  "d_name" VARCHAR(10) NOT NULL,
  "d_street_1" VARCHAR(20) NOT NULL,
  "d_street_2" VARCHAR(20) NOT NULL,
  "d_city" VARCHAR(20) NOT NULL,
  "d_state" VARCHAR(2) NOT NULL,
  "d_zip" VARCHAR(9) NOT NULL,
  PRIMARY KEY ("d_w_id", "d_id")
);

CREATE TABLE "item" (
  "i_id" integer PRIMARY KEY NOT NULL,
  "i_im_id" integer NOT NULL,
  "i_name" VARCHAR(24) NOT NULL,
  "i_price" FLOAT NOT NULL,
  "i_data" VARCHAR(50) NOT NULL
);
)|";
// TODO indexing
    {
        pqxx::nontransaction N(*conn);
        pqxx::result R(N.exec(createQuery));
    }
    // if (state.range(2) > 0) {
    //     for (int index = 0; index < state.range(2); ++index) {
    //         pqxx::nontransaction N(*conn);
    //         pqxx::result R(N.exec("CREATE INDEX field" + to_string(index) +
    //                               " ON bench.create_bench\r\n(a" +
    //                               to_string(index) + ");"));
    //     }
    // }
    // Use clients to scale loading too
    vector<vector<int>> w_ids;
    w_ids.resize(clients);

    cout << "Warehouses: " << params.warehouses << endl;
    for (int w_id = params.startingWarehouse; w_id <= params.endingWarehouse;
         ++w_id) {
#ifdef PRINT_BENCH_GEN
        cout << w_id << endl;
#endif
        w_ids[w_id % clients].push_back(w_id);
    }

    int threadId;
    const static int BATCH_SIZE = 500;
	#pragma omp parallel private(threadId) num_threads(clients)
    {
        auto pgconn = PostgreSQLDBHandler::GetConnection();
        threadId = omp_get_thread_num();
#ifdef PRINT_BENCH_GEN
        cout << "Thread: " << threadId << endl;
#endif
        // Need to load items...
        if(threadId == 0) {
            auto originalRows = randomHelper.uniqueIds(params.items/10, 1, params.items);
            std::vector<Item> items;
            items.reserve(BATCH_SIZE);

            for(int iId = 1; iId <= params.items; ++iId) {
                items.push_back(Item());
                randomHelper.generateItem(iId, find(originalRows.begin(), originalRows.end(), iId) != originalRows.end(), items.back());
#ifdef PRINT_BENCH_GEN
                cout << items.back();
#endif
                if(items.size() == BATCH_SIZE) {
                    // TODO drain items into DB and clear
                    string insertQuery = "INSERT INTO bench.item VALUES\r\n";
                    for(int i = 0; i < items.size(); ++i) {
                        insertQuery += " (" + to_string(items[i].iId) + "," +
                                       to_string(items[i].iImId) + ",'" +
                                       items[i].iName + "'," +
                                       to_string(items[i].iPrice) + ",'" +
                                       items[i].iData+"'";
                        if (i == items.size() - 1) {
                            insertQuery.append(");");
                        } else {
                            insertQuery.append("),");
                        }
                    }
                    {
                        pqxx::nontransaction N(*pgconn);
                        pqxx::result R(N.exec(insertQuery));
                    }
                    items.clear();
                }
            }

            if (items.size() > 0) {
                string insertQuery = "INSERT INTO bench.item VALUES\r\n";
                for (int i = 0; i < items.size(); ++i) {
                        insertQuery += " (" + to_string(items[i].iId) + "," +
                                       to_string(items[i].iImId) + ",\"" +
                                       items[i].iName + "\"," +
                                       to_string(items[i].iPrice) + ",\"" +
                                       items[i].iData+"\"";
                    if (i == items.size() - 1) {
                        insertQuery.append(");");
                    } else {
                        insertQuery.append("),");
                    }
                }
                {
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                }
                items.clear();
            }
        }
        for (int wId : w_ids[threadId]) {
            Warehouse warehouse;
            randomHelper.generateWarehouse(wId, warehouse);
#ifdef PRINT_BENCH_GEN
                cout << warehouse;
#endif
                // TODO insert into DB
                string insertQuery = "INSERT INTO bench.warehouse VALUES\r\n(" + 
                to_string(warehouse.wId) + ",'" +
                warehouse.wName + "','" +
                warehouse.wAddress.street1 + "','" +
                warehouse.wAddress.street2 + "','" +
                warehouse.wAddress.city + "','" +
                warehouse.wAddress.state + "','" +
                warehouse.wAddress.zip + "'," + to_string(warehouse.wTax) + "," +
                to_string(warehouse.wYtd) + ");";
                {
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                }

                for (int dId = 1; dId <= params.districtsPerWarehouse; ++dId) {
                    int dNextOid = params.customersPerDistrict + 1;
                    District dist;
                    randomHelper.generateDistrict(dId, wId, dNextOid, dist);
#ifdef PRINT_BENCH_GEN
                    cout << dist;
#endif

                    auto selectedBadCredits =
                        randomHelper.uniqueIds(params.customersPerDistrict / 10,
                                               1, params.customersPerDistrict);

                    std::vector<Customer> customers;
                    std::vector<History> histories;
                    customers.reserve(params.customersPerDistrict);
                    histories.reserve(params.customersPerDistrict);
                    deque<int> cIdPermuation;
                    for (int cId = 1; cId <= params.customersPerDistrict;
                         ++cId) {
                        Customer cust;
                        randomHelper.generateCustomer(
                            wId, dId, cId,
                            find(selectedBadCredits.begin(),
                                 selectedBadCredits.end(),
                                 cId) != selectedBadCredits.end(),
                            cust);
#ifdef PRINT_BENCH_GEN
                        cout << cust;
#endif
                        customers.push_back(cust);
                        // TODO insert into DB

                        History hist;
                        randomHelper.generateHistory(wId, dId, cId, hist);
#ifdef PRINT_BENCH_GEN
                        cout << hist;
#endif
                        histories.push_back(hist);
                        // TODO insert into DB

                        cIdPermuation.push_back(cId);
                    } // Customer

                    assert(cIdPermuation[0] == 1);
                    assert(cIdPermuation[params.customersPerDistrict - 1] ==
                           params.customersPerDistrict);

                    randomHelper.shuffle(cIdPermuation);

                    std::vector<Order> orders;
                    std::vector<OrderLine> orderLines;
                    std::vector<NewOrder> newOrders;
                    orders.reserve(params.customersPerDistrict);
                    orderLines.reserve(params.customersPerDistrict *
                                       MAX_OL_CNT);
                    newOrders.reserve(params.newOrdersPerDistrict);
                    for (int oId = 1; oId <= params.customersPerDistrict;
                         ++oId) {
                        int oOlCnt =
                            randomHelper.number(MIN_OL_CNT, MAX_OL_CNT);
                        Order order;
                        bool newOrder = (params.customersPerDistrict -
                                         params.newOrdersPerDistrict) < oId;
                        randomHelper.generateOrder(wId, dId, oId,
                                                   cIdPermuation[oId - 1],
                                                   oOlCnt, newOrder, order);
#ifdef PRINT_BENCH_GEN
                        cout << order;
#endif
                        // TODO insert into DB

                        for (int olNumber = 0; olNumber < oOlCnt; ++olNumber) {
                            OrderLine line;
                            randomHelper.generateOrderLine(
                                params, wId, dId, oId, olNumber, params.items,
                                newOrder, line);
#ifdef PRINT_BENCH_GEN
                            cout << line;
#endif
                            // TODO insert into DB
                        }

                        if (newOrder) {
                            NewOrder newOrder;
                            newOrder.wId = wId;
                            newOrder.dId = dId;
                            newOrder.oId = oId;
#ifdef PRINT_BENCH_GEN
                            cout << "NewOrder: " << newOrder.oId << " "
                                 << newOrder.wId << " " << newOrder.dId << endl;
#endif
                            newOrders.push_back(newOrder);
                            // TODO insert into neworder DB
                        }
                    } // Order
                    // TODO save district
                    // TODO save customers
                    // TODO save orders
                    // TODO save order lines
                    // TODO save new order
                    // TODO save history
                } // District

                vector<Stock> stocks;
                stocks.reserve(BATCH_SIZE);
                auto originalStockItems =
                    randomHelper.uniqueIds(params.items / 10, 1, params.items);
                for (int iId = 1; iId < params.items; ++iId) {
                    stocks.push_back(Stock());
                    randomHelper.generateStock(wId, iId,
                                               find(originalStockItems.begin(),
                                                    originalStockItems.end(),
                                                    iId) !=
                                                   originalStockItems.end(),
                                               stocks.back());
#ifdef PRINT_BENCH_GEN
                    cout << stocks.back();
#endif
                    if (stocks.size() == BATCH_SIZE) {
                        // TODO write to DB
                        stocks.clear();
                    }
                } // Stock items
                if (stocks.size() > 0) {
                        // TODO write to DB
                    stocks.clear();
                }
        } // Warehouse
    } // Per thread/client

    created = true;
    auto end = chrono::steady_clock::now();
    cout << " Done in " << chrono::duration<double, milli>(end - start).count()
         << " ms" << endl
         << endl;
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
                                         string("delete_bench") + postfix +
                                         R"|( ( _id  serial PRIMARY KEY,
  )|";
                    for (int field = 0; field < Precalculator::Columns;
                         ++field) {
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
