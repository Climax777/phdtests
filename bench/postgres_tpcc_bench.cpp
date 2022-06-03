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
#include <fmt/core.h>
#include <fmt/chrono.h>

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
    PostgreSQLDBHandler::DropTable(conn, "bench", "new_order");
    PostgreSQLDBHandler::DropTable(conn, "bench", "history");
    PostgreSQLDBHandler::DropTable(conn, "bench", "order_line");
    PostgreSQLDBHandler::DropTable(conn, "bench", "\"order\"");
    PostgreSQLDBHandler::DropTable(conn, "bench", "stock");
    PostgreSQLDBHandler::DropTable(conn, "bench", "item");
    PostgreSQLDBHandler::DropTable(conn, "bench", "customer");
    PostgreSQLDBHandler::DropTable(conn, "bench", "district");
    PostgreSQLDBHandler::DropTable(conn, "bench", "warehouse");
    string createQuery = R"|(
CREATE TABLE warehouse (
	w_id integer PRIMARY KEY DEFAULT '1' NOT NULL,
	w_name VARCHAR(10) NOT NULL,
	w_street_1 VARCHAR(20) NOT NULL,
	w_street_2 VARCHAR(20) NOT NULL,
	w_city VARCHAR(20) NOT NULL,
	w_state VARCHAR(2) NOT NULL,
	w_zip VARCHAR(9) NOT NULL,
	w_tax numeric(4,4) NOT NULL,
	w_ytd numeric(12,2) NOT NULL
);

CREATE TABLE "district" (
  "d_w_id" integer NOT NULL,
  "d_next_o_id" integer NOT NULL,
  "d_id" SMALLINT NOT NULL,
  "d_ytd" numeric(12,2) NOT NULL,
  "d_tax" numeric(4,4) NOT NULL,
  "d_name" VARCHAR(10) NOT NULL,
  "d_street_1" VARCHAR(20) NOT NULL,
  "d_street_2" VARCHAR(20) NOT NULL,
  "d_city" VARCHAR(20) NOT NULL,
  "d_state" VARCHAR(2) NOT NULL,
  "d_zip" VARCHAR(9) NOT NULL,
  PRIMARY KEY ("d_w_id", "d_id")
);

CREATE TABLE "customer" (
  "c_id" integer NOT NULL,
  "c_w_id" integer NOT NULL,
  "c_d_id" smallint NOT NULL,
  "c_payment_cnt" numeric(4) NOT NULL,
  "c_delivery_cnt" numeric(4) NOT NULL,
  "c_first" VARCHAR(16) NOT NULL,
  "c_middle" VARCHAR(2) NOT NULL,
  "c_last" VARCHAR(16) NOT NULL,
  "c_street_1" VARCHAR(20) NOT NULL,
  "c_street_2" VARCHAR(20) NOT NULL,
  "c_city" VARCHAR(20) NOT NULL,
  "c_state" VARCHAR(2) NOT NULL,
  "c_zip" VARCHAR(9) NOT NULL,
  "c_phone" VARCHAR(16) NOT NULL,
  "c_credit" VARCHAR(2) NOT NULL,
  "c_credit_lim" numeric(12,2) NOT NULL,
  "c_discount" numeric(4,4) NOT NULL,
  "c_balance" numeric(12,2) NOT NULL,
  "c_ytd_payment" numeric(12,2) NOT NULL,
  "c_data" VARCHAR(500) NOT NULL,
  "c_since" timestamp DEFAULT 'now' NOT NULL,
  PRIMARY KEY ("c_w_id", "c_d_id", "c_id")
);

CREATE TABLE "history" (
  "h_c_id" integer,
  "h_c_w_id" integer NOT NULL,
  "h_w_id" integer NOT NULL,
  "h_c_d_id" smallint NOT NULL,
  "h_d_id" smallint NOT NULL,
  "h_amount" numeric(6,2) NOT NULL,
  "h_data" varchar(24) NOT NULL,
  "h_date" timestamp NOT NULL
);

CREATE TABLE "new_order" (
  "no_w_id" integer NOT NULL,
  "no_o_id" integer NOT NULL,
  "no_d_id" smallint NOT NULL,
  PRIMARY KEY ("no_w_id", "no_d_id", "no_o_id")
);

CREATE TABLE "order" (
  "o_id" integer NOT NULL,
  "o_w_id" integer NOT NULL,
  "o_d_id" smallint NOT NULL,
  "o_c_id" integer NOT NULL,
  "o_carrier_id" smallint,
  "o_ol_cnt" numeric(2) NOT NULL,
  "o_all_local" numeric(1) NOT NULL,
  "o_entry_d" timestamp default 'now' NOT NULL,
  PRIMARY KEY ("o_w_id", "o_d_id", "o_id")
);

CREATE TABLE "order_line" (
  "ol_o_id" integer NOT NULL,
  "ol_w_id" integer NOT NULL,
  "ol_d_id" smallint NOT NULL,
  "ol_number" smallint NOT NULL,
  "ol_i_id" integer NOT NULL,
  "ol_supply_w_id" integer NOT NULL,
  "ol_quantity" numeric(2) NOT NULL,
  "ol_amount" numeric(6,2),
  "ol_dist_info" varchar(24),
  "ol_delivery_d" timestamp,
  PRIMARY KEY ("ol_w_id", "ol_d_id", "ol_o_id", "ol_number")
);

CREATE TABLE "item" (
  "i_id" integer PRIMARY KEY NOT NULL,
  "i_im_id" integer NOT NULL,
  "i_name" VARCHAR(24) NOT NULL,
  "i_price" numeric(5,2) NOT NULL,
  "i_data" VARCHAR(50) NOT NULL
);

CREATE TABLE "stock" (
  "s_i_id" integer NOT NULL,
  "s_w_id" integer NOT NULL,
  "s_ytd" numeric(8) NOT NULL,
  "s_quantity" numeric(4) NOT NULL,
  "s_order_cnt" numeric(4) NOT NULL,
  "s_remote_cnt" numeric(4) NOT NULL,
  "s_dist_01" varchar(24) NOT NULL,
  "s_dist_02" varchar(24) NOT NULL,
  "s_dist_03" varchar(24) NOT NULL,
  "s_dist_04" varchar(24) NOT NULL,
  "s_dist_05" varchar(24) NOT NULL,
  "s_dist_06" varchar(24) NOT NULL,
  "s_dist_07" varchar(24) NOT NULL,
  "s_dist_08" varchar(24) NOT NULL,
  "s_dist_09" varchar(24) NOT NULL,
  "s_dist_10" varchar(24) NOT NULL,
  "s_data" varchar(50) NOT NULL,
  PRIMARY KEY ("s_w_id", "s_i_id")
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
                    // drain items into DB and clear
                    string insertQuery = "INSERT INTO bench.item VALUES\r\n";
                    for (int i = 0; i < items.size(); ++i) {
                        insertQuery += fmt::format(
                            "({:d}, {:d}, '{:s}', {:f}, '{:s}'", items[i].iId,
                            items[i].iImId, items[i].iName, items[i].iPrice,
                            items[i].iData);
                        if (i == items.size() - 1) {
                            insertQuery.append(");");
                        } else {
                            insertQuery.append("),\r\n");
                        }
                    }
                   try {
                        pqxx::nontransaction N(*pgconn);
                        pqxx::result R(N.exec(insertQuery));
                   } catch (pqxx::pqxx_exception& e) {
                       cerr << "Item insert failed: " << e.base().what()
                            << endl;
                       cerr << insertQuery << endl;
                   }
                    items.clear();
                }
            }

            if (items.size() > 0) {
                string insertQuery = "INSERT INTO bench.item VALUES\r\n";
                for (int i = 0; i < items.size(); ++i) {
                        insertQuery += fmt::format(
                            "({:d}, {:d}, '{:s}', {:f}, '{:s}'", items[i].iId,
                            items[i].iImId, items[i].iName, items[i].iPrice,
                            items[i].iData);
                    if (i == items.size() - 1) {
                        insertQuery.append(");");
                    } else {
                        insertQuery.append("),\r\n");
                    }
                }
                try
                {
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                   } catch (pqxx::pqxx_exception& e) {
                    cerr << "Item cntd. insert failed: " << e.base().what() << endl;
                    cerr << insertQuery << endl;
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
                string insertQuery = fmt::format("INSERT INTO bench.warehouse VALUES\r\n({:d}, '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', {:f}, {:f});",
                warehouse.wId,
                warehouse.wName,
                warehouse.wAddress.street1,
                warehouse.wAddress.street2,
                warehouse.wAddress.city,
                warehouse.wAddress.state,
                warehouse.wAddress.zip,
                warehouse.wTax,
                warehouse.wYtd);
                try {
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                   } catch (pqxx::pqxx_exception& e) {
                    cerr << "Warehouse insert failed: " << e.base().what() << endl;
                    cerr << insertQuery << endl;
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
                        //cout << cust;
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
                        orders.push_back(order);
#ifdef PRINT_BENCH_GEN
                        cout << order;
#endif
                        // TODO insert into DB

                        for (int olNumber = 0; olNumber < oOlCnt; ++olNumber) {
                            OrderLine line;
                            randomHelper.generateOrderLine(
                                params, wId, dId, oId, olNumber, params.items,
                                newOrder, line);
                            orderLines.push_back(line);
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
                    // Save district
                string insertQuery = fmt::format("INSERT INTO bench.district VALUES\r\n({:d}, {:d}, {:d}, {:f}, {:f}, '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}');",
                dist.dWId,
                dist.dNextOId,
                dist.dId,
                dist.dYtd,
                dist.dTax,
                dist.dName,
                dist.dAddress.street1,
                dist.dAddress.street2,
                dist.dAddress.city,
                dist.dAddress.state,
                dist.dAddress.zip);
                try {
                    // cout << "Writing thread " << omp_get_thread_num() << "
                    // district" << endl;
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                } catch (pqxx::pqxx_exception &e) {
                    cerr << "District insert failed: " << e.base().what()
                         << endl;
                    cerr << insertQuery << endl;
                }

                // save customers
                insertQuery = "INSERT INTO bench.customer VALUES\r\n";
                for (int i = 0; i < customers.size(); ++i) {
                        insertQuery += fmt::format(
                            "({:d}, {:d}, {:d}, {:d}, {:d}, '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', {:f}, {:f}, {:f}, {:f}, '{:s}', '{:%Y-%m-%d %H:%M:%S}'", 
                            customers[i].cId,
                            customers[i].cWId,
                            customers[i].cDId,
                            customers[i].cPaymentCnt,
                            customers[i].cDeliveryCnt,
                            customers[i].cFirst,
                            customers[i].cMiddle,
                            customers[i].cLast,
                            customers[i].cAddress.street1,
                            customers[i].cAddress.street2,
                            customers[i].cAddress.city,
                            customers[i].cAddress.state,
                            customers[i].cAddress.zip,
                            customers[i].cPhone,
                            customers[i].cCredit,
                            customers[i].cCreditLimit,
                            customers[i].cDiscount,
                            customers[i].cBalance,
                            customers[i].cYtdPayment,
                            customers[i].cData,
                            customers[i].cSince
                            );
                    if (i == customers.size() - 1) {
                        insertQuery.append(");");
                    } else {
                        insertQuery.append("),\r\n");
                    }
                }
                try{
                    //cout << insertQuery << endl;
                    //cout << "Writing thread " << omp_get_thread_num() << " customers" << endl;
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                   } catch (pqxx::pqxx_exception& e) {
                       cerr << "Customer insert failed: " << e.base().what() << endl;
                       cerr << insertQuery << endl;
                }
                // save orders
                insertQuery = "INSERT INTO bench.order VALUES\r\n";
                for (int i = 0; i < orders.size(); ++i) {
                        insertQuery += fmt::format(
                            "({:d}, {:d}, {:d}, {:d}, {:s}, {:d}, {:d}, '{:%Y-%m-%d %H:%M:%S}'", 
                            orders[i].oId,
                            orders[i].oWId,
                            orders[i].oDId,
                            orders[i].oCId,
                            orders[i].oCarrierId == NULL_CARRIER_ID ? "null" : to_string(orders[i].oCarrierId),
                            orders[i].oOlCnt,
                            orders[i].oAllLocal,
                            orders[i].oEntryD
                            );
                    if (i == orders.size() - 1) {
                        insertQuery.append(");");
                    } else {
                        insertQuery.append("),\r\n");
                    }
                }
                try{
                    //cout << "Writing thread " << omp_get_thread_num() << " orders" << endl;
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                   } catch (pqxx::pqxx_exception& e) {
                       cerr << "Orders insert failed: " << e.base().what() << endl;
                       cerr << insertQuery << endl;
                }
                // save order lines
                insertQuery = "INSERT INTO bench.order_line VALUES\r\n";
                for (int i = 0; i < orderLines.size(); ++i) {
                        insertQuery += fmt::format(
                            "({:d}, {:d}, {:d}, {:d}, {:d}, {:d}, {:d}, {:f}, '{:s}', ",
                            orderLines[i].olOId,
                            orderLines[i].olWId,
                            orderLines[i].olDId,
                            orderLines[i].olNumber,
                            orderLines[i].olIId,
                            orderLines[i].olSupplyWId,
                            orderLines[i].olQuantity,
                            orderLines[i].olAmount,
                            orderLines[i].olDistInfo
                            );
                            if(orderLines[i].olDeliveryD.time_since_epoch().count() == 0) {
                                insertQuery.append("null");
                            } else {
                                insertQuery.append(fmt::format("'{:%Y-%m-%d %H:%M:%S}'", orderLines[i].olDeliveryD));
                            }
                            
                    if (i == orderLines.size() - 1) {
                        insertQuery.append(");");
                    } else {
                        insertQuery.append("),\r\n");
                    }
                }
                try{
                    //cout << insertQuery << endl;
                    //cout << "Writing thread " << omp_get_thread_num() << " order lines" << endl;
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                   } catch (pqxx::pqxx_exception& e) {
                       cerr << "Order lines insert failed: " << e.base().what() << endl;
                       cerr << insertQuery << endl;
                }
                // save new order
                insertQuery = "INSERT INTO bench.new_order VALUES\r\n";
                for (int i = 0; i < newOrders.size(); ++i) {
                        insertQuery += fmt::format(
                            "({:d}, {:d}, {:d}", 
                            newOrders[i].wId,
                            newOrders[i].oId,
                            newOrders[i].dId
                            );
                    if (i == newOrders.size() - 1) {
                        insertQuery.append(");");
                    } else {
                        insertQuery.append("),\r\n");
                    }
                }
                try{
                    //cout << insertQuery << endl;
                    //cout << "Writing thread " << omp_get_thread_num() << " new order lines" << endl;
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                   } catch (pqxx::pqxx_exception& e) {
                       cerr << "New orders insert failed: " << e.base().what() << endl;
                       cerr << insertQuery << endl;
                }
                    // TODO save history
                insertQuery = "INSERT INTO bench.history VALUES\r\n";
                for (int i = 0; i < histories.size(); ++i) {
                        insertQuery += fmt::format(
                            "({:d}, {:d}, {:d}, {:d}, {:d}, {:f}, '{:s}', '{:%Y-%m-%d %H:%M:%S}'", 
                            histories[i].hCId,
                            histories[i].hCWId,
                            histories[i].hWId,
                            histories[i].hCDId,
                            histories[i].hDId,
                            histories[i].hAmount,
                            histories[i].hData,
                            histories[i].hDate
                            );
                    if (i == histories.size() - 1) {
                        insertQuery.append(");");
                    } else {
                        insertQuery.append("),\r\n");
                    }
                }
                try{
                    //cout << insertQuery << endl;
                    //cout << "Writing thread " << omp_get_thread_num() << " history" << endl;
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                   } catch (pqxx::pqxx_exception& e) {
                       cerr << "History insert failed: " << e.base().what() << endl;
                       cerr << insertQuery << endl;
                }
                } // District

                vector<Stock> stocks;
                stocks.reserve(BATCH_SIZE);
                auto originalStockItems =
                    randomHelper.uniqueIds(params.items / 10, 1, params.items);
                for (int iId = 1; iId <= params.items; ++iId) {
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
                        // write to DB
                    string insertQuery = "INSERT INTO bench.stock VALUES\r\n";
                    for (int i = 0; i < stocks.size(); ++i) {
                        insertQuery += fmt::format(
                            "({:d}, {:d}, {:d}, {:d}, {:d}, {:d}, ", 
                            stocks[i].sIId,
                            stocks[i].sWId, 
                            stocks[i].sYtd, 
                            stocks[i].sQuantity, 
                            stocks[i].sOrderCnt, 
                            stocks[i].sRemoteCnt);
                            for(auto dist: stocks[i].sDists) {
                                insertQuery += fmt::format("'{:s}', ", dist);
                            }
                            insertQuery += fmt::format("'{}'", stocks[i].sData);
                        if (i == stocks.size() - 1) {
                            insertQuery.append(");");
                        } else {
                            insertQuery.append("),\r\n");
                        }
                    }
                    try{
                    //cout << "Writing thread " << omp_get_thread_num() << " stock" << endl;
                        pqxx::nontransaction N(*pgconn);
                        pqxx::result R(N.exec(insertQuery));
                   } catch (pqxx::pqxx_exception& e) {
                       cerr << "Stocks insert failed: " << e.base().what() << endl;
                       cerr << insertQuery << endl;
                    }
                        stocks.clear();
                    }
                } // Stock items
                if (stocks.size() > 0) {
                    string insertQuery = "INSERT INTO bench.stock VALUES\r\n";
                    for (int i = 0; i < stocks.size(); ++i) {
                        insertQuery += fmt::format(
                            "({:d}, {:d}, {:d}, {:d}, {:d}, {:d}, ", 
                            stocks[i].sIId,
                            stocks[i].sWId, 
                            stocks[i].sYtd, 
                            stocks[i].sQuantity, 
                            stocks[i].sOrderCnt, 
                            stocks[i].sRemoteCnt);
                            for(auto dist: stocks[i].sDists) {
                                insertQuery += fmt::format("'{:s}', ", dist);
                            }
                            insertQuery += fmt::format("'{}'", stocks[i].sData);
                        if (i == stocks.size() - 1) {
                            insertQuery.append(");");
                        } else {
                            insertQuery.append("),\r\n");
                        }
                    }
                    try{
                    //cout << "Writing thread " << omp_get_thread_num() << " stock cntd." << endl;
                        pqxx::nontransaction N(*pgconn);
                        pqxx::result R(N.exec(insertQuery));
                   } catch (pqxx::pqxx_exception& e) {
                       cerr << "Stocks cntd. insert failed: " << e.base().what() << endl;
                       cerr << insertQuery << endl;
                    }
                    stocks.clear();
                }
        } // Warehouse
    } // Per thread/client

    cout << "Done populating, altering DB..." << endl;

    string alterQuery = R"|(
CREATE UNIQUE INDEX "customer_i2" ON "customer" USING BTREE ("c_w_id", "c_d_id", "c_last", "c_first", "c_id");

CREATE UNIQUE INDEX "orders_i2" ON "order" USING BTREE ("o_w_id", "o_d_id", "o_c_id", "o_id");

ALTER TABLE "district" ADD FOREIGN KEY ("d_w_id") REFERENCES "warehouse" ("w_id");

ALTER TABLE "customer" ADD FOREIGN KEY ("c_w_id", "c_d_id") REFERENCES "district" ("d_w_id", "d_id");

ALTER TABLE "history" ADD FOREIGN KEY ("h_c_w_id", "h_c_d_id", "h_c_id") REFERENCES "customer" ("c_w_id", "c_d_id", "c_id");

ALTER TABLE "history" ADD FOREIGN KEY ("h_w_id", "h_d_id") REFERENCES "district" ("d_w_id", "d_id");

ALTER TABLE "order" ADD FOREIGN KEY ("o_w_id", "o_d_id", "o_c_id") REFERENCES "customer" ("c_w_id", "c_d_id", "c_id");

ALTER TABLE "order_line" ADD FOREIGN KEY ("ol_w_id", "ol_d_id", "ol_o_id") REFERENCES "order" ("o_w_id", "o_d_id", "o_id");

ALTER TABLE "stock" ADD FOREIGN KEY ("s_i_id") REFERENCES "item" ("i_id");
ALTER TABLE "stock" ADD FOREIGN KEY ("s_w_id") REFERENCES "warehouse" ("w_id");

ALTER TABLE "order_line" ADD FOREIGN KEY ("ol_supply_w_id", "ol_i_id") REFERENCES "stock" ("s_w_id", "s_i_id");

ALTER TABLE "new_order" ADD FOREIGN KEY ("no_w_id", "no_d_id", "no_o_id") REFERENCES "order" ("o_w_id", "o_d_id", "o_id");
)|";
// TODO indexing
    {
        pqxx::nontransaction N(*conn);
        pqxx::result R(N.exec(alterQuery));
    }
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
