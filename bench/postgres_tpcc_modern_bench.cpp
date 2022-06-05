#include "benchmark/benchmark.h"
#include "dbphd/postgresql/postgresql.hpp"
#include "dbphd/tpc/tpchelpers.hpp"

using namespace tpcc;

#include <algorithm>
#include <chrono>
#include <deque>
#include <fmt/chrono.h>
#include <fmt/core.h>
#include <iostream>
#include <omp.h>
#include <pqxx/nontransaction.hxx>
#include <pqxx/result.hxx>
#include <pqxx/transaction_base.hxx>
#include <random>
#include <thread>
#include <vector>

//#define PRINT_BENCH_GEN
//#define PRINT_TRACE
using namespace std;

static void tokenize(const std::string& str, std::vector<std::string> &token_v, char delimiter = ','){
    size_t start = str.find_first_not_of(delimiter), end=start;

    while (start != std::string::npos){
        // Find next occurence of delimiter
        end = str.find(delimiter, start);
        // Push back the token found into vector
        token_v.push_back(str.substr(start, end-start));
        // Skip all occurences of the delimiter to find new start
        start = str.find_first_not_of(delimiter, end);
    }
}
// Only for denormalized version for pqxx
static void ParseFromCompositePQXX(const std::string &entry,
                                   std::vector<std::string> &tokens) {
    string actual = entry.substr(1, entry.size() - 2); // strip ()
    tokenize(actual, tokens);
}

static void LoadBenchmark(std::shared_ptr<pqxx::connection> conn,
                          ScaleParameters &params, int warehouses, int clients) {
    static volatile bool created = false;
    static ScaleParameters oldParams = ScaleParameters::makeDefault(1);
    static volatile int oldclients = 0;
    if (created && oldParams == params && clients == oldclients)
        return;
    created = false;
    oldParams = params;
    oldclients = clients;
    cout << endl
         << "Creating Postgres modern TPC-C Tables with " << omp_get_num_procs()
         << " threads ..." << endl;
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

CREATE TYPE order_line AS (
  "ol_number" smallint,
  "ol_i_id" integer,
  "ol_supply_w_id" integer,
  "ol_quantity" numeric(2),
  "ol_amount" numeric(6,2),
  "ol_dist_info" varchar(24),
  "ol_delivery_d" timestamp
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
  "o_lines" order_line[],
  PRIMARY KEY ("o_w_id", "o_d_id", "o_id")
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
    try
    {
        pqxx::nontransaction N(*conn);
        pqxx::result R(N.exec(createQuery));
    } catch(pqxx::pqxx_exception& e) {
        cerr << "Error building schema:\r\n" << e.base().what() << endl;
        throw;
    }
    // Use clients to scale loading too
    vector<vector<int>> w_ids;
    w_ids.resize(omp_get_num_procs());

    cout << "Warehouses: " << params.warehouses << " clients: " << clients << endl;
    for (int w_id = params.startingWarehouse; w_id <= params.endingWarehouse;
         ++w_id) {
#ifdef PRINT_BENCH_GEN
        cout << w_id << endl;
#endif
        w_ids[w_id % clients].push_back(w_id);
    }

    int threadId;
    const static int BATCH_SIZE = 500;
#pragma omp parallel private(threadId) num_threads(omp_get_num_procs())
    {
        auto pgconn = PostgreSQLDBHandler::GetConnection();
        threadId = omp_get_thread_num();
#ifdef PRINT_BENCH_GEN
        cout << "Thread: " << threadId << endl;
#endif
        // Need to load items...
        if (threadId == 0) {
            auto originalRows =
                randomHelper.uniqueIds(params.items / 10, 1, params.items);
            std::vector<Item> items;
            items.reserve(BATCH_SIZE);

            for (int iId = 1; iId <= params.items; ++iId) {
                items.push_back(Item());
                randomHelper.generateItem(iId,
                                          find(originalRows.begin(),
                                               originalRows.end(),
                                               iId) != originalRows.end(),
                                          items.back());
#ifdef PRINT_BENCH_GEN
                cout << items.back();
#endif
                if (items.size() == BATCH_SIZE) {
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
                    } catch (pqxx::pqxx_exception &e) {
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
                try {
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                } catch (pqxx::pqxx_exception &e) {
                    cerr << "Item cntd. insert failed: " << e.base().what()
                         << endl;
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
            // insert into DB
            string insertQuery = fmt::format(
                "INSERT INTO bench.warehouse VALUES\r\n({:d}, '{:s}', '{:s}', "
                "'{:s}', '{:s}', '{:s}', '{:s}', {:f}, {:f});",
                warehouse.wId, warehouse.wName, warehouse.wAddress.street1,
                warehouse.wAddress.street2, warehouse.wAddress.city,
                warehouse.wAddress.state, warehouse.wAddress.zip,
                warehouse.wTax, warehouse.wYtd);
            try {
                pqxx::nontransaction N(*pgconn);
                pqxx::result R(N.exec(insertQuery));
            } catch (pqxx::pqxx_exception &e) {
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
                    randomHelper.uniqueIds(params.customersPerDistrict / 10, 1,
                                           params.customersPerDistrict);

                std::vector<Customer> customers;
                std::vector<History> histories;
                customers.reserve(params.customersPerDistrict);
                histories.reserve(params.customersPerDistrict);
                deque<int> cIdPermuation;
                for (int cId = 1; cId <= params.customersPerDistrict; ++cId) {
                    Customer cust;
                    randomHelper.generateCustomer(
                        wId, dId, cId,
                        find(selectedBadCredits.begin(),
                             selectedBadCredits.end(),
                             cId) != selectedBadCredits.end(),
                        cust);
                    // cout << cust;
#ifdef PRINT_BENCH_GEN
                    cout << cust;
#endif
                    customers.push_back(cust);

                    History hist;
                    randomHelper.generateHistory(wId, dId, cId, hist);
#ifdef PRINT_BENCH_GEN
                    cout << hist;
#endif
                    histories.push_back(hist);

                    cIdPermuation.push_back(cId);
                } // Customer

                assert(cIdPermuation[0] == 1);
                assert(cIdPermuation[params.customersPerDistrict - 1] ==
                       params.customersPerDistrict);

                randomHelper.shuffle(cIdPermuation);

                std::vector<Order> orders;
                //std::vector<OrderLine> orderLines;
                std::vector<NewOrder> newOrders;
                orders.reserve(params.customersPerDistrict);
                newOrders.reserve(params.newOrdersPerDistrict);
                for (int oId = 1; oId <= params.customersPerDistrict; ++oId) {
                    int oOlCnt = randomHelper.number(MIN_OL_CNT, MAX_OL_CNT);
                    Order order;
                    order.oLines.reserve(MAX_OL_CNT);
                    bool newOrder = (params.customersPerDistrict -
                                     params.newOrdersPerDistrict) < oId;
                    randomHelper.generateOrder(wId, dId, oId,
                                               cIdPermuation[oId - 1], oOlCnt,
                                               newOrder, order);
#ifdef PRINT_BENCH_GEN
                    cout << order;
#endif

                    for (int olNumber = 0; olNumber < oOlCnt; ++olNumber) {
                        OrderLine line;
                        randomHelper.generateOrderLine(params, wId, dId, oId,
                                                       olNumber, params.items,
                                                       newOrder, line);
                        order.oLines.push_back(line);
#ifdef PRINT_BENCH_GEN
                        cout << line;
#endif
                    }
                    orders.push_back(order);

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
                    }
                } // Order
                // Save district
                string insertQuery = fmt::format(
                    "INSERT INTO bench.district VALUES\r\n({:d}, {:d}, {:d}, "
                    "{:f}, {:f}, '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', "
                    "'{:s}');",
                    dist.dWId, dist.dNextOId, dist.dId, dist.dYtd, dist.dTax,
                    dist.dName, dist.dAddress.street1, dist.dAddress.street2,
                    dist.dAddress.city, dist.dAddress.state, dist.dAddress.zip);
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
                        "({:d}, {:d}, {:d}, {:d}, {:d}, '{:s}', '{:s}', "
                        "'{:s}', '{:s}', '{:s}', '{:s}', '{:s}', '{:s}', "
                        "'{:s}', '{:s}', {:f}, {:f}, {:f}, {:f}, '{:s}', "
                        "'{:%Y-%m-%d %H:%M:%S}'",
                        customers[i].cId, customers[i].cWId, customers[i].cDId,
                        customers[i].cPaymentCnt, customers[i].cDeliveryCnt,
                        customers[i].cFirst, customers[i].cMiddle,
                        customers[i].cLast, customers[i].cAddress.street1,
                        customers[i].cAddress.street2,
                        customers[i].cAddress.city, customers[i].cAddress.state,
                        customers[i].cAddress.zip, customers[i].cPhone,
                        customers[i].cCredit, customers[i].cCreditLimit,
                        customers[i].cDiscount, customers[i].cBalance,
                        customers[i].cYtdPayment, customers[i].cData,
                        customers[i].cSince);
                    if (i == customers.size() - 1) {
                        insertQuery.append(");");
                    } else {
                        insertQuery.append("),\r\n");
                    }
                }
                try {
                    // cout << insertQuery << endl;
                    // cout << "Writing thread " << omp_get_thread_num() << "
                    // customers" << endl;
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                } catch (pqxx::pqxx_exception &e) {
                    cerr << "Customer insert failed: " << e.base().what()
                         << endl;
                    cerr << insertQuery << endl;
                }
                // save orders
                insertQuery = "INSERT INTO bench.order VALUES\r\n";
                for (int i = 0; i < orders.size(); ++i) {
                    insertQuery +=
                        fmt::format("({:d}, {:d}, {:d}, {:d}, {:s}, {:d}, "
                                    "{:d}, '{:%Y-%m-%d %H:%M:%S}', ARRAY[\r\n",
                                    orders[i].oId, orders[i].oWId,
                                    orders[i].oDId, orders[i].oCId,
                                    orders[i].oCarrierId == NULL_CARRIER_ID
                                        ? "null"
                                        : to_string(orders[i].oCarrierId),
                                    orders[i].oOlCnt, orders[i].oAllLocal,
                                    orders[i].oEntryD);
                    for (int j = 0; j < orders[i].oLines.size(); ++j) {
                        insertQuery +=
                            fmt::format("\t({:d}, {:d}, {:d}, {:d}, {:f}, "
                                        "'{:s}', ",
                                        orders[i].oLines[j].olNumber,
                                        orders[i].oLines[j].olIId,
                                        orders[i].oLines[j].olSupplyWId,
                                        orders[i].oLines[j].olQuantity,
                                        orders[i].oLines[j].olAmount,
                                        orders[i].oLines[j].olDistInfo);
                        if (orders[i]
                                .oLines[j]
                                .olDeliveryD.time_since_epoch()
                                .count() == 0) {
                            insertQuery.append("null");
                        } else {
                            insertQuery.append(
                                fmt::format("'{:%Y-%m-%d %H:%M:%S}'",
                                            orders[i].oLines[j].olDeliveryD));
                        }

                        if (j == orders[i].oLines.size() - 1) {
                            insertQuery.append(")::bench.order_line");
                        } else {
                            insertQuery.append(")::bench.order_line,\r\n");
                        }
                    }
                    if (i == orders.size() - 1) {
                        insertQuery.append("]);");
                    } else {
                        insertQuery.append("]),\r\n");
                    }
                }
                try {
                    // cout << "Writing thread " << omp_get_thread_num() << "
                    // orders" << endl;
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                } catch (pqxx::pqxx_exception &e) {
                    cerr << "Orders insert failed: " << e.base().what() << endl;
                    cerr << insertQuery << endl;
                }
                // save new order
                insertQuery = "INSERT INTO bench.new_order VALUES\r\n";
                for (int i = 0; i < newOrders.size(); ++i) {
                    insertQuery +=
                        fmt::format("({:d}, {:d}, {:d}", newOrders[i].wId,
                                    newOrders[i].oId, newOrders[i].dId);
                    if (i == newOrders.size() - 1) {
                        insertQuery.append(");");
                    } else {
                        insertQuery.append("),\r\n");
                    }
                }
                try {
                    // cout << insertQuery << endl;
                    // cout << "Writing thread " << omp_get_thread_num() << "
                    // new order lines" << endl;
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                } catch (pqxx::pqxx_exception &e) {
                    cerr << "New orders insert failed: " << e.base().what()
                         << endl;
                    cerr << insertQuery << endl;
                }
                insertQuery = "INSERT INTO bench.history VALUES\r\n";
                for (int i = 0; i < histories.size(); ++i) {
                    insertQuery +=
                        fmt::format("({:d}, {:d}, {:d}, {:d}, {:d}, {:f}, "
                                    "'{:s}', '{:%Y-%m-%d %H:%M:%S}'",
                                    histories[i].hCId, histories[i].hCWId,
                                    histories[i].hWId, histories[i].hCDId,
                                    histories[i].hDId, histories[i].hAmount,
                                    histories[i].hData, histories[i].hDate);
                    if (i == histories.size() - 1) {
                        insertQuery.append(");");
                    } else {
                        insertQuery.append("),\r\n");
                    }
                }
                try {
                    // cout << insertQuery << endl;
                    // cout << "Writing thread " << omp_get_thread_num() << "
                    // history" << endl;
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                } catch (pqxx::pqxx_exception &e) {
                    cerr << "History insert failed: " << e.base().what()
                         << endl;
                    cerr << insertQuery << endl;
                }
            } // District

            vector<Stock> stocks;
            stocks.reserve(BATCH_SIZE);
            auto originalStockItems =
                randomHelper.uniqueIds(params.items / 10, 1, params.items);
            for (int iId = 1; iId <= params.items; ++iId) {
                stocks.push_back(Stock());
                randomHelper.generateStock(
                    wId, iId,
                    find(originalStockItems.begin(), originalStockItems.end(),
                         iId) != originalStockItems.end(),
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
                            stocks[i].sIId, stocks[i].sWId, stocks[i].sYtd,
                            stocks[i].sQuantity, stocks[i].sOrderCnt,
                            stocks[i].sRemoteCnt);
                        for (auto dist : stocks[i].sDists) {
                            insertQuery += fmt::format("'{:s}', ", dist);
                        }
                        insertQuery += fmt::format("'{}'", stocks[i].sData);
                        if (i == stocks.size() - 1) {
                            insertQuery.append(");");
                        } else {
                            insertQuery.append("),\r\n");
                        }
                    }
                    try {
                        // cout << "Writing thread " << omp_get_thread_num() <<
                        // " stock" << endl;
                        pqxx::nontransaction N(*pgconn);
                        pqxx::result R(N.exec(insertQuery));
                    } catch (pqxx::pqxx_exception &e) {
                        cerr << "Stocks insert failed: " << e.base().what()
                             << endl;
                        cerr << insertQuery << endl;
                    }
                    stocks.clear();
                }
            } // Stock items
            if (stocks.size() > 0) {
                string insertQuery = "INSERT INTO bench.stock VALUES\r\n";
                for (int i = 0; i < stocks.size(); ++i) {
                    insertQuery += fmt::format(
                        "({:d}, {:d}, {:d}, {:d}, {:d}, {:d}, ", stocks[i].sIId,
                        stocks[i].sWId, stocks[i].sYtd, stocks[i].sQuantity,
                        stocks[i].sOrderCnt, stocks[i].sRemoteCnt);
                    for (auto dist : stocks[i].sDists) {
                        insertQuery += fmt::format("'{:s}', ", dist);
                    }
                    insertQuery += fmt::format("'{}'", stocks[i].sData);
                    if (i == stocks.size() - 1) {
                        insertQuery.append(");");
                    } else {
                        insertQuery.append("),\r\n");
                    }
                }
                try {
                    // cout << "Writing thread " << omp_get_thread_num() << "
                    // stock cntd." << endl;
                    pqxx::nontransaction N(*pgconn);
                    pqxx::result R(N.exec(insertQuery));
                } catch (pqxx::pqxx_exception &e) {
                    cerr << "Stocks cntd. insert failed: " << e.base().what()
                         << endl;
                    cerr << insertQuery << endl;
                }
                stocks.clear();
            }
        } // Warehouse
    }     // Per thread/client

    cout << "Done populating, altering DB..." << endl;

    string alterQuery = R"|(
CREATE UNIQUE INDEX "customer_i2" ON "customer" USING BTREE ("c_w_id", "c_d_id", "c_last", "c_first", "c_id");

CREATE UNIQUE INDEX "orders_i2" ON "order" USING BTREE ("o_w_id", "o_d_id", "o_c_id", "o_id");

ALTER TABLE "district" ADD FOREIGN KEY ("d_w_id") REFERENCES "warehouse" ("w_id");

ALTER TABLE "customer" ADD FOREIGN KEY ("c_w_id", "c_d_id") REFERENCES "district" ("d_w_id", "d_id");

ALTER TABLE "history" ADD FOREIGN KEY ("h_c_w_id", "h_c_d_id", "h_c_id") REFERENCES "customer" ("c_w_id", "c_d_id", "c_id");

ALTER TABLE "history" ADD FOREIGN KEY ("h_w_id", "h_d_id") REFERENCES "district" ("d_w_id", "d_id");

ALTER TABLE "order" ADD FOREIGN KEY ("o_w_id", "o_d_id", "o_c_id") REFERENCES "customer" ("c_w_id", "c_d_id", "c_id");

ALTER TABLE "stock" ADD FOREIGN KEY ("s_i_id") REFERENCES "item" ("i_id");
ALTER TABLE "stock" ADD FOREIGN KEY ("s_w_id") REFERENCES "warehouse" ("w_id");

ALTER TABLE "new_order" ADD FOREIGN KEY ("no_w_id", "no_d_id", "no_o_id") REFERENCES "order" ("o_w_id", "o_d_id", "o_id");
)|";
    {
        pqxx::nontransaction N(*conn);
        pqxx::result R(N.exec(alterQuery));
    }
    created = true;
    auto end = chrono::steady_clock::now();
    cout << " Done in " << chrono::duration<double, milli>(end - start).count()
         << " ms" << endl
         << endl;
}

static bool doDelivery(benchmark::State &state, ScaleParameters &params,
                DeliveryParams &dparams, pqxx::transaction<> &transaction) {
#ifdef PRINT_TRACE
    cout << "DoDelivery" << endl;
#endif
    string newOrderQuery =
        fmt::format("SELECT * from bench.new_order WHERE no_d_id = {:d} AND "
                    "no_w_id = {:d} ORDER BY no_o_id ASC LIMIT 1;",
                    dparams.dId, dparams.wId);
#ifdef PRINT_TRACE
    cout << "noq" << endl;
#endif
    pqxx::result no_result =
        transaction.exec(newOrderQuery, "DeliveryTXNNewOrder");

    if (no_result.size() == 0) {
        // No orders for this district. TODO report when >1%
        if (state.counters.count("no_new_orders") == 0)
            state.counters["no_new_orders"] = benchmark::Counter(1);
        else {
            state.counters["no_new_orders"].value++;
        }
        return true;
    }

    int oId = no_result.front().at("no_o_id").as<int>();
    assert(oId >= 1);

    string orderQuery = fmt::format(
        "SELECT o_c_id,o_id,o_d_id,o_w_id,o_lines from bench.\"order\" WHERE o_d_id = "
        "{:d} AND o_w_id = {:d} AND o_id = {:d} LIMIT 1;",
        dparams.dId, dparams.wId, oId);

#ifdef PRINT_TRACE
    cout << "oq" << endl;
#endif
    pqxx::row o_result = transaction.exec1(orderQuery, "DeliveryTXNOrder");
    int cId = o_result.at("o_c_id").as<int>();
    // TODO upgrade to new driver (7.2) with to_composite support
    pqxx::array_parser ol_result = o_result["o_lines"].as_array();
    pair<pqxx::array_parser::juncture, string> elem;
    double total = 0;
    do {
        elem = ol_result.get_next();
        if(elem.first == pqxx::array_parser::juncture::string_value) {
            vector<string> line;
            ParseFromCompositePQXX(elem.second,line);
            double amount = stod(line[4]);
            total += amount;
        }
    } while (elem.first != pqxx::array_parser::juncture::done);
    // TODO improve!!
    string orderUpdate = fmt::format(
        "UPDATE bench.\"order\" SET o_carrier_id = {:d},o_lines = ( SELECT "
        "ARRAY_AGG(ROW("
        "ol.ol_number,"
        "ol.ol_i_id,"
        "ol.ol_supply_w_id,"
        "ol.ol_quantity,"
        "ol.ol_amount,"
        "ol.ol_dist_info,"
        "'{:%Y-%m-%d %H:%M:%S}'"
        ")::bench.order_line)"
        "FROM UNNEST(o_lines) ol"
        ") WHERE "
        "o_d_id = {:d} AND o_w_id = {:d} AND o_id = {:d};",
        dparams.oCarrierId, dparams.olDeliveryD, dparams.dId, dparams.wId, oId);
#ifdef PRINT_TRACE
    cout << "ouq" << endl;
#endif
    pqxx::result o_update_result =
        transaction.exec(orderUpdate, "DeliveryTXNUpdateOrder");
    assert(o_update_result.affected_rows() == 1);

//     string orderLineUpdate = fmt::format(
//         "UPDATE bench.order_line SET ol_delivery_d = '{:%Y-%m-%d %H:%M:%S}' "
//         "WHERE ol_d_id = {:d} AND ol_w_id = {:d} AND ol_o_id = {:d};",
//         dparams.olDeliveryD, dparams.dId, dparams.wId, oId);
// 
// #ifdef PRINT_TRACE
//     cout << "oluq" << endl;
// #endif
//     pqxx::result ol_update_result =
//         transaction.exec(orderLineUpdate, "DeliveryTXNUpdateOrderLines");
//     assert(ol_update_result.affected_rows() > 0);

    string custUpdate =
        fmt::format("UPDATE bench.customer SET c_balance = c_balance + {:f} "
                    "WHERE c_d_id = {:d} AND c_w_id = {:d} AND c_id = {:d};",
                    total, dparams.dId, dparams.wId, cId);

#ifdef PRINT_TRACE
    cout << "cuq" << endl;
#endif
    pqxx::result cust_update_result =
        transaction.exec(custUpdate, "DeliveryTXNUpdateCust");
    assert(cust_update_result.affected_rows() == 1);

    string newOrderDelete =
        fmt::format("DELETE from bench.new_order WHERE no_d_id = {:d} AND "
                    "no_w_id = {:d} AND no_o_id = {:d};",
                    dparams.dId, dparams.wId, oId);
#ifdef PRINT_TRACE
    cout << "nod" << endl;
#endif
    pqxx::result no_delete_result =
        transaction.exec0(newOrderDelete, "DeliveryTXNNewOrderDelete");
    assert(no_delete_result.affected_rows() == 1);

    assert(total > 0);

    return true;
}

static bool doDeliveryN(benchmark::State &state, ScaleParameters &params,
                 shared_ptr<pqxx::connection> conn,
                 int n = DISTRICTS_PER_WAREHOUSE) {
#ifdef PRINT_TRACE
    cout << "DoDeliveryN" << endl;
#endif
    DeliveryParams dparams;
    randomHelper.generateDeliveryParams(params, dparams);
    pqxx::transaction<> transaction(*conn);
    for (int dId = 1; dId <= n; ++dId) {
        dparams.dId = dId;
        bool result = doDelivery(state, params, dparams, transaction);
        if (!result)
            return false;
    }
    transaction.commit();
    return true;
}

static bool doOrderStatus(benchmark::State &state, ScaleParameters &params,
                   shared_ptr<pqxx::connection> conn) {
#ifdef PRINT_TRACE
    cout << "OrderStatus" << endl;
#endif
    OrderStatusParams osparams;
    randomHelper.generateOrderStatusParams(params, osparams);
    pqxx::transaction<> transaction(*conn);

    pqxx::row customer;
    if (osparams.cId != INT32_MIN) {
        string custById = fmt::format(
            "SELECT c_id,c_first,c_middle,c_last,c_balance from bench.customer "
            "WHERE c_id = {:d} AND c_w_id = {:d} AND c_d_id = {:d};",
            osparams.cId, osparams.wId, osparams.dId);

#ifdef PRINT_TRACE
        cout << "cqi" << endl;
#endif
        customer = transaction.exec1(custById, "OrderStatusTXNCustById");
    } else {
        string custByLastName = fmt::format(
            "SELECT c_id,c_first,c_middle,c_last,c_balance from bench.customer "
            "WHERE c_last = '{:s}' AND c_w_id = {:d} AND c_d_id = {:d} ORDER BY c_first;",
            osparams.cLast, osparams.wId, osparams.dId);

#ifdef PRINT_TRACE
        cout << "cql" << endl;
#endif
        pqxx::result customers =
            transaction.exec(custByLastName, "OrderStatusTXNCustByLastName");
        assert(customers.size() > 0);
        int index = (customers.size() - 1) / 2;
        customer = customers[index];
    }
    int cId = customer.at("c_id").as<int>();

    string orderQuery =
        fmt::format("SELECT o_id,o_carrier_id,o_entry_d,o_lines from bench.\"order\" "
                    "WHERE o_c_id = {:d} AND o_w_id = {:d} AND o_d_id = {:d} "
                    "ORDER BY o_id DESC LIMIT 1;",
                    cId, osparams.wId, osparams.dId);

#ifdef PRINT_TRACE
    cout << "oq" << endl;
#endif
    pqxx::row order = transaction.exec1(orderQuery, "OrderStatusTXNOrders");
    assert(!order.empty());

    int oId = order.at("o_id").as<int>();

// Order lines already included
//     string orderLinesQuery = fmt::format(
//         "SELECT ol_supply_w_id,ol_i_id,ol_quantity,ol_amount,ol_delivery_d "
//         "from bench.order_line WHERE ol_d_id = {:d} AND ol_w_id = {:d} AND "
//         "ol_o_id = {:d};",
//         osparams.dId, osparams.wId, oId);
// 
// #ifdef PRINT_TRACE
//     cout << "olq" << endl;
// #endif
//     pqxx::result ol_result =
//         transaction.exec(orderLinesQuery, "OrderStatusTXNOrderLines");
//     assert(ol_result.size() > 0);
    // TODO actually return result... customer, order, orderlines

    transaction.commit();
    return true;
}

static bool doPayment(benchmark::State &state, ScaleParameters &params,
               shared_ptr<pqxx::connection> conn) {
#ifdef PRINT_TRACE
    cout << "Payment" << endl;
#endif
    PaymentParams pparams;
    randomHelper.generatePaymentParams(params, pparams);
    pqxx::transaction<> transaction(*conn);

    string updateDistrict =
        fmt::format("UPDATE bench.district SET d_ytd = d_ytd + {:f} WHERE d_id "
                    "= {:d} AND d_w_id = {:d} RETURNING "
                    "d_name,d_street_1,d_street_2,d_city,d_state,d_zip;",
                    pparams.hAmount, pparams.dId, pparams.wId);

#ifdef PRINT_TRACE
    cout << "distq" << endl;
#endif
    pqxx::row district =
        transaction.exec1(updateDistrict, "PaymentTXNUpdateDistrict");
    assert(!district.empty());

    string updateWarehouse = fmt::format(
        "UPDATE bench.warehouse SET w_ytd = w_ytd + {:f} WHERE w_id = {:d} "
        "RETURNING w_name,w_street_1,w_street_2,w_city,w_state,w_zip;",
        pparams.hAmount, pparams.wId);

#ifdef PRINT_TRACE
    cout << "whq" << endl;
#endif
    pqxx::row warehouse =
        transaction.exec1(updateWarehouse, "PaymentTXNUpdateWarehouse");
    assert(!warehouse.empty());

    pqxx::row customer;
    if (pparams.cId != INT32_MIN) {
        string custById = fmt::format(
            "SELECT "
            "c_id,c_w_id,c_d_id,c_delivery_cnt,c_first,c_middle,c_last,c_"
            "street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_credit,c_"
            "credit_lim,c_discount,c_data,c_since from bench.customer "
            "WHERE c_id = {:d} AND c_w_id = {:d} AND c_d_id = {:d};",
            pparams.cId, pparams.cWId, pparams.cDId);

#ifdef PRINT_TRACE
        cout << "cqi" << endl;
#endif
        customer = transaction.exec1(custById, "PaymentTXNCustById");
    } else {
        string custByLastName = fmt::format(
            "SELECT "
            "c_id,c_w_id,c_d_id,c_delivery_cnt,c_first,c_middle,c_last,c_"
            "street_1,c_street_2,c_city,c_state,c_zip,c_phone,c_credit,c_"
            "credit_lim,c_discount,c_data,c_since from bench.customer "
            "WHERE c_last = '{:s}' AND c_w_id = {:d} AND c_d_id = {:d} ORDER BY c_first;",
            pparams.cLast, pparams.cWId, pparams.cDId);

#ifdef PRINT_TRACE
        cout << "cql" << endl;
#endif
        pqxx::result customers =
            transaction.exec(custByLastName, "PaymentTXNCustByLastName");
        assert(customers.size() > 0);
        int index = (customers.size() - 1) / 2;
        customer = customers[index];
    }
    int cId = customer.at("c_id").as<int>();
    string cData = customer.at("c_data").as<string>();
    string cCredit = customer.at("c_credit").as<string>();

    string cDataChanged = "";
    if (cCredit == BAD_CREDIT) {
        string newData = fmt::format("{:d} {:d} {:d} {:d} {:d} {:f}",
                                     pparams.cId, pparams.cDId, pparams.cWId,
                                     pparams.dId, pparams.wId, pparams.hAmount);
        cData = newData + "|" + cData;
        if (cData.length() > MAX_C_DATA) {
            cData.resize(MAX_C_DATA);
        }
        cDataChanged = fmt::format(" c_data = '{:s}',", cData);
    }

    string updateCustomer = fmt::format(
        "UPDATE bench.customer SET{:s} c_balance = c_balance - {:f}, "
        "c_ytd_payment = c_ytd_payment + {:f}, c_payment_cnt = c_payment_cnt + "
        "1 WHERE c_id = {:d} AND c_w_id = {:d} AND c_d_id = {:d}",
        cDataChanged, pparams.hAmount, pparams.hAmount, cId, pparams.cWId,
        pparams.cDId);
#ifdef PRINT_TRACE
    cout << "cuq" << endl;
#endif
    pqxx::result c_update =
        transaction.exec0(updateCustomer, "PaymentTXNCustUpdate");
    assert(c_update.affected_rows() == 1);

    string h_data =
        fmt::format("{:s}    {:s}", warehouse["w_name"].as<string>(),
                    district["d_name"].as<string>());

    string insertQuery = fmt::format(
        "INSERT INTO bench.history VALUES\r\n ({:d}, {:d}, {:d}, {:d}, "
        "{:d}, {:f}, '{:s}', '{:%Y-%m-%d %H:%M:%S}'",
        cId, pparams.cWId, pparams.wId, pparams.cDId, pparams.dId,
        pparams.hAmount, h_data, pparams.hDate);
#ifdef PRINT_TRACE
    cout << "hi" << endl;
#endif
    pqxx::result insertResult =
        transaction.exec0(updateCustomer, "PaymentTXNHistory");
    assert(insertResult.affected_rows() == 1);
    transaction.commit();
    return true;
}

static bool doStockLevel(benchmark::State &state, ScaleParameters &params,
                  shared_ptr<pqxx::connection> conn) {
#ifdef PRINT_TRACE
    cout << "stockLevel" << endl;
#endif
    StockLevelParams sparams;
    randomHelper.generateStockLevelParams(params, sparams);
    pqxx::transaction<> transaction(*conn);

    string distQuery =
        fmt::format("SELECT d_next_o_id from bench.district "
                    "WHERE d_id = {:d} AND d_w_id = {:d} LIMIT 1;",
                    sparams.dId, sparams.wId);

#ifdef PRINT_TRACE
    cout << "dq" << endl;
#endif
    pqxx::row district = transaction.exec1(distQuery, "StockLevelTXNDistQuery");
    assert(!district.empty());
    int nextOid = district["d_next_o_id"].as<int>();

    string stockQuery = fmt::format(
        "SELECT COUNT(DISTINCT((line::bench.order_line).ol_i_id)) from("
        " SELECT unnest(o_lines) as line, s_i_id FROM bench.order,"
        " bench.stock where o_w_id = {:d} and o_d_id = {:d} and "
        "o_id ="
        " {:d} AND o_id < {:d} AND o_id >= {:d} AND "
        "s_w_id ="
        " 1 AND s_quantity < {:d}) sub WHERE s_i_id ="
        "(line::bench.order_line).ol_i_id;",
        sparams.wId, sparams.dId, nextOid, nextOid - 20, sparams.wId,
        sparams.threshold);
#ifdef PRINT_TRACE
    cout << "sq" << endl;
#endif
    pqxx::result stock =
        transaction.exec(stockQuery, "StockLevelTXNStockQuery");
    assert(stock.size() > 0);
    transaction.commit();
    return true;
}

static bool doNewOrder(benchmark::State &state, ScaleParameters &params,
                shared_ptr<pqxx::connection> conn, int &numFails) {
#ifdef PRINT_TRACE
    cout << "newOrder" << endl;
#endif
    NewOrderParams noparams;
    randomHelper.generateNewOrderParams(params, noparams);

    pqxx::transaction<> transaction(*conn);
    string updateDistrict = fmt::format(
        "UPDATE bench.district SET d_next_o_id = d_next_o_id + 1 WHERE d_id = "
        "{:d} AND d_w_id = {:d} RETURNING d_id,d_w_id,d_tax,d_next_o_id;",
        noparams.dId, noparams.wId);

#ifdef PRINT_TRACE
    cout << "du" << endl;
#endif
    pqxx::row district =
        transaction.exec1(updateDistrict, "NewOrderTXNUpdateDistrict");
    assert(!district.empty());

    double dTax = district["d_tax"].as<double>();
    int dNextOId = district["d_next_o_id"].as<int>();

    // TODO sharding?
    string itemQuery =
        fmt::format("SELECT i_id,i_price,i_name,i_data from bench.item "
                    "WHERE i_id IN ({});",
                    fmt::join(noparams.iIds, ","));
#ifdef PRINT_TRACE
    cout << "iq" << endl;
#endif
    pqxx::result items = transaction.exec(itemQuery, "StockLevelTXNItemQuery");
    if (items.size() != noparams.iIds.size()) {
        numFails++;
        transaction.abort();
        return false;
    }
    // Get id index
    auto getiIdIndex = [&](int iid) {
        int index = find(noparams.iIds.begin(), noparams.iIds.end(), iid) -
                    noparams.iIds.begin();
        assert(index >= 0);
        return index;
    };

    // wId lookup
    auto getwId = [&](int iid) {
        int index = getiIdIndex(iid);
        return noparams.iIWds[index];
    };

    // get Qty index
    auto getQty = [&](int iid) {
        int index = getiIdIndex(iid);
        return noparams.iQtys[index];
    };

    auto getItem = [&](int iid) {
        return *find_if(items.begin(), items.end(),
                        [=](pqxx::result::reference row) {
                            return row["i_id"].as<int>() == iid;
                        });
    };

    string queryWarehouse = fmt::format(
        "SELECT w_tax FROM bench.warehouse WHERE w_id = {:d};", noparams.wId);
#ifdef PRINT_TRACE
    cout << "whq" << endl;
#endif
    pqxx::row warehouse =
        transaction.exec1(queryWarehouse, "NewOrderTXNQueryWarehouse");
    assert(!warehouse.empty());
    double wTax = warehouse["w_tax"].as<double>();

    string queryCustomer =
        fmt::format("SELECT c_discount,c_last,c_credit FROM bench.customer "
                    "WHERE c_w_id = {:d} AND c_d_id = {:d} AND c_id = {:d};",
                    noparams.wId, noparams.dId, noparams.cId);
#ifdef PRINT_TRACE
    cout << "cq" << endl;
#endif
    pqxx::row customer =
        transaction.exec1(queryCustomer, "NewOrderTXNQueryCustomer");
    assert(!customer.empty());
    double cDiscount = customer["c_discount"].as<double>();

    int olCnt = noparams.iIds.size();
    int oCarrierId = NULL_CARRIER_ID;

    // All from same warehouse...
    bool allLocal = all_of(noparams.iIWds.begin(), noparams.iIWds.end(),
                           [=](int i) { return i == noparams.iIWds[0]; });

    pqxx::result stock;
    if (allLocal) {
        string stockQuery = fmt::format(
            "SELECT "
            "s_i_id,s_w_id,s_quantity,s_data,s_ytd,s_order_cnt,s_remote_cnt,s_"
            "dist_{:02d} "
            "from bench.stock "
            "WHERE s_w_id = {:d} AND s_i_id IN ({}) ;",
            noparams.dId, noparams.wId, fmt::join(noparams.iIds, ","));

#ifdef PRINT_TRACE
        cout << "sal" << endl;
#endif
        stock = transaction.exec(stockQuery, "StockLevelTXNStockLocal");
        assert(stock.size() == olCnt);
    } else {
        string stockQuery =
            fmt::format("SELECT "
                        "s_i_id,s_w_id,s_quantity,s_data,s_ytd,s_order_cnt,s_"
                        "remote_cnt,s_dist_{:02d} "
                        "from bench.stock "
                        "WHERE\r\n ",
                        noparams.dId, noparams.wId);

        for (int i = 0; i < noparams.iIds.size(); ++i) {
            stockQuery +=
                fmt::format("(s_w_id = {:d} AND s_i_id = {:d})",
                            getwId(noparams.iIds[i]), noparams.iIds[i]);
            if (i + 1 < noparams.iIds.size()) {
                stockQuery += " OR ";
            } else {
                stockQuery += ";";
            }
        }

#ifdef PRINT_TRACE
        cout << "sor" << endl;
#endif
        stock = transaction.exec(stockQuery, "StockLevelTXNStockRemote");
        assert(stock.size() == olCnt);
    }

    auto getStock = [&](int iid) {
        return *find_if(stock.begin(), stock.end(),
                        [=](pqxx::result::reference row) {
                            return row["s_i_id"].as<int>() == iid;
                        });
    };

    string insertOrder = fmt::format(
        "INSERT INTO bench.order VALUES\r\n "
        "({:d},{:d},{:d},{:d},{:d},{:d},{:d},'{:%Y-%m-%d %H:%M:%S}', ARRAY[",
        dNextOId, noparams.wId, noparams.dId, noparams.cId, oCarrierId, olCnt,
        allLocal, noparams.oEntryDate);
#ifdef PRINT_TRACE
    cout << "io" << endl;
#endif
    vector<tuple<string, int, string, double, double>> itemData;
    itemData.reserve(olCnt);
    double total = 0;
    for (int i = 0; i < olCnt; ++i) {
        int olNumber = i + 1;
        int olIId = noparams.iIds[i];
        int iIdIdx = getiIdIndex(olIId);
        int olSupplyWId = noparams.iIWds[i];
        int olQuantity = noparams.iQtys[i];

        auto item = getItem(olIId);
        auto stockitem = getStock(olIId);

        int sQuantity = stockitem["s_ytd"].as<int>();
        int sYtd = stockitem["s_ytd"].as<int>() + olQuantity;

        if (sQuantity >= olQuantity + 10) {
            sQuantity = sQuantity - olQuantity;
        } else {
            sQuantity = sQuantity + 91 - olQuantity;
        }

        int sOrderCnt = stockitem["s_order_cnt"].as<int>() + 1;
        int sRemoteCnt = stockitem["s_remote_cnt"].as<int>();

        if (olSupplyWId != noparams.wId) {
            sRemoteCnt++;
        }

        string updateStock = fmt::format(
            "UPDATE bench.stock SET s_quantity = {:d}, s_ytd = {:d}, "
            "s_order_cnt = {:d}, s_remote_cnt = {:d} WHERE s_i_id = {:d} AND "
            "s_w_id = {:d}",
            sQuantity, sYtd, sOrderCnt, sRemoteCnt, olIId, olSupplyWId);
#ifdef PRINT_TRACE
        cout << "suq" << endl;
#endif
        pqxx::result updateStockResult =
            transaction.exec0(updateStock, "StockLevelTXNStockUpdate");
        assert(updateStockResult.affected_rows() == 1);

        double olAmount = olQuantity * item["i_price"].as<double>();
        total += olAmount;

        insertOrder += fmt::format(
            "\r\n({:d},{:d},{:d},{:d},{:f},'{:s}',null)::bench.order_line",
            olNumber, olIId, olSupplyWId,
            olQuantity, olAmount, stockitem.back().as<string>());

        if(i+1 < olCnt) {
            insertOrder += ",";
        }

        string iData = item["i_data"].as<string>();
        string sData = stockitem["s_data"].as<string>();
        string brandGeneric = "G";
        if (iData.find(ORIGINAL_STRING) != -1 &&
            sData.find(ORIGINAL_STRING) != -1) {
            brandGeneric = "B";
        }
        itemData.push_back(make_tuple(item["i_name"].as<string>(), sQuantity,
                                      brandGeneric,
                                      item["i_price"].as<double>(), olAmount));
    }
    insertOrder += "]);";
    pqxx::result iOResult =
        transaction.exec0(insertOrder, "StockLevelTXNInsertOrder");
    assert(iOResult.affected_rows() == 1);

    string insertQuery = fmt::format(
        "INSERT INTO bench.new_order VALUES\r\n ({:d}, {:d}, {:d});",
        noparams.wId, dNextOId, noparams.dId);
#ifdef PRINT_TRACE
    cout << "noi" << endl;
#endif
    pqxx::result noInsert =
        transaction.exec0(insertQuery, "NewOrderTXNInsertNewOrder");
    assert(noInsert.affected_rows() == 1);

    total *= (1 - cDiscount) * (1 + wTax + dTax);

    transaction.commit();
    return true;
}

static ScaleParameters params = ScaleParameters::makeDefault(4);
static void BM_PQXX_TPCC_MODERN(benchmark::State &state) {
    auto conn = PostgreSQLDBHandler::GetConnection();
    if (state.thread_index() == 0) {
        int warehouses = state.range(0);
        params = ScaleParameters::makeDefault(warehouses);
        try {
            LoadBenchmark(conn, params, warehouses, state.threads());
        } catch (...) {
            cerr << "Error loading benchmark" << endl;
            throw;
        }
    }
    int numDeliveries = 0;
    int numOrderStatuses = 0;
    int numNewOrders = 0;
    int numFailedNewOrders = 0;
    int numPayments = 0;
    int numStockLevels = 0;
    int numDeadlocks = 0;
    int numOtherErrors = 0;
    for (auto _ : state) {
        // auto start = std::chrono::high_resolution_clock::now();
        tpcc::TransactionType type = randomHelper.nextTransactionType();
        // Start transaction

        try {
        bool result = false;
        switch (type) {
        case tpcc::TransactionType::Delivery:
            result = doDeliveryN(state, params, conn);
            numDeliveries++;
            break;
        case tpcc::TransactionType::OrderStatus:
            result = doOrderStatus(state, params, conn);
            numOrderStatuses++;
            break;
        case tpcc::TransactionType::Payment:
            result = doPayment(state, params, conn);
            numPayments++;
            break;
        case tpcc::TransactionType::StockLevel:
            result = doStockLevel(state, params, conn);
            numStockLevels++;
            break;
        case tpcc::TransactionType::NewOrder:
            result = doNewOrder(state, params, conn, numFailedNewOrders);
            numNewOrders++;
            break;
        }
        } catch(pqxx::deadlock_detected& e) {
            cout << "Deadlock!\r\n" << e.what() << endl;
            numDeadlocks++;
        } catch(pqxx::pqxx_exception& e) {
            cout << "pqxxException!\r\n" << e.base().what() << endl;
            throw;
        } catch(std::exception& e) {
            cout << "std exception!\r\n" << e.what() << endl;
            throw;
        } catch(...) {
            cout << "Unknown exception!\r\n" << endl;
            throw;
        }
        // auto end = std::chrono::high_resolution_clock::now();
        // auto elapsed_seconds =
        //     std::chrono::duration_cast<std::chrono::duration<double>>(end -
        //                                                               start);
        // state.SetIterationTime(elapsed_seconds.count());
    }

    int total = numDeliveries + numNewOrders + numFailedNewOrders +
                numOrderStatuses + numPayments + numStockLevels;

    state.counters["deadlocks"] = numDeadlocks;

    state.counters["txn"] = total;
    state.counters["txnRate"] =
        benchmark::Counter(total, benchmark::Counter::kIsRate);
    state.counters["txnRateInv"] = benchmark::Counter(
        total, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);

    state.counters["delivery"] = numDeliveries;
    state.counters["deliveryRate"] =
        benchmark::Counter(numDeliveries, benchmark::Counter::kIsRate);
    state.counters["deliveryRateInv"] =
        benchmark::Counter(numDeliveries, benchmark::Counter::kIsRate |
                                              benchmark::Counter::kInvert);

    state.counters["newOrder"] = numNewOrders;
    state.counters["newOrderRate"] =
        benchmark::Counter(numNewOrders, benchmark::Counter::kIsRate);
    state.counters["newOrderRateInv"] =
        benchmark::Counter(numNewOrders, benchmark::Counter::kIsRate |
                                             benchmark::Counter::kInvert);

    state.counters["newOrderFail"] = numFailedNewOrders;

    state.counters["payment"] = numPayments;
    state.counters["paymentRate"] =
        benchmark::Counter(numPayments, benchmark::Counter::kIsRate);
    state.counters["paymentRateInv"] = benchmark::Counter(
        numPayments, benchmark::Counter::kIsRate | benchmark::Counter::kInvert);

    state.counters["status"] = numOrderStatuses;
    state.counters["statusRate"] =
        benchmark::Counter(numOrderStatuses, benchmark::Counter::kIsRate);
    state.counters["statusRateInv"] =
        benchmark::Counter(numOrderStatuses, benchmark::Counter::kIsRate |
                                                 benchmark::Counter::kInvert);

    state.counters["stock"] = numStockLevels;
    state.counters["stockRate"] =
        benchmark::Counter(numStockLevels, benchmark::Counter::kIsRate);
    state.counters["stockRateInv"] =
        benchmark::Counter(numStockLevels, benchmark::Counter::kIsRate |
                                               benchmark::Counter::kInvert);
}

BENCHMARK(BM_PQXX_TPCC_MODERN)->RangeMultiplier(2)->Range(1,10)->Iterations(10000)->ThreadRange(1,16)->UseRealTime();
