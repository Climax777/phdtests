#include "benchmark/benchmark.h"
#include "dbphd/mongodb/mongodb.hpp"
#include <mongocxx/exception/exception.hpp>
#include <mongocxx/exception/operation_exception.hpp>
#include <bsoncxx/builder/list.hpp>
#include <bsoncxx/types.hpp>
#include <bsoncxx/types/bson_value/value.hpp>
using bsoncxx::builder::basic::kvp;
using bsoncxx::builder::basic::make_document;
using bsoncxx::builder::list;
using bsoncxx::builder::document;

#include "dbphd/tpc/tpchelpers.hpp"

using namespace tpcc;

#include <algorithm>
#include <chrono>
#include <deque>
#include <fmt/chrono.h>
#include <fmt/core.h>
#include <iostream>
#include <omp.h>
#include <random>
#include <thread>
#include <vector>
#include <unordered_set>

//#define PRINT_BENCH_GEN
//#define PRINT_TRACE
using namespace std;

static void LoadBenchmark(mongocxx::pool::entry& conn,
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
         << "Creating mongodb modern TPC-C Tables with " << omp_get_num_procs()
         << " threads ..." << endl;
    cout.flush();
    auto start = chrono::steady_clock::now();
    {
        auto db = conn->database("bench");
        db.drop();
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
        w_ids[w_id % w_ids.size()].push_back(w_id);
    }

    int threadId;
    const static int BATCH_SIZE = 500;
#pragma omp parallel private(threadId) num_threads(omp_get_num_procs())
    {
        auto mongoconn = MongoDBHandler::GetConnection();
        auto db = mongoconn->database("bench");
        auto warehouseC = db.collection("warehouse");
        auto districtC = db.collection("district");
        auto customerC = db.collection("customer");
        auto historyC = db.collection("history");
        auto orderC = db.collection("order");
        auto stockC = db.collection("stock");
        threadId = omp_get_thread_num();
#ifdef PRINT_BENCH_GEN
        cout << "Thread: " << threadId << endl;
#endif
        // Need to load items...
        if (threadId == 0) {
            auto itemC = db.collection("item");
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
                    auto writer = itemC.create_bulk_write();
                    // drain items into DB and clear
                    for (int i = 0; i < items.size(); ++i) {
                        document itemInsert = {
                            "i_id",         items[i].iId,    "i_im_id",
                            items[i].iImId, "i_name",        items[i].iName,
                            "i_price",      items[i].iPrice, "i_data",
                            items[i].iData,
                        };
                        mongocxx::model::insert_one inserter(MDV(itemInsert));
                        writer.append(inserter);
                    }
                    try {
                        auto result = writer.execute();
                        assert(result.has_value() == true);
                        assert(result.value().inserted_count() == BATCH_SIZE);
                    } catch (mongocxx::exception &e) {
                        cerr << "Item insert failed: " << e.what() << endl;
                    }
                    items.clear();
                }
            }

            if (items.size() > 0) {
                mongocxx::options::bulk_write options;
                options.ordered(false);
                options.bypass_document_validation(true);
                auto writer = itemC.create_bulk_write(options);
                // drain items into DB and clear
                for (int i = 0; i < items.size(); ++i) {
                    document itemInsert = {
                        "i_id",   items[i].iId,   "i_im_id", items[i].iImId,
                        "i_name", items[i].iName, "i_price", items[i].iPrice,
                        "i_data", items[i].iData,
                    };
                    mongocxx::model::insert_one inserter(MDV(itemInsert));
                    writer.append(inserter);
                }
                try {
                    auto result = writer.execute();
                    assert(result.has_value() == true);
                    assert(result.value().inserted_count() == items.size());
                } catch (mongocxx::exception &e) {
                    cerr << "Item ext. insert failed: " << e.what() << endl;
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
            document whInsert = {
                "w_id",      warehouse.wId,
                "w_name",    warehouse.wName,
                "w_street1", warehouse.wAddress.street1,
                "w_street2", warehouse.wAddress.street2,
                "w_city",    warehouse.wAddress.city,
                "w_state",   warehouse.wAddress.state,
                "w_zip",     warehouse.wAddress.zip,
                "w_tax",     warehouse.wTax,
                "w_ytd",     warehouse.wYtd,
            };
            try {
                auto result = warehouseC.insert_one(MDV(whInsert));
                assert(result.has_value() == true);
                assert(result.value().result().inserted_count() == 1);
            } catch (mongocxx::exception &e) {
                cerr << "Warehouse insert failed: " << e.what() << endl;
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
                orders.reserve(params.customersPerDistrict);
                for (int oId = 1; oId <= params.customersPerDistrict; ++oId) {
                    int oOlCnt = randomHelper.number(MIN_OL_CNT, MAX_OL_CNT);
                    Order order;
                    order.oLines.reserve(MAX_OL_CNT);
                    bool newOrder = (params.customersPerDistrict -
                                     params.newOrdersPerDistrict) < oId;
                    randomHelper.generateOrder(wId, dId, oId,
                                               cIdPermuation[oId - 1], oOlCnt,
                                               newOrder, order);

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

                    order.oNew = newOrder;
                    order.oDeliveryD =
                        newOrder ? chrono::system_clock::time_point(0s)
                                 : chrono::system_clock::now();

#ifdef PRINT_BENCH_GEN
                    cout << order;
#endif
                    orders.push_back(order);
                } // Order
                // Save district
                document distInsert = {
                    "d_w_id",      dist.dWId,
                    "d_next_o_id", dist.dNextOId,
                    "d_id",        dist.dId,
                    "d_ytd",       dist.dYtd,
                    "d_tax",       dist.dTax,
                    "d_name",      dist.dName,
                    "d_street1",   dist.dAddress.street1,
                    "d_street2",   dist.dAddress.street2,
                    "d_city",      dist.dAddress.city,
                    "d_state",     dist.dAddress.state,
                    "d_zip",       dist.dAddress.zip,
                };
                try {
                    auto result = districtC.insert_one(MDV(distInsert));
                    assert(result.has_value() == true);
                    assert(result.value().result().inserted_count() == 1);
                } catch (mongocxx::exception &e) {
                    cerr << "District insert failed: " << e.what()
                         << endl;
                }

                // save customers
                auto customerWriter = customerC.create_bulk_write();
                for (int i = 0; i < customers.size(); ++i) {
                    document custInsert = {
                        "c_id", customers[i].cId,
                        "c_w_id", customers[i].cWId,
                        "c_d_id", customers[i].cDId,
                        "c_payment_cnt", customers[i].cPaymentCnt,
                        "c_delivery_cnt", customers[i].cDeliveryCnt,
                        "c_first", customers[i].cFirst,
                        "c_middle", customers[i].cMiddle,
                        "c_last", customers[i].cLast,
                        "c_street1", customers[i].cAddress.street1,
                        "c_street2", customers[i].cAddress.street2,
                        "c_city", customers[i].cAddress.city,
                        "c_state", customers[i].cAddress.state,
                        "c_zip", customers[i].cAddress.zip,
                        "c_phone", customers[i].cPhone,
                        "c_credit", customers[i].cCredit,
                        "c_credit_lim", customers[i].cCreditLimit,
                        "c_discount", customers[i].cDiscount,
                        "c_balance", customers[i].cBalance,
                        "c_ytd_payment", customers[i].cYtdPayment,
                        "c_data", customers[i].cData,
                        "c_since", bsoncxx::types::b_date{customers[i].cSince},
                    };
                    mongocxx::model::insert_one inserter(MDV(custInsert));
                    customerWriter.append(inserter);
                }
                try {
                    auto result = customerWriter.execute();
                    assert(result.has_value() == true);
                    assert(result.value().inserted_count() == customers.size());
                } catch (mongocxx::exception &e) {
                    cerr << "Customer insert failed: " << e.what()
                         << endl;
                }
                // save orders
                auto orderWriter = orderC.create_bulk_write();
                for (int i = 0; i < orders.size(); ++i) {
                    bsoncxx::builder::basic::array orderLines;
                    for (int j = 0; j < orders[i].oLines.size(); ++j) {
                        document orderLineInsert;
                        orderLineInsert = {
                            "ol_o_id",        orders[i].oLines[j].olOId,
                            "ol_w_id",        orders[i].oLines[j].olWId,
                            "ol_d_id",        orders[i].oLines[j].olDId,
                            "ol_number",      orders[i].oLines[j].olNumber,
                            "ol_i_id",        orders[i].oLines[j].olIId,
                            "ol_supply_w_id", orders[i].oLines[j].olSupplyWId,
                            "ol_quantity",    orders[i].oLines[j].olQuantity,
                            "ol_amount",      orders[i].oLines[j].olAmount,
                            "ol_dist_info",   orders[i].oLines[j].olDistInfo,
                        };
                        orderLines.append(MDV(orderLineInsert));
                    }
                    document orderInsert = {
                        "o_id", orders[i].oId,
                        "o_w_id", orders[i].oWId,
                        "o_d_id", orders[i].oDId,
                        "o_c_id", orders[i].oCId,
                        "o_carrier_id", orders[i].oCarrierId == NULL_CARRIER_ID
                                        ? "null"
                                        : to_string(orders[i].oCarrierId),
                        "o_ol_cnt", orders[i].oOlCnt,
                        "o_all_local", orders[i].oAllLocal,
                        "o_entry_d", bsoncxx::types::b_date{orders[i].oEntryD},
                        "o_delivery_d", bsoncxx::types::b_date{orders[i].oDeliveryD},
                        "o_lines", orderLines.extract(),
                    };
                    mongocxx::model::insert_one inserter(MDV(orderInsert));
                    orderWriter.append(inserter);
                }
                try {
                    auto result = orderWriter.execute();
                    assert(result.has_value() == true);
                    assert(result.value().inserted_count() == orders.size());
                } catch (mongocxx::exception &e) {
                    cerr << "Orders insert failed: " << e.what() << endl;
                }

                //save history
                auto historyWriter = historyC.create_bulk_write();
                for (int i = 0; i < histories.size(); ++i) {
                    document historyInsert = {
                        "h_c_id", histories[i].hCId,
                        "h_c_w_id", histories[i].hCWId,
                        "h_w_id", histories[i].hWId,
                        "h_c_d_id", histories[i].hCDId,
                        "h_d_id", histories[i].hDId,
                        "h_amount", histories[i].hAmount,
                        "h_data", histories[i].hData,
                        "h_date", bsoncxx::types::b_date{histories[i].hDate},
                    };
                    mongocxx::model::insert_one inserter(MDV(historyInsert));
                    historyWriter.append(inserter);
                }
                try {
                    auto result = historyWriter.execute();
                    assert(result.has_value() == true);
                    assert(result.value().inserted_count() == histories.size());
                } catch (mongocxx::exception &e) {
                    cerr << "History insert failed: " << e.what()
                         << endl;
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
                    auto writer = stockC.create_bulk_write();
                    for (int i = 0; i < stocks.size(); ++i) {
                        document stockInsert = {
                            "s_i_id", stocks[i].sIId,
                            "s_w_id", stocks[i].sWId,
                            "s_ytd", stocks[i].sYtd,
                            "s_quantity", stocks[i].sQuantity,
                            "s_order_cnt", stocks[i].sOrderCnt,
                            "s_remote_cnt", stocks[i].sRemoteCnt,
                            "s_dist_01", stocks[i].sDists[0],
                            "s_dist_02", stocks[i].sDists[1],
                            "s_dist_03", stocks[i].sDists[2],
                            "s_dist_04", stocks[i].sDists[3],
                            "s_dist_05", stocks[i].sDists[4],
                            "s_dist_06", stocks[i].sDists[5],
                            "s_dist_07", stocks[i].sDists[6],
                            "s_dist_08", stocks[i].sDists[7],
                            "s_dist_09", stocks[i].sDists[8],
                            "s_dist_10", stocks[i].sDists[9],
                            "s_data", stocks[i].sData,
                        };
                        mongocxx::model::insert_one inserter(MDV(stockInsert));
                        writer.append(inserter);
                    }
                    try {
                        auto result = writer.execute();
                        assert(result.has_value() == true);
                        assert(result.value().inserted_count() == stocks.size());
                    } catch (mongocxx::exception &e) {
                        cerr << "Stocks insert failed: " << e.what()
                             << endl;
                    }
                    stocks.clear();
                }
            } // Stock items
            if (stocks.size() > 0) {
                    // write to DB
                    auto writer = stockC.create_bulk_write();
                    for (int i = 0; i < stocks.size(); ++i) {
                        document stockInsert = {
                            "s_i_id", stocks[i].sIId,
                            "s_w_id", stocks[i].sWId,
                            "s_ytd", stocks[i].sYtd,
                            "s_quantity", stocks[i].sQuantity,
                            "s_order_cnt", stocks[i].sOrderCnt,
                            "s_remote_cnt", stocks[i].sRemoteCnt,
                            "s_dist_01", stocks[i].sDists[0],
                            "s_dist_02", stocks[i].sDists[1],
                            "s_dist_03", stocks[i].sDists[2],
                            "s_dist_04", stocks[i].sDists[3],
                            "s_dist_05", stocks[i].sDists[4],
                            "s_dist_06", stocks[i].sDists[5],
                            "s_dist_07", stocks[i].sDists[6],
                            "s_dist_08", stocks[i].sDists[7],
                            "s_dist_09", stocks[i].sDists[8],
                            "s_dist_10", stocks[i].sDists[9],
                            "s_data", stocks[i].sData,
                        };
                        mongocxx::model::insert_one inserter(MDV(stockInsert));
                        writer.append(inserter);
                    }
                    try {
                        auto result = writer.execute();
                        assert(result.has_value() == true);
                        assert(result.value().inserted_count() == stocks.size());
                    } catch (mongocxx::exception &e) {
                        cerr << "Stocks insert cntd. failed: " << e.what()
                             << endl;
                    }
                    stocks.clear();
            }
        } // Warehouse
    }     // Per thread/client

    cout << "Done populating, altering DB..." << endl;

    {
        auto options = mongocxx::options::index();
        options.background(false);
        auto db = conn->database("bench");
        auto warehouseC = db.collection("warehouse");
        warehouseC.create_index(MDV("w_id", 1), options);
        auto districtC = db.collection("district");
        districtC.create_index(MDV("d_w_id", 1, "d_id", 1), options);
        auto customerC = db.collection("customer");
        customerC.create_index(MDV("c_w_id", 1, "c_d_id", 1, "c_id", 1), options);
        customerC.create_index(MDV("c_w_id", 1, "c_d_id", 1, "c_last", 1, "c_first", 1, "c_id", 1), options);
        auto historyC = db.collection("history");
        auto orderC = db.collection("order");
        orderC.create_index(MDV("o_w_id", 1, "o_d_id", 1, "o_id", 1), options);
        orderC.create_index(MDV("o_w_id", 1, "o_d_id", 1, "o_c_id", 1, "o_id", 1), options);
        auto newOrderOptions = mongocxx::options::index();
        newOrderOptions.background(false);
        document pfx = {"o_new", bsoncxx::types::b_bool{true}};
        newOrderOptions.partial_filter_expression(pfx.view().get_document());
        newOrderOptions.name("new_orders");
        orderC.create_index(MDV("o_w_id", 1, "o_d_id", 1, "o_id", 1), newOrderOptions);
        auto itemC = db.collection("item");
        itemC.create_index(MDV("i_id", 1), options);
        auto stockC = db.collection("stock");
        stockC.create_index(MDV("s_w_id", 1, "s_i_id", 1), options);
    }
    created = true;
    auto end = chrono::steady_clock::now();
    cout << " Done in " << chrono::duration<double, milli>(end - start).count()
         << " ms" << endl
         << endl;
}

static bool doDelivery(benchmark::State &state, ScaleParameters &params,
                       DeliveryParams &dparams, mongocxx::pool::entry &conn,
                       mongocxx::client_session &session) {
#ifdef PRINT_TRACE
    cout << "DoDelivery" << endl;
#endif
#ifdef PRINT_TRACE
    cout << "noq" << endl;
#endif
    auto colOrder = conn->database("bench").collection("order");
    auto options = mongocxx::options::find_one_and_update();
    options.projection(MDV("o_lines", 1, "o_c_id", 1, "o_id", 1, "o_d_id", 1, "o_w_id", 1, "_id", 0));
    options.sort(MDV("o_id", 1));
    options.return_document(mongocxx::options::return_document::k_after);
    auto no_result = colOrder.find_one_and_update(session, MDV("o_d_id", dparams.dId, "o_w_id", dparams.wId, "o_new", true), MDV("$unset", MDV("o_new", 1),"$set", MDV("o_carrier_id", dparams.oCarrierId, "o_delivery_d", bsoncxx::types::b_date{dparams.olDeliveryD})), options);
    if(no_result.has_value() == false) {
        // No orders for this district. TODO report when >1%
        if (state.counters.count("no_new_orders") == 0)
            state.counters["no_new_orders"] = benchmark::Counter(1);
        else {
            state.counters["no_new_orders"].value++;
        }
        return true;
    }
    bsoncxx::document::view o_result = *no_result;
    int oId = o_result["o_id"].get_int32();
    int cId = o_result["o_c_id"].get_int32();
    assert(oId >= 1);

#ifdef PRINT_TRACE
    cout << "olq" << endl;
#endif
    int count = 0;
    double total = 0;
    for (auto ol : o_result["o_lines"].get_array().value) {
        count++;
        total += ol["ol_amount"].get_double();
    }
    assert(count > 0);

    #ifdef PRINT_TRACE
    cout << "cuq" << endl;
#endif
    auto colCustomer = conn->database("bench").collection("customer");
    auto cust_update_result = colCustomer.update_one(session, MDV("c_d_id", dparams.dId, "c_w_id", dparams.wId, "c_id", cId), MDV("$set", MDV("c_balance", total)));
    assert(cust_update_result.has_value() == true);
    assert(cust_update_result.value().modified_count() == 1);
    assert(total > 0);

    return true;
}

static bool doDeliveryN(benchmark::State &state, ScaleParameters &params,
                 mongocxx::pool::entry& conn, int &retries,
                 int n = DISTRICTS_PER_WAREHOUSE) {
#ifdef PRINT_TRACE
    cout << "DoDeliveryN" << endl;
#endif
    DeliveryParams dparams;
    randomHelper.generateDeliveryParams(params, dparams);

    int tries = 0;
    mongocxx::client_session::with_transaction_cb callback =
        [&](mongocxx::client_session *session) {
            tries++;
            for (int dId = 1; dId <= n; ++dId) {
                dparams.dId = dId;
                bool result =
                    doDelivery(state, params, dparams, conn, *session);
                if (!result)
                    return false;
            }
            return true;
        };
    auto session = conn->start_session();
    session.with_transaction(callback);
    retries += tries - 1;
    return true;
}

static bool doOrderStatus(benchmark::State &state, ScaleParameters &params,
                   mongocxx::pool::entry& conn, int &retries) {
#ifdef PRINT_TRACE
    cout << "OrderStatus" << endl;
#endif
    OrderStatusParams osparams;
    randomHelper.generateOrderStatusParams(params, osparams);
    int tries = 0;
    mongocxx::client_session::with_transaction_cb callback =
        [&](mongocxx::client_session *session) {
            tries++;
            auto colCustomer = conn->database("bench").collection("customer");

            std::optional<bsoncxx::document::value> customer;
            if (osparams.cId != INT32_MIN) {
#ifdef PRINT_TRACE
                cout << "cqi" << endl;
#endif
                auto findOptions = mongocxx::options::find();
                findOptions.comment("OrderStatusTXNCustById");
                findOptions.projection(MDV("c_id", 1, "c_first", 1, "c_middle",
                                           1, "c_last", 1, "c_balance", 1,
                                           "_id", 0));
                customer = colCustomer.find_one(*session,
                                                MDV("c_id", osparams.cId,
                                                    "c_w_id", osparams.wId,
                                                    "c_d_id", osparams.dId),
                                                findOptions);
            } else {
#ifdef PRINT_TRACE
                cout << "cql" << endl;
#endif
                auto findOptions = mongocxx::options::find();
                findOptions.comment("OrderStatusTXNCustByLastName");
                findOptions.projection(MDV("c_id", 1, "c_first", 1, "c_middle",
                                           1, "c_last", 1, "c_balance", 1,
                                           "_id", 0));
                findOptions.sort(MDV("c_first", 1));
                auto customers =
                    colCustomer.find(*session,
                                     MDV("c_last", osparams.cLast, "c_w_id",
                                         osparams.wId, "c_d_id", osparams.dId),
                                     findOptions);
                std::vector<bsoncxx::document::value> results;
                for (auto &&cust : customers) {
                    results.push_back(MDV2(cust));
                }
                assert(results.size() > 0);
                int index = (results.size() - 1) / 2;
                customer = results[index];
            }
            assert(customer.has_value() == true);
            int cId = (*customer)["c_id"].get_int32();

#ifdef PRINT_TRACE
            cout << "oq" << endl;
#endif
            auto colOrder = conn->database("bench").collection("order");
            auto findOptions = mongocxx::options::find();
            findOptions.comment("OrderStatusTXNOrders");
            findOptions.projection(MDV(
                "o_id", 1, "o_delivery_d", 1, "o_carrier_id", 1, "o_entry_d", 1,
                "o_lines.ol_supply_w_id", 1, "o_lines.ol_i_id", 1,
                "o_lines.ol_quantity", 1, "o_lines.ol_amount", 1, "_id", 0));
            findOptions.sort(MDV("o_id", -1));
            findOptions.limit(1);
            auto theOrders =
                colOrder.find(*session,
                              MDV("o_c_id", cId, "o_w_id", osparams.wId,
                                  "o_d_id", osparams.dId),
                              findOptions);
            std::vector<bsoncxx::document::value> allOrders;
            for (auto &&ord : theOrders) {
                allOrders.push_back(MDV2(ord));
            }
            assert(allOrders.size() == 1);
            auto order = allOrders[0];

            int oId = order["o_id"].get_int32();
            int olCount = order["o_lines"].get_array().value.length();
            assert(olCount > 0);
            // TODO actually return result... customer, order, orderlines
        };
    auto session = conn->start_session();
    session.with_transaction(callback);
    retries += tries - 1;
    return true;
}

static bool doPayment(benchmark::State &state, ScaleParameters &params,
               mongocxx::pool::entry& conn, int &retries) {
#ifdef PRINT_TRACE
    cout << "Payment" << endl;
#endif
    PaymentParams pparams;
    randomHelper.generatePaymentParams(params, pparams);
    int tries = 0;
    mongocxx::client_session::with_transaction_cb callback =
        [&](mongocxx::client_session *session) {
            tries++;
#ifdef PRINT_TRACE
            cout << this_thread::get_id() << " distq" << endl;
#endif
            auto colDistrict = conn->database("bench").collection("district");
            auto updateDistOptions = mongocxx::options::find_one_and_update();
            updateDistOptions.projection(
                MDV("d_name", 1, "d_street_1", 1, "d_street_2", 1, "d_city", 1,
                    "d_state", 1, "d_zip", 1, "_id", 0));
            auto district = colDistrict.find_one_and_update(
                *session, MDV("d_id", pparams.dId, "d_w_id", pparams.wId),
                MDV("$inc", MDV("d_ytd", pparams.hAmount)), updateDistOptions);
            assert(district.has_value() == true);

#ifdef PRINT_TRACE
            cout << this_thread::get_id() << " whq" << endl;
#endif
            auto colWarehouse = conn->database("bench").collection("warehouse");
            auto updateWarehouseOptions =
                mongocxx::options::find_one_and_update();
            updateWarehouseOptions.projection(
                MDV("w_name", 1, "w_street_1", 1, "w_street_2", 1, "w_city", 1,
                    "w_state", 1, "w_zip", 1, "_id", 0));
            auto warehouse = colWarehouse.find_one_and_update(
                *session, MDV("w_id", pparams.wId),
                MDV("$inc", MDV("w_ytd", pparams.hAmount)),
                updateWarehouseOptions);
            assert(warehouse.has_value() == true);

            auto colCustomer = conn->database("bench").collection("customer");
            std::optional<bsoncxx::document::value> customer;
            if (pparams.cId != INT32_MIN) {
#ifdef PRINT_TRACE
                cout << this_thread::get_id() << " cqi" << endl;
#endif
                auto customerOptions = mongocxx::options::find();
                customerOptions.projection(MDV(
                    "c_id", 1, "c_w_id", 1, "c_d_id", 1, "c_delivery_cnt", 1,
                    "c_first", 1, "c_middle", 1, "c_last", 1, "c_street_1", 1,
                    "c_street_2", 1, "c_city", 1, "c_state", 1, "c_zip", 1,
                    "c_phone", 1, "c_credit", 1, "c_credit_lim", 1,
                    "c_discount", 1, "c_data", 1, "c_since", 1, "_id", 0));
                customer = colCustomer.find_one(*session,
                                                MDV("c_id", pparams.cId,
                                                    "c_w_id", pparams.cWId,
                                                    "c_d_id", pparams.cDId),
                                                customerOptions);
            } else {
#ifdef PRINT_TRACE
                cout << this_thread::get_id() << " cql" << endl;
#endif
                auto customerOptions = mongocxx::options::find();
                customerOptions.sort(MDV("c_first", 1));
                customerOptions.projection(MDV(
                    "c_id", 1, "c_w_id", 1, "c_d_id", 1, "c_delivery_cnt", 1,
                    "c_first", 1, "c_middle", 1, "c_last", 1, "c_street_1", 1,
                    "c_street_2", 1, "c_city", 1, "c_state", 1, "c_zip", 1,
                    "c_phone", 1, "c_credit", 1, "c_credit_lim", 1,
                    "c_discount", 1, "c_data", 1, "c_since", 1, "_id", 0));
                auto customers =
                    colCustomer.find(*session,
                                     MDV("c_last", pparams.cLast, "c_w_id",
                                         pparams.cWId, "c_d_id", pparams.cDId),
                                     customerOptions);

                std::vector<bsoncxx::document::value> results;
                for (auto &&cust : customers) {
                    results.push_back(MDV2(cust));
                }
                assert(results.size() > 0);
                int index = (results.size() - 1) / 2;
                customer = results[index];
            }
            assert(customer.has_value() == true);
            int cId = (*customer)["c_id"].get_int32();
            string cData((*customer)["c_data"].get_string());
            string cCredit((*customer)["c_credit"].get_string());
            if (cCredit == BAD_CREDIT) {
                string newData = fmt::format(
                    "{:d} {:d} {:d} {:d} {:d} {:f}", pparams.cId, pparams.cDId,
                    pparams.cWId, pparams.dId, pparams.wId, pparams.hAmount);
                cData = newData + "|" + cData;
                if (cData.length() > MAX_C_DATA) {
                    cData.resize(MAX_C_DATA);
                }
            }

#ifdef PRINT_TRACE
            cout << "cuq" << endl;
#endif
            auto colHistory = conn->database("bench").collection("history");
            auto c_update = colCustomer.update_one(
                *session,
                MDV("c_id", cId, "c_w_id", pparams.cWId, "c_d_id", pparams.cDId),
                MDV("$set", MDV("c_data", cData), "$inc",
                    MDV("c_balance", -pparams.hAmount, "c_ytd_payment",
                        pparams.hAmount, "c_payment_cnt", 1)));
            assert(c_update.has_value() == true);
            assert(c_update.value().modified_count() == 1);

            string h_data =
                fmt::format("{:s}    {:s}", (*warehouse)["w_name"].get_string(),
                            (*district)["d_name"].get_string());

#ifdef PRINT_TRACE
            cout << "hi" << endl;
#endif
            auto insertResult = colHistory.insert_one(
                *session,
                MDV("h_c_id", cId, "h_c_w_id", pparams.cWId, "h_w_id",
                    pparams.wId, "h_c_d_id", pparams.cDId, "h_d_id",
                    pparams.dId, "h_amount", pparams.hAmount, "h_data", h_data,
                    "h_date", bsoncxx::types::b_date{pparams.hDate}, ));
            assert(insertResult.has_value() == true);
            assert(insertResult.value().result().inserted_count() == 1);
        };
    auto session = conn->start_session();
    session.with_transaction(callback);
    retries += tries-1;
    return true;
}

static bool doStockLevel(benchmark::State &state, ScaleParameters &params,
                  mongocxx::pool::entry& conn, int &retries) {
#ifdef PRINT_TRACE
    cout << "stockLevel" << endl;
#endif
    StockLevelParams sparams;
    randomHelper.generateStockLevelParams(params, sparams);
    int tries = 0;
    mongocxx::client_session::with_transaction_cb callback =
        [&](mongocxx::client_session *session) {
            tries++;

#ifdef PRINT_TRACE
            cout << "dq" << endl;
#endif
            auto colDistrict = conn->database("bench").collection("district");
            auto queryOptions = mongocxx::options::find();
            queryOptions.projection(MDV("d_next_o_id", 1, "_id", 0));
            auto district = colDistrict.find_one(
                *session, MDV("d_id", sparams.dId, "d_w_id", sparams.wId));
            assert(district.has_value() == true);
            int nextOid = (*district)["d_next_o_id"].get_int32();

            auto colOrderLine =
                conn->database("bench").collection("order");
            auto orderLinesQuery = mongocxx::options::find();
            orderLinesQuery.projection(MDV("o_lines.ol_i_id", 1, "_id", 0));
            orderLinesQuery.batch_size(1000);
            auto orderLinesResult = colOrderLine.find(
                *session,
                MDV("o_w_id", sparams.wId, "o_d_id", sparams.dId, "o_o_id",
                    MDV("$lt", nextOid, "$gte", nextOid - 20)),
                orderLinesQuery);

            unordered_set<int32_t> ols;
            for (auto &&ol : orderLinesResult) {
                for(auto&& oline : ol["o_lines"].get_array().value) {
                    ols.insert(oline["ol_i_id"].get_int32());
                }
            }
            assert(ols.size() > 0);
            bsoncxx::builder::basic::array builder{};
            for (auto it = ols.begin(); it != ols.end();) {
                builder.append(std::move(ols.extract(it++).value()));
            }

#ifdef PRINT_TRACE
            cout << "sq" << endl;
#endif
            auto colStock = conn->database("bench").collection("stock");
            auto count = colStock.count_documents(
                *session, MDV("s_w_id", sparams.wId, "s_i_id",
                              MDV("$in", builder.extract()), "s_quantity",
                              MDV("$lt", sparams.threshold)));

            // auto colDistrict = conn->database("bench").collection("district");
            // auto pipeline = mongocxx::pipeline{};
            // pipeline.match(MDV("d_w_id", sparams.wId, "d_id", sparams.dId))
            //     .limit(1)
            //     .project(MDV(
            //         "_id", 0, "o_id_min", MDV("$subtract", bsoncxx::builder::array{"$d_next_o_id", 20}), "o_id_max", "$d_next_o_id"))
            //     .lookup(MDV(
            //         "from", "order", "as", "o", "let", MDV("oidmin", "$o_id_min", "oidmax", "$o_id_max"),
            //         "pipeline",
            //         bsoncxx::builder::array{MDV("$match",
            //                 MDV("o_d_id", sparams.dId, "o_w_id", sparams.wId,
            //                     "$expr", MDV("$and", bsoncxx::builder::array{MDV("$lt", bsoncxx::builder::array{"$o_id", "$$oidmax"}), MDV("$gte", bsoncxx::builder::array{"$o_id", "$$oidmin"})}))),
            //             MDV("$project",
            //                 MDV("_id", 0, "i_ids", "$o_lines.ol_i_id"))}))
            //     .unwind("$o")
            //     .unwind("$o.i_ids")
            //     .lookup(
            //         MDV("from", "stock", "as", "o", "let",
            //             MDV("ids", "$o.i_ids"), "pipeline",
            //                 bsoncxx::builder::array{MDV("$match",
            //                         MDV("s_w_id", sparams.wId, "s_quantity",
            //                             MDV("$lt", sparams.threshold), "$expr",
            //                             MDV("$eq", bsoncxx::builder::array{"$s_i_id", "$$ids"}))),
            //                     MDV("$project", MDV("s_w_id", 1))}))
            //     .unwind("$o")
            //     .count("c");
            // //cout << bsoncxx::to_json(pipeline.view_array()) << endl;
            // auto results = colDistrict.aggregate(*session, pipeline);
            // std::vector<int> counts;
            // for(auto&& count: results) {
            //     counts.push_back(count["c"].get_int32());
            // }
            // if(counts.size() == 1)
            //     auto volatile count = counts[0];
        };
    auto session = conn->start_session();
    session.with_transaction(callback);
    retries += tries-1;

    return true;
}

static bool doNewOrder(benchmark::State &state, ScaleParameters &params,
                mongocxx::pool::entry& conn, int &numFails, int &retries) {
#ifdef PRINT_TRACE
    cout << "newOrder" << endl;
#endif
    NewOrderParams noparams;
    randomHelper.generateNewOrderParams(params, noparams);
    int tries = 0;
    mongocxx::client_session::with_transaction_cb callback =
        [&](mongocxx::client_session *session) {
            tries++;
#ifdef PRINT_TRACE
            cout << "du" << endl;
#endif
            auto colDistrict = conn->database("bench").collection("district");
            auto optionsDist = mongocxx::options::find_one_and_update();
            optionsDist.projection(MDV("d_id", 1, "d_w_id", 1, "d_tax", 1,
                                       "d_next_o_id", 1, "_id", 0));
            auto district = colDistrict.find_one_and_update(
                *session, MDV("d_id", noparams.dId, "d_w_id", noparams.wId),
                MDV("$inc", MDV("d_next_o_id", 1)), optionsDist);
            assert(district.has_value() == true);

            double dTax = (*district)["d_tax"].get_double();
            int dNextOId = (*district)["d_next_o_id"].get_int32();

        // TODO sharding?
#ifdef PRINT_TRACE
            cout << "iq" << endl;
#endif
            auto colItem = conn->database("bench").collection("item");
            auto optionsItem = mongocxx::options::find();
            optionsItem.projection(MDV("i_id", 1, "i_price", 1, "i_name", 1,
                                       "i_data", 1, "_id", 0));
            bsoncxx::builder::basic::array iids{};
            for (auto iId : noparams.iIds) {
                iids.append(iId);
            }
            auto itemsResults = colItem.find(
                *session, MDV("i_id", MDV("$in", iids.view())), optionsItem);
            std::vector<bsoncxx::document::value> items;
            for (auto &&item : itemsResults) {
                items.push_back(MDV2(item));
            }
            if (items.size() != noparams.iIds.size()) {
                numFails++;
                session->abort_transaction();
                return false;
            }
            // Get id index
            auto getiIdIndex = [&](int iid) {
                int index =
                    find(noparams.iIds.begin(), noparams.iIds.end(), iid) -
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
                                [=](bsoncxx::document::value &row) {
                                    return row["i_id"].get_int32() == iid;
                                });
            };

#ifdef PRINT_TRACE
            cout << "whq" << endl;
#endif
            auto colWarehouse = conn->database("bench").collection("warehouse");
            auto warehouseQuery = mongocxx::options::find();
            warehouseQuery.projection(MDV("w_tax", 1, "_id", 0));
            auto warehouse =
                colWarehouse.find_one(*session, MDV("w_id", noparams.wId));
            assert(warehouse.has_value() == true);
            double wTax = (*warehouse)["w_tax"].get_double();

#ifdef PRINT_TRACE
            cout << "cq" << endl;
#endif
            auto colCustomer = conn->database("bench").collection("customer");
            auto customerQuery = mongocxx::options::find();
            customerQuery.projection(
                MDV("c_discount", 1, "c_last", 1, "c_credit", 1, "_id", 0));
            auto customer =
                colCustomer.find_one(*session,
                                     MDV("c_w_id", noparams.wId, "c_d_id",
                                         noparams.dId, "c_id", noparams.cId),
                                     customerQuery);
            assert(customer.has_value() == true);
            double cDiscount = (*customer)["c_discount"].get_double();

            int olCnt = noparams.iIds.size();
            int oCarrierId = NULL_CARRIER_ID;

            // All from same warehouse...
            bool allLocal =
                all_of(noparams.iIWds.begin(), noparams.iIWds.end(),
                       [=](int i) { return i == noparams.iIWds[0]; });

            auto colStock = conn->database("bench").collection("stock");
            std::vector<bsoncxx::document::value> stock;
            if (allLocal) {
#ifdef PRINT_TRACE
                cout << "sal" << endl;
#endif
                auto stockQuery = mongocxx::options::find();
                stockQuery.projection(MDV(
                    "s_i_id", 1, "s_w_id", 1, "s_quantity", 1, "s_data", 1,
                    "s_ytd", 1, "s_order_cnt", 1, "s_remote_cnt", 1,
                    fmt::format("s_dist_{:02d}", noparams.dId), 1, "_id", 0));
                auto stockResult =
                    colStock.find(*session,
                                  MDV("s_w_id", noparams.wId, "s_i_id",
                                      MDV("$in", iids.view())),
                                  stockQuery);
                for (auto &&stck : stockResult) {
                    stock.push_back(MDV2(stck));
                }
                assert(stock.size() == olCnt);
            } else {
                bsoncxx::builder::basic::array filters{};
                for (int i = 0; i < noparams.iIds.size(); ++i) {
                    filters.append(MDV("s_w_id", getwId(noparams.iIds[i]),
                                       "s_i_id", noparams.iIds[i]));
                }

#ifdef PRINT_TRACE
                cout << "sor" << endl;
#endif
                auto stockQuery = mongocxx::options::find();
                stockQuery.projection(MDV(
                    "s_i_id", 1, "s_w_id", 1, "s_quantity", 1, "s_data", 1,
                    "s_ytd", 1, "s_order_cnt", 1, "s_remote_cnt", 1,
                    fmt::format("s_dist_{:02d}", noparams.dId), 1, "_id", 0));
                auto stockResult = colStock.find(
                    *session, MDV("$or", filters.view()), stockQuery);
                for (auto &&stck : stockResult) {
                    stock.push_back(MDV2(stck));
                }
                assert(stock.size() == olCnt);
            }

            auto getStock = [&](int iid) {
                return *find_if(stock.begin(), stock.end(),
                                [=](bsoncxx::document::value &row) {
                                    return row["s_i_id"].get_int32() == iid;
                                });
            };


            vector<tuple<string, int, string, double, double>> itemData;
            itemData.reserve(olCnt);
            double total = 0;
            auto stockBulk = colStock.create_bulk_write(*session);
            auto newOrderBulk = bsoncxx::builder::basic::array{};
            for (int i = 0; i < olCnt; ++i) {
                int olNumber = i + 1;
                int olIId = noparams.iIds[i];
                int iIdIdx = getiIdIndex(olIId);
                int olSupplyWId = noparams.iIWds[i];
                int olQuantity = noparams.iQtys[i];

                auto item = getItem(olIId);
                auto stockitem = getStock(olIId);

                int sQuantity = stockitem["s_ytd"].get_int32();
                int sYtd = stockitem["s_ytd"].get_int32() + olQuantity;

                if (sQuantity >= olQuantity + 10) {
                    sQuantity = sQuantity - olQuantity;
                } else {
                    sQuantity = sQuantity + 91 - olQuantity;
                }

                int sOrderCnt = stockitem["s_order_cnt"].get_int32() + 1;
                int sRemoteCnt = stockitem["s_remote_cnt"].get_int32();

                if (olSupplyWId != noparams.wId) {
                    sRemoteCnt++;
                }

#ifdef PRINT_TRACE
                cout << "suq" << endl;
#endif
                mongocxx::model::update_one updater(
                    MDV("s_i_id", olIId, "s_w_id", olSupplyWId),
                    MDV("$set", MDV("s_quantity", sQuantity, "s_ytd", sYtd,
                                    "s_order_cnt", sOrderCnt, "s_remote_cnt",
                                    sRemoteCnt, )));
                stockBulk.append(updater);

                double olAmount = olQuantity * item["i_price"].get_double();
                total += olAmount;

#ifdef PRINT_TRACE
                cout << "iol" << endl;
#endif
                auto ol = MDV("ol_o_id", dNextOId, "ol_w_id", noparams.wId, "ol_d_id",
                        noparams.dId, "ol_number", olNumber, "ol_i_id", olIId,
                        "ol_supply_w_id", olSupplyWId, "ol_quantity",
                        olQuantity, "ol_amount", olAmount, "ol_dist_info",
                        stockitem[fmt::format("s_dist_{:02d}", noparams.dId)]
                            .get_string(),
                        );
                newOrderBulk.append(bsoncxx::types::b_document{ol});

                string iData(item["i_data"].get_string());
                string sData(stockitem["s_data"].get_string());
                string brandGeneric = "G";
                if (iData.find(ORIGINAL_STRING) != -1 &&
                    sData.find(ORIGINAL_STRING) != -1) {
                    brandGeneric = "B";
                }
                itemData.push_back(make_tuple(
                    string(item["i_name"].get_string()), sQuantity,
                    brandGeneric, item["i_price"].get_double(), olAmount));
            }
#ifdef PRINT_TRACE
            cout << "io" << endl;
#endif
            auto colOrder = conn->database("bench").collection("order");
            auto iOResult = colOrder.insert_one(
                *session,
                MDV("o_id", dNextOId, "o_w_id", noparams.wId, "o_d_id",
                    noparams.dId, "o_c_id", noparams.cId, "o_carrier_id",
                    oCarrierId, "o_ol_cnt", olCnt, "o_all_local", allLocal,
                    "o_new", true, "o_entry_d",
                    bsoncxx::types::b_date{noparams.oEntryDate}, "o_lines",
                    newOrderBulk.extract(), ));
            assert(iOResult.has_value() == true);
            assert(iOResult.value().result().inserted_count() == 1);
            auto stockResult = stockBulk.execute();
            assert(stockResult.has_value() == true);
            assert(stockResult.value().matched_count() == olCnt);
            total *= (1 - cDiscount) * (1 + wTax + dTax);
            return true;
        };
    auto session = conn->start_session();
    session.with_transaction(callback);
    retries += tries -1;
    return true;
}

static ScaleParameters params = ScaleParameters::makeDefault(4);
static void BM_MONGO_TPCC_MODERN(benchmark::State &state) {
	auto conn = MongoDBHandler::GetConnection();
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
    int retries = 0;
    for (auto _ : state) {
        // auto start = std::chrono::high_resolution_clock::now();
        tpcc::TransactionType type = randomHelper.nextTransactionType();
        // Start transaction

        try {
        bool result = false;
        switch (type) {
        case tpcc::TransactionType::Delivery:
            result = doDeliveryN(state, params, conn, retries);
            numDeliveries++;
            break;
        case tpcc::TransactionType::OrderStatus:
            result = doOrderStatus(state, params, conn, retries);
            numOrderStatuses++;
            break;
        case tpcc::TransactionType::Payment:
            result = doPayment(state, params, conn, retries);
            numPayments++;
            break;
        case tpcc::TransactionType::StockLevel:
            result = doStockLevel(state, params, conn, retries);
            numStockLevels++;
            break;
        case tpcc::TransactionType::NewOrder:
            result = doNewOrder(state, params, conn, numFailedNewOrders, retries);
            numNewOrders++;
            break;
        }
        } catch(mongocxx::operation_exception& e) {
            if(e.has_error_label("TransientTransactionError")) {
                cout << this_thread::get_id() << " Operation exception! Transient \r\n" << e.what() << endl;
            } else if(e.has_error_label("UnknownTransactionCommitResult")) {
                cout << this_thread::get_id() << " Operation exception! Unknown \r\n" << e.what() << endl;
            }
        } catch(mongocxx::exception& e) {
            cout << this_thread::get_id() << " mongocxxException!\r\n" << e.what() << endl;
        } catch(std::exception& e) {
            cout << this_thread::get_id() << " std exception!\r\n" << e.what() << endl;
        } catch(...) {
            cout << this_thread::get_id() << " Unknown exception!\r\n" << endl;
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

    state.counters["retries"] = retries;

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

BENCHMARK(BM_MONGO_TPCC_MODERN)->RangeMultiplier(2)->Range(1,10)->Iterations(10000)->ThreadRange(1,16)->UseRealTime();
