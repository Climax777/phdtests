#include "dbphd/tpc/tpchelpers.hpp"
#include <algorithm>
#include <cassert>
#include <cstring>
#include <iostream>
#include <random>

using namespace std;

namespace tpcc {
bool NuRandC::isValid(int cRun, int cLoad) {
    int cDelta = abs(cRun - cLoad);
    return 65 <= cDelta && cDelta <= 119 && cDelta != 96 && cDelta != 112;
}
thread_local random_device RandomHelper::rd;
thread_local mt19937 RandomHelper::gen(rd()); // "static" does not appear here

NuRandC NuRandC::createRandom() {
    return NuRandC(randomHelper.number(0, 255), randomHelper.number(0, 1023),
                   randomHelper.number(0, 8191));
}

NuRandC NuRandC::createRandomForRun(const NuRandC &cLoad) {
    NuRandC cTest = createRandom();

    while (!isValid(cTest.cLast, cLoad.cLast)) {
        cTest.cLast = randomHelper.number(0, 255);
    }
    return cTest;
}

int RandomHelper::number(int l, int u) {
    uniform_int_distribution<> dist(l, u);
    return dist(gen); 
}

int RandomHelper::numberExcluding(int l, int u, int excluding) {
    assert(l < u);
    assert(l <= excluding && excluding <= u);

    int num = number(l, u - 1);
    if (num >= excluding) {
        num += 1;
    }
    assert(l <= num && num <= u && num != excluding);
    return num;
}

string RandomHelper::alphaString(int lower, int upper) {
    return generateString(lower, upper, 'a', 26);
}

string RandomHelper::numString(int lower, int upper) {
    return generateString(lower, upper, '0', 10);
}

string RandomHelper::generateString(int lower, int upper, char base, int num) {
    int len = number(lower, upper);
    string result;
    result.resize(len);
    for (int i = 0; i < len; ++i) {
        result[i] = static_cast<char>(base + number(0, num - 1));
    }

    return result;
}

int RandomHelper::NuRand(int A, int x, int y) {
    assert(x <= y);
    if (cValues.isUninitialized()) {
        setCValues(NuRandC::createRandom());
    }
    int C = 0;
    switch (A) {
    case 255:
        C = cValues.cLast;
        break;
    case 1023:
        C = cValues.cId;
        break;
    case 8191:
        C = cValues.orderLineItemID;
        break;
    default:
        cerr << "NuRand: A = " << A << " invalid " << endl;
        exit(1);
    }
    return (((number(0, A) | number(x, y)) + C) % (y - x + 1)) + x;
}

string RandomHelper::randomLastName(int maxcid) {
    return lastName(NuRand(255, 0, std::min(999, maxcid - 1)));
}
string RandomHelper::lastName(int num) {
    assert(num >= 0 && num <= 999);
    static const char *const SYLLABLES[] = {
        "BAR", "OUGHT", "ABLE",  "PRI",   "PRES",
        "ESE", "ANTI",  "CALLY", "ATION", "EING",
    };
    static const int LENGTHS[] = {
        3, 5, 4, 3, 4, 3, 4, 5, 5, 4,
    };

    string name;
    int indicies[] = {num / 100, (num / 10) % 10, num % 10};

    for (int i = 0; i < sizeof(indicies) / sizeof(*indicies); ++i) {
        std::copy(SYLLABLES[indicies[i]],
                  SYLLABLES[indicies[i]] +
                      static_cast<size_t>(LENGTHS[indicies[i]]),
                  back_inserter(name));
    }
    return name;
}

void RandomHelper::seed(int s) { gen.seed(s); }

vector<int> RandomHelper::uniqueIds(int num, int min, int max) {
    vector<int> results;
    // TODO asserts...
    assert(max - min + 1 >=
           num); // Number of unique values has to fit within the range
    results.reserve(num);
    for (int i = 0; i < num; ++i) {
        int val = INT32_MIN;
        while (val == INT32_MIN ||
               find(results.begin(), results.end(), val) != results.end()) {
            val = number(min, max);
        }
        results.push_back(val);
    }

    assert(results.size() == num);
    return results;
}
TransactionType RandomHelper::nextTransactionType() {
    // TODO consider the shuffled deck generator
    discrete_distribution<> dist({45, 43, 4, 4, 4});
    return (TransactionType)dist(gen);
}

ScaleParameters ScaleParameters::makeDefault(int Warehouses) {
    return ScaleParameters(NUM_ITEMS, Warehouses, DISTRICTS_PER_WAREHOUSE,
                           CUSTOMERS_PER_DISTRICT,
                           INITIAL_NEW_ORDERS_PER_DISTRICT);
}

ScaleParameters ScaleParameters::makeScaled(int Warehouses,
                                            double scaleFactor) {
    assert(scaleFactor >= 1.0);
    int items = (int)NUM_ITEMS / scaleFactor;
    if (items <= 0)
        items = 1;
    int districts = (int)max(DISTRICTS_PER_WAREHOUSE, 1);
    int customers = (int)max(CUSTOMERS_PER_DISTRICT / scaleFactor, 1.0);
    int newOrders =
        (int)max(INITIAL_NEW_ORDERS_PER_DISTRICT / scaleFactor, 0.0);
    return ScaleParameters(items, Warehouses, districts, customers, newOrders);
}

int RandomHelper::makeWarehouseId(const ScaleParameters &params) {
    int wId = number(params.startingWarehouse, params.endingWarehouse);
    assert(wId >= params.startingWarehouse);
    assert(wId <= params.endingWarehouse);
    return wId;
}
int RandomHelper::makeDistrictId(const ScaleParameters &params) {
    return number(1, params.districtsPerWarehouse);
}
int RandomHelper::makeCustomerId(const ScaleParameters &params) {
    return NuRand(1023, 1, params.customersPerDistrict);
}
int RandomHelper::makeItemId(const ScaleParameters &params) {
    return NuRand(8191, 1, params.items);
}

void RandomHelper::generateDeliveryParams(const ScaleParameters &params,
                                          DeliveryParams &out) {
    out.wId = makeWarehouseId(params);
    out.oCarrierId = number(MIN_CARRIER_ID, MAX_CARRIER_ID);
    out.olDeliveryD = chrono::system_clock::now();
}
void RandomHelper::generateNewOrderParams(const ScaleParameters &params,
                                          NewOrderParams &out) {
    out.wId = makeWarehouseId(params);
    out.dId = makeDistrictId(params);
    out.cId = makeCustomerId(params);
    int olCnt = number(MIN_OL_CNT, MAX_OL_CNT);
    out.oEntryDate = chrono::system_clock::now();

    bool rollback = number(1, 100) == 1;
    out.iIds.clear();
    out.iIWds.clear();
    out.iQtys.clear();
    out.iIds.reserve(olCnt);
    out.iIWds.reserve(olCnt);
    out.iQtys.reserve(olCnt);

    for (int i = 0; i < olCnt; ++i) {
        if (rollback && (i + 1 == olCnt)) {
            out.iIds.push_back(params.items + 1);
        } else {
            int iId = makeItemId(params);
            while (find(out.iIds.begin(), out.iIds.end(), iId) !=
                   out.iIds.end()) {
                iId = makeItemId(params);
            }
            out.iIds.push_back(iId);
        }

        bool remote = (number(1, 100) == 1);
        if (params.warehouses > 1 && remote) {
            out.iIWds.push_back(numberExcluding(
                params.startingWarehouse, params.endingWarehouse, out.wId));
        } else {
            out.iIWds.push_back(out.wId);
        }
        out.iQtys.push_back(number(1, MAX_OL_QUANTITY));
    }
}
void RandomHelper::generateOrderStatusParams(const ScaleParameters &params,
                                             OrderStatusParams &out) {
    out.wId = makeWarehouseId(params);
    out.dId = makeDistrictId(params);
    out.cLast.clear();
    out.cId = INT32_MIN;
    if (number(1, 100) <= 60) {
        out.cLast = randomLastName(params.customersPerDistrict);
    } else {
        out.cId = makeCustomerId(params);
    }
}
void RandomHelper::generatePaymentParams(const ScaleParameters &params,
                                         PaymentParams &out) {
    int x = number(1, 100);
    int y = number(1, 100);
    out.wId = makeWarehouseId(params);
    out.dId = makeDistrictId(params);
    out.cWId = INT32_MIN;
    out.cDId = INT32_MIN;
    out.cId = INT32_MIN;
    out.hAmount = fixedPoint(2, MAX_PAYMENT, MIN_PAYMENT);
    out.hDate = chrono::system_clock::now();
    out.cLast.clear();

    if (params.warehouses == 1 || x <= 85) {
        out.cWId = out.wId;
        out.cDId = out.dId;
    } else {
        out.cWId = numberExcluding(params.startingWarehouse,
                                   params.endingWarehouse, out.wId);
        assert(out.cWId != out.wId);
        out.cDId = makeDistrictId(params);
    }

    if (y <= 60) {
        out.cLast = randomLastName(params.customersPerDistrict);
    } else {
        out.cId = makeCustomerId(params);
    }
}
void RandomHelper::generateStockLevelParams(const ScaleParameters &params,
                                            StockLevelParams &out) {
    out.wId = makeWarehouseId(params);
    out.dId = makeDistrictId(params);
    out.threshold =
        number(MIN_STOCK_LEVEL_THRESHOLD, MAX_STOCK_LEVEL_THRESHOLD);
}

void RandomHelper::generateAddress(StreetAddress &out) {
    out.street1 = alphaString(MIN_STREET, MAX_STREET);
    out.street2 = alphaString(MIN_STREET, MAX_STREET);
    out.city = alphaString(MIN_CITY, MAX_CITY);
    out.state = alphaString(STATE, STATE);
    out.zip = generateZip();
}
double RandomHelper::generateTax() {
    return fixedPoint(TAX_DECIMALS, MAX_TAX, MIN_TAX);
}
std::string RandomHelper::generateZip() {
    int len = ZIP_LENGTH - strlen(ZIP_SUFFIX);
    return numString(len, len) + ZIP_SUFFIX;
}
void RandomHelper::fillOriginal(std::string &data) {
    int origlen = strlen(ORIGINAL_STRING);
    int position = number(0, data.length() - origlen);
    data.replace(position, origlen, string_view(ORIGINAL_STRING));
}

void RandomHelper::generateItem(int id, bool original, Item &out) {
    out.iId = id;
    out.iImId = number(MIN_IM, MAX_IM);
    out.iName = alphaString(MIN_I_NAME, MAX_I_NAME);
    out.iPrice = fixedPoint(MONEY_DECIMALS, MAX_PRICE, MIN_PRICE);
    out.iData = alphaString(MIN_I_DATA, MAX_I_DATA);
    if (original)
        fillOriginal(out.iData);
}
void RandomHelper::generateWarehouse(int wid, Warehouse &out) {
    out.wId = wid;
    out.wTax = generateTax();
    out.wYtd = INITIAL_W_YTD;
    out.wName = alphaString(MIN_NAME, MAX_NAME);
    generateAddress(out.wAddress);
}
void RandomHelper::generateDistrict(int did, int wid, int nextOId,
                                    District &out) {
    out.dId = did;
    out.dWId = wid;
    out.dNextOId = nextOId;
    out.dName = alphaString(MIN_NAME, MAX_NAME);
    generateAddress(out.dAddress);
    out.dTax = generateTax();
    out.dYtd = INITIAL_D_YTD;
}
void RandomHelper::generateCustomer(int cwid, int cdid, int cid, bool badCredit,
                                    Customer &out) {
    out.cFirst = alphaString(MIN_FIRST, MAX_FIRST);
    out.cMiddle = MIDDLE;
    assert(cid >= 1 && cid <= CUSTOMERS_PER_DISTRICT);
    out.cId = cid;
    out.cWId = cwid;
    out.cDId = cdid;
    if (cid <= 1000)
        out.cLast = lastName(cid - 1);
    else
        out.cLast = randomLastName(CUSTOMERS_PER_DISTRICT);

    out.cPhone = numString(PHONE, PHONE);
    out.cSince = chrono::system_clock::now();
    out.cCredit = badCredit ? BAD_CREDIT : GOOD_CREDIT;
    out.cCreditLimit = INITIAL_CREDIT_LIM;
    out.cDiscount = fixedPoint(DISCOUNT_DECIMALS, MAX_DISCOUNT, MIN_DISCOUNT);
    out.cBalance = INITIAL_BALANCE;
    out.cYtdPayment = INITIAL_YTD_PAYMENT;
    out.cPaymentCnt = INITIAL_PAYMENT_CNT;
    out.cDeliveryCnt = INITIAL_DELIVERY_CNT;
    out.cData = alphaString(MIN_C_DATA, MAX_C_DATA);
    generateAddress(out.cAddress);
}
void RandomHelper::generateOrder(int owid, int odid, int oid, int ocid,
                                 int oilcnt, bool newOrder, Order &out) {
    out.oEntryD = chrono::system_clock::now();
    out.oCarrierId =
        newOrder ? NULL_CARRIER_ID : number(MIN_CARRIER_ID, MAX_CARRIER_ID);
    out.oAllLocal = INITIAL_ALL_LOCAL;
    out.oId = oid;
    out.oCId = ocid;
    out.oDId = odid;
    out.oWId = owid;
    out.oOlCnt = oilcnt;
}
void RandomHelper::generateOrderLine(const ScaleParameters &params, int olwid,
                                     int oldid, int oloid, int olnumber,
                                     int maxitems, bool newOrder,
                                     OrderLine &out) {
    out.olIId = number(1, maxitems);
    out.olSupplyWId = olwid;
    out.olDeliveryD = newOrder ? chrono::system_clock::time_point(0s)
                               : chrono::system_clock::now();
    out.olQuantity = INITIAL_QUANTITY;

    bool remote = number(1, 100) == 1;
    if (params.warehouses > 0 && remote) {
        out.olSupplyWId = numberExcluding(params.startingWarehouse,
                                          params.endingWarehouse, olwid);
    }

    out.olAmount =
        fixedPoint(MONEY_DECIMALS, MAX_PRICE * MAX_OL_QUANTITY, MIN_AMOUNT);
    out.olDistInfo = alphaString(DIST, DIST);
    out.olOId = oloid;
    out.olDId = oldid;
    out.olWId = olwid;
    out.olNumber = olnumber;
}
void RandomHelper::generateStock(int swid, int siid, bool original,
                                 Stock &out) {
    out.sIId = siid;
    out.sWId = swid;
    out.sQuantity = number(MIN_QUANTITY, MAX_QUANTITY);
    out.sYtd = 0;
    out.sOrderCnt = 0;
    out.sRemoteCnt = 0;
    out.sData = alphaString(MIN_I_DATA, MAX_I_DATA);
    if (original)
        fillOriginal(out.sData);

    for (int i = 0; i < DISTRICTS_PER_WAREHOUSE; ++i) {
        out.sDists[i] = alphaString(DIST, DIST);
    }
}
void RandomHelper::generateHistory(int hcwid, int hcdid, int hcid,
                                   History &out) {
    out.hCWId = hcwid;
    out.hCDId = hcdid;
    out.hCId = hcid;
    out.hWId = hcwid;
    out.hDId = hcdid;
    out.hDate = chrono::system_clock::now();
    out.hAmount = INITIAL_AMOUNT;
    out.hData = alphaString(MIN_DATA, MAX_DATA);
}
} // namespace tpcc