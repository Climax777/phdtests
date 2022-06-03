#if !defined(TPCHELPERS)
#define TPCHELPERS
#include <random>
#include <string>
#include <sstream>
#include <vector>
#include <cassert>
#include <chrono>
#include <array>
#include <algorithm>
#include <fmt/core.h>
#include <fmt/chrono.h>

namespace tpcc {
enum class TransactionType {
    NewOrder,
    Payment,
    OrderStatus,
    Delivery,
    StockLevel
};

class Random;

struct NuRandC {
    NuRandC() : cLast(INT32_MIN), cId(INT32_MIN), orderLineItemID(INT32_MIN) {
    }
  NuRandC(int cLast, int cId, int orderLineItemID)
      : cLast(cLast), cId(cId), orderLineItemID(orderLineItemID) {}
  int cLast;
  int cId;
  int orderLineItemID;

void reset() {
    cLast = cId = orderLineItemID = INT32_MIN;
}
    bool isUninitialized() {
        return cLast == INT32_MIN || cId == INT32_MIN || orderLineItemID == INT32_MIN;
    }

    static bool isValid(int cRun, int cLoad);
  static NuRandC createRandom();
  static NuRandC createRandomForRun(const NuRandC &cLoad);
};

class ScaleParameters;

// Constants
static const int MONEY_DECIMALS = 2;

// Item constants
static const int NUM_ITEMS = 100000;
static const int MIN_IM = 1;
static const int MAX_IM = 10000;
static const double MIN_PRICE = 1.00;
static const double MAX_PRICE = 100.00;
static const int MIN_I_NAME = 14;
static const int MAX_I_NAME = 24;
static const int MIN_I_DATA = 26;
static const int MAX_I_DATA = 50;

// Warehouse constants
static const double MIN_TAX = 0.0;
static const double MAX_TAX = 0.2;
static const int TAX_DECIMALS = 2;
static const double INITIAL_W_YTD = 300000.00;
static const int MIN_NAME = 6;
static const int MAX_NAME = 10;
static const int MIN_STREET = 10;
static const int MAX_STREET = 20;
static const int MIN_CITY = 10;
static const int MAX_CITY = 20;
static const int STATE = 2;
static const int ZIP_LENGTH = 9;
static const char* ZIP_SUFFIX = "11111";

// Stock constants
static const int MIN_QUANTITY = 10;
static const int MAX_QUANTITY = 100;
static const int DIST = 24;
static const int STOCK_PER_WAREHOUSE = 100000;

// District constants
static const int DISTRICTS_PER_WAREHOUSE = 10;
static const double INITIAL_D_YTD = 30000.0;
static const int INITIAL_NEXT_O_ID = 3001;

// Customer constants
static const int CUSTOMERS_PER_DISTRICT = 3000;
static const double INITIAL_CREDIT_LIM = 50000.00;
static const double MIN_DISCOUNT = 0.0;
static const double MAX_DISCOUNT = 0.5;
static const int DISCOUNT_DECIMALS = 4;
static const double INITIAL_BALANCE = -10.00;
static const double INITIAL_YTD_PAYMENT = 10.00;
static const int INITIAL_PAYMENT_CNT = 1;
static const int INITIAL_DELIVERY_CNT = 0;
static const int MIN_FIRST = 6;
static const int MAX_FIRST = 10;
static const char* MIDDLE = "OE";
static const int PHONE = 16;
static const int MIN_C_DATA = 300;
static const int MAX_C_DATA = 500;
static const char* GOOD_CREDIT = "GC";
static const char* BAD_CREDIT = "BC";

// Order constants
static const int MIN_CARRIER_ID = 1;
static const int MAX_CARRIER_ID = 10;
static const int NULL_CARRIER_ID = 0;
static const int NULL_CARRIER_LOWER_BOUND = 2101;
static const int MIN_OL_CNT = 5;
static const int MAX_OL_CNT = 15;
static const int INITIAL_ALL_LOCAL = 1;
static const int INITIAL_ORDERS_PER_DISTRICT = 3000;
// For new order
static const int MAX_OL_QUANTITY = 10;

// Order line constants
static const int INITIAL_QUANTITY = 5;
static const double MIN_AMOUNT = 0.01;

// History constants
static const int MIN_DATA = 12;
static const int MAX_DATA = 24;
static const double INITIAL_AMOUNT = 10.00;

// New order constants
static const int INITIAL_NEW_ORDERS_PER_DISTRICT = 900;

// TPC-C 2.4.3.4
static const char* INVALID_ITEM_MESSAGE = "Item number is not valid";

// Stock level transactions
static const int MIN_STOCK_LEVEL_THRESHOLD = 10;
static const int MAX_STOCK_LEVEL_THRESHOLD = 20;

// Used to generate payment transactions
static const double MIN_PAYMENT = 1.0;
static const double MAX_PAYMENT = 5000.0;

// Indicates "brand" items and stock in i_data and s_data
static const char* ORIGINAL_STRING = "ORIGINAL";

struct StockLevelParams {
    int wId;
    int dId;
    int threshold;
};
struct PaymentParams {
    int wId;
    int dId;
    double hAmount;
    int cWId;
    int cDId;
    int cId;
    std::string cLast;
    std::chrono::time_point<std::chrono::system_clock> hDate;
};

struct OrderStatusParams {
    int wId;
    int dId;
    int cId;
    std::string cLast;
};

struct NewOrderParams {
    int wId;
    int dId;
    int cId;
    std::chrono::time_point<std::chrono::system_clock> oEntryDate;
    std::vector<int> iIds;
    std::vector<int> iIWds;
    std::vector<int> iQtys;
};


struct DeliveryParams {
    int wId;
    int oCarrierId;
    std::chrono::time_point<std::chrono::system_clock> olDeliveryD;
};

struct NewOrder {
    int oId;
    int wId;
    int dId;
};

struct Item {
    int iId;
    int iImId;
    std::string iName;
    double iPrice;
    std::string iData;

    friend auto operator<<(std::ostream &os, Item const &m)
        -> std::ostream & {
        return os << "Item: " << m.iId << " " << m.iName << " " << m.iPrice << " " << m.iImId << " " << m.iData << "\r\n";
    }
};

struct StreetAddress {
    std::string street1;
    std::string street2;
    std::string city;
    std::string state;
    std::string zip;

    friend auto operator<<(std::ostream &os, StreetAddress const &m)
        -> std::ostream & {
        return os << m.street1 << " " << m.street2 << " " << m.city << " " << m.state << " " << m.zip;
    }
};

struct Warehouse {
    int wId;
    double wTax;
    double wYtd;
    std::string wName;
    StreetAddress wAddress;

    friend auto operator<<(std::ostream &os, Warehouse const &m)
        -> std::ostream & {
        return os << "Warehouse: " << m.wId << " " << m.wTax << " " << m.wYtd << " " << m.wName << " " << m.wAddress << "\r\n";
    }
};

struct District {
    int dId;
    int dWId;
    double dTax;
    double dYtd;
    std::string dName;
    StreetAddress dAddress;
    int dNextOId;

    friend auto operator<<(std::ostream &os, District const &m)
        -> std::ostream & {
        return os << "District: " << m.dId << " " << m.dWId << " " << m.dTax << " " << m.dYtd << " " << m.dName << " " << m.dNextOId << " " << m.dAddress << "\r\n";
    }
};

struct Customer {
    int cId;
    int cWId;
    int cDId;
    std::string cFirst;
    std::string cMiddle;
    std::string cLast;
    std::string cPhone;
    StreetAddress cAddress;
    std::chrono::time_point<std::chrono::system_clock> cSince;
    std::string cCredit;
    double cCreditLimit;
    double cDiscount;
    double cBalance;
    double cYtdPayment;
    int cPaymentCnt;
    int cDeliveryCnt;
    std::string cData;

    friend auto operator<<(std::ostream &os, Customer const &m)
        -> std::ostream & {
        return os << "Customer: " << m.cId << " " << m.cWId << " " << m.cDId << " " 
            << m.cFirst << " " << m.cMiddle << " " << m.cLast << " " << m.cPhone << " " 
            << m.cAddress << " " << m.cSince.time_since_epoch().count() << " " << m.cCredit << " " << m.cCreditLimit 
            << " " << m.cDiscount << " " << m.cBalance << " " << m.cYtdPayment << " " << m.cPaymentCnt 
            << " " << m.cDeliveryCnt << " " << m.cData
            << "\r\n";
    }
};

struct Order {
    int oId;
    int oWId;
    int oDId;
    int oCId;
    int oOlCnt;
    std::chrono::time_point<std::chrono::system_clock> oEntryD;
    int oCarrierId;
    bool oAllLocal;

    friend auto operator<<(std::ostream &os, Order const &m)
        -> std::ostream & {
        return os << "Order: " << m.oId << " " << m.oWId << " " << m.oDId 
        << " " << m.oCId << " " << m.oOlCnt << " " << m.oEntryD.time_since_epoch().count()
        << " " << m.oCarrierId << " " << m.oAllLocal
        << "\r\n";
    }
};

struct OrderLine {
    int olOId;
    int olNumber;
    int olWId;
    int olDId;
    int olIId;
    int olSupplyWId;
    std::chrono::time_point<std::chrono::system_clock> olDeliveryD;
    int olQuantity;
    double olAmount;
    std::string olDistInfo;

    friend auto operator<<(std::ostream &os, OrderLine const &m)
        -> std::ostream & {
        return os << "OrderLine: " << m.olIId << " " << m.olNumber << " " << m.olWId << " " << m.olDId
        << " " << m.olIId << " " << m.olSupplyWId << " " << m.olDeliveryD.time_since_epoch().count()
        << " " << m.olQuantity << " " << m.olAmount << " " <<  m.olDistInfo
        << "\r\n";
    }
};

struct Stock {
    int sWId;
    int sIId;
    int sQuantity;
    int sYtd;
    int sOrderCnt;
    int sRemoteCnt;
    std::string sData;
    std::array<std::string, DISTRICTS_PER_WAREHOUSE> sDists;

    friend auto operator<<(std::ostream &os, Stock const &m)
        -> std::ostream & {
        os << "Stock: " << m.sWId << " " << m.sIId << " " << m.sQuantity << " "
        << m.sYtd << " " << m.sOrderCnt << " " << m.sRemoteCnt << " " << m.sData;

        for(auto dist: m.sDists) {
            os << " " << dist;
        }
        return os << "\r\n";
    }
};

struct History {
    int hWId;
    int hDId;
    int hCWId;
    int hCDId;
    int hCId;
    std::chrono::time_point<std::chrono::system_clock> hDate;
    double hAmount;
    std::string hData;
    friend auto operator<<(std::ostream &os, History const &m)
        -> std::ostream & {
        return os << "History: " << m.hWId << " " << m.hDId << " " << m.hCId << " "
        << m.hCWId << " " << m.hCDId << " " << m.hCId << " " << m.hDate.time_since_epoch().count() <<
        " " << m.hAmount << " " << m.hData
        << "\r\n";
    }
};
class RandomHelper {
public:
    RandomHelper()  {
    }

    template<typename T>
    void shuffle(T& container){
        std::shuffle(container.begin(), container.end(), this->gen);
    }
    int number(int l, int u);
    int numberExcluding(int l, int u, int excluding);
    void seed(int s);

    std::string alphaString(int lower, int upper);
    std::string numString(int lower, int upper);

    std::string lastName(int num);
    std::string randomLastName(int maxcid);
    void setCValues(const NuRandC& c) {
        cValues = c;
    }

    int NuRand(int A, int x, int y);

    template <typename T> T fixedPoint(int digits, T u, T l) {
        assert(digits > 0);
        assert(l < u);
        T multiplier = pow(10, digits);
        int intl = static_cast<int>(l * multiplier + 0.5);
        int intu = static_cast<int>(u * multiplier + 0.5);
        return (T)number(intl, intu) / multiplier;
    }

    std::vector<int> uniqueIds(int num, int min, int max);

// Per execution generators
    TransactionType nextTransactionType();
    int makeWarehouseId(const ScaleParameters& params);
    int makeDistrictId(const ScaleParameters& params);
    int makeCustomerId(const ScaleParameters& params);
    int makeItemId(const ScaleParameters& params);
    // Transactions
    void generateDeliveryParams(const ScaleParameters& params, DeliveryParams& out);
    void generateNewOrderParams(const ScaleParameters& params, NewOrderParams& out);
    void generateOrderStatusParams(const ScaleParameters& params, OrderStatusParams& out);
    void generatePaymentParams(const ScaleParameters& params, PaymentParams& out);
    void generateStockLevelParams(const ScaleParameters& params, StockLevelParams& out);

    // Loading
    void generateItem(int id, bool original, Item& out);
    void generateWarehouse(int wid, Warehouse& out);
    void generateDistrict(int did, int wid, int nextOId, District& out);
    void generateCustomer(int cwid, int cdid, int cid, bool badCredit, Customer& out);
    void generateOrder(int owid, int odid, int oid, int ocid, int oilcnt, bool newOrder, Order& out);
    void generateOrderLine(const ScaleParameters& params, int olwid, int oldid, int oloid, int olnumber, int maxitems, bool newOrder, OrderLine& out);
    void generateStock(int swid, int siid, bool original, Stock& out);
    void generateHistory(int hcwid, int hcdid, int hcid, History& out);
private:
    std::string generateString(int lower, int upper, char base, int num);

    void generateAddress(StreetAddress& out);
    double generateTax();
    std::string generateZip();
    void fillOriginal(std::string& data);

    thread_local static std::random_device rd;
    thread_local static std::mt19937 gen;

    NuRandC cValues;
};

RandomHelper randomHelper;


struct ScaleParameters {
    ScaleParameters(int Items, int Warehouses, int DistrictsPerWarehouse, int CustomersPerDistrict, int NewOrdersPerDistrict)
    : items(Items), warehouses(Warehouses), districtsPerWarehouse(DistrictsPerWarehouse), customersPerDistrict(CustomersPerDistrict), newOrdersPerDistrict(NewOrdersPerDistrict) {
        assert(warehouses > 0);
        assert(1 <= items && items <= NUM_ITEMS);
        assert(1 <= districtsPerWarehouse && districtsPerWarehouse <= DISTRICTS_PER_WAREHOUSE);
        assert(1 <= customersPerDistrict && customersPerDistrict <= CUSTOMERS_PER_DISTRICT);
        assert(1 <= newOrdersPerDistrict && newOrdersPerDistrict <= INITIAL_NEW_ORDERS_PER_DISTRICT);
        startingWarehouse = 1;
        endingWarehouse = warehouses + startingWarehouse - 1;
    }
    int items;
    int warehouses;
    int startingWarehouse;
    int districtsPerWarehouse;
    int customersPerDistrict;
    int newOrdersPerDistrict;
    int endingWarehouse;

    static ScaleParameters makeDefault(int Warehouses);
    static ScaleParameters makeScaled(int Warehouses, double scaleFactor);
};

} // namespace tpcc


#endif // TPCHELPERS
