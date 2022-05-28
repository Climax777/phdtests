#if !defined(TPCHELPERS)
#define TPCHELPERS
#include <random>
#include <string>
#include <vector>
#include <cassert>
#include <chrono>


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
    NuRandC() : cLast(0), cId(0), orderLineItemID(0) {
    }
  NuRandC(int cLast, int cId, int orderLineItemID)
      : cLast(cLast), cId(cId), orderLineItemID(orderLineItemID) {}
  int cLast;
  int cId;
  int orderLineItemID;

    static bool isValid(int cRun, int cLoad);
  static NuRandC createRandom();
  static NuRandC createRandomForRun(const NuRandC &cLoad);
};

class ScaleParameters;

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
    string cLast;
    std::chrono::time_point<std::chrono::system_clock> hDate;
};

struct OrderStatusParams {
    int wId;
    int dId;
    int cId;
    string cLast;
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
class RandomHelper {
public:
    RandomHelper() : gen(rd()) {
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

    template<typename T>
    T fixedPoint(int digits, T u, T l);

    std::vector<int> uniqueIds(int num, int min, int max);

// Per execution generators
    TransactionType nextType();
    int makeWarehouseId(const ScaleParameters& params);
    int makeDistrictId(const ScaleParameters& params);
    int makeCustomerId(const ScaleParameters& params);
    int makeItemId(const ScaleParameters& params);
    void generateDeliveryParams(const ScaleParameters& params, DeliveryParams& out);
    void generateNewOrderParams(const ScaleParameters& params, NewOrderParams& out);
    void generateOrderStatusParams(const ScaleParameters& params, OrderStatusParams& out);
    void generatePaymentParams(const ScaleParameters& params, PaymentParams& out);
    void generateStockLevelParams(const ScaleParameters& params, StockLevelParams& out);

private:
    std::string generateString(int lower, int upper, char base, int num);
    std::random_device rd;
    std::mt19937 gen;

    NuRandC cValues;
};

static RandomHelper random;

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
