#include <iostream>
#include "gtest/gtest.h"
#include <unordered_map>

#include "dbphd/tpc/tpchelpers.hpp"

using namespace tpcc;
using namespace std;

TEST(TPCHelpers, number) {
    randomHelper.seed(0);
    EXPECT_EQ(randomHelper.number(1,100), 55);
    EXPECT_EQ(randomHelper.number(1,100), 60);
    EXPECT_EQ(randomHelper.number(1,100), 72);
    EXPECT_EQ(randomHelper.number(1,100), 85);
    EXPECT_EQ(randomHelper.number(1,100), 61);
    EXPECT_EQ(randomHelper.number(100,100), 100);
}

TEST(TPCHelpers, numberExcluding) {
    randomHelper.seed(0);
    EXPECT_EQ(randomHelper.numberExcluding(1,100,55), 56);
    EXPECT_EQ(randomHelper.numberExcluding(1,100,60), 59);
    EXPECT_EQ(randomHelper.numberExcluding(1,100,72), 71);
    EXPECT_EQ(randomHelper.numberExcluding(1,100,85), 84);
    EXPECT_EQ(randomHelper.numberExcluding(1,100,61), 60);
}

TEST(TPCHelpers, fixedPoint) {
    randomHelper.seed(0);
    EXPECT_DOUBLE_EQ(randomHelper.fixedPoint<double>(2,100,1), 55.33);
    EXPECT_DOUBLE_EQ(randomHelper.fixedPoint<double>(3,100,1), 59.692);
    EXPECT_DOUBLE_EQ(randomHelper.fixedPoint<double>(4,100,1), 71.8038);
    EXPECT_DOUBLE_EQ(randomHelper.fixedPoint<double>(5,100,1), 84.582310000000007);
    EXPECT_DOUBLE_EQ(randomHelper.fixedPoint<double>(6,100,1), 60.673574000000002);
}

TEST(TPCHelpers, alphaString) {
    randomHelper.seed(0);
    EXPECT_STRCASEEQ(randomHelper.alphaString(1,10).c_str(), "psvpwo");
    EXPECT_STRCASEEQ(randomHelper.alphaString(1,10).c_str(), "lqqjlhxbz");
    EXPECT_STRCASEEQ(randomHelper.alphaString(1,10).c_str(), "jmu");
    EXPECT_STRCASEEQ(randomHelper.alphaString(1,10).c_str(), "nmokyvbic");
    EXPECT_STRCASEEQ(randomHelper.alphaString(1,10).c_str(), "ajvyudw");
    EXPECT_STRCASEEQ(randomHelper.alphaString(10,10).c_str(), "zmuulnurds");
}

TEST(TPCHelpers, numString) {
    randomHelper.seed(0);
    EXPECT_STRCASEEQ(randomHelper.numString(1,10).c_str(), "578685");
    EXPECT_STRCASEEQ(randomHelper.numString(1,10).c_str(), "466342809");
    EXPECT_STRCASEEQ(randomHelper.numString(1,10).c_str(), "347");
    EXPECT_STRCASEEQ(randomHelper.numString(1,10).c_str(), "545398030");
    EXPECT_STRCASEEQ(randomHelper.numString(1,10).c_str(), "0389718");
    EXPECT_STRCASEEQ(randomHelper.numString(10,10).c_str(), "9478457617");
}

TEST(TPCHelpers, lastName) {
    EXPECT_STRCASEEQ(randomHelper.lastName(1).c_str(), "BARBAROUGHT");
    EXPECT_STRCASEEQ(randomHelper.lastName(50).c_str(), "BARESEBAR");
    EXPECT_STRCASEEQ(randomHelper.lastName(100).c_str(), "OUGHTBARBAR");
    EXPECT_STRCASEEQ(randomHelper.lastName(400).c_str(), "PRESBARBAR");
    EXPECT_STRCASEEQ(randomHelper.lastName(500).c_str(), "ESEBARBAR");
    EXPECT_STRCASEEQ(randomHelper.lastName(999).c_str(), "EINGEINGEING");
}

TEST(TPCHelpers, NuRandC) {
    randomHelper.seed(0);
    NuRandC rand = NuRandC::createRandom();
    EXPECT_EQ(rand.cId, 607);
    EXPECT_EQ(rand.cLast, 183);
    EXPECT_EQ(rand.orderLineItemID, 4495);
}

TEST(TPCHelpers, NuRand) {
    randomHelper.seed(0);
    EXPECT_EQ(randomHelper.NuRand(255,0,100), 31);
    EXPECT_EQ(randomHelper.NuRand(1023,0,100), 88);
    EXPECT_EQ(randomHelper.NuRand(8191,0,100), 56);
    EXPECT_EQ(randomHelper.NuRand(255,0,100), 2);
}

TEST(TPCHelpers, randomLastName) {
    randomHelper.seed(0);
    EXPECT_STRCASEEQ(randomHelper.randomLastName(10).c_str(), "BARBARPRES");
    EXPECT_STRCASEEQ(randomHelper.randomLastName(100).c_str(), "BARPRIBAR");
    EXPECT_STRCASEEQ(randomHelper.randomLastName(5).c_str(), "BARBAROUGHT");
    EXPECT_STRCASEEQ(randomHelper.randomLastName(3).c_str(), "BARBAROUGHT");
}

TEST(TPCHelpers, uniqueIds) {
    randomHelper.seed(0);
    auto ids = randomHelper.uniqueIds(5, 1, 100);
    EXPECT_EQ(ids[0], 55);
    EXPECT_EQ(ids[1], 60);
    EXPECT_EQ(ids[2], 72);
    EXPECT_EQ(ids[3], 85);
    EXPECT_EQ(ids[4], 61);
}

// TransactionHelpers
TEST(TPCHelpers, nextTransactionType) {
    randomHelper.seed(0);
    unordered_map<TransactionType, int> map;
    map[TransactionType::NewOrder] = 0;
    map[TransactionType::Payment] = 0;
    map[TransactionType::OrderStatus] = 0;
    map[TransactionType::Delivery] = 0;
    map[TransactionType::StockLevel] = 0;
    for(int i = 0; i < 100; ++i) {
      map[randomHelper.nextTransactionType()]++;
    }

    EXPECT_EQ(map[TransactionType::NewOrder], 39);
    EXPECT_EQ(map[TransactionType::Payment], 48);
    EXPECT_EQ(map[TransactionType::OrderStatus], 5);
    EXPECT_EQ(map[TransactionType::Delivery], 4);
    EXPECT_EQ(map[TransactionType::StockLevel], 4);
}
