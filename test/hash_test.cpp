//
// Created by jiahua on 19-6-25.
//

#include <gtest/gtest.h>
#include <string>
#include "hash.h"
#include "db_impl.h"

using namespace rsdb;
using namespace std;


TEST(hash_test, hash_test_1) {
    string dat1("Hello, World");
    string dat2("Hello, Earth");
    auto hash1 = Hash(dat1.c_str(), dat1.size());
    auto hash2 = Hash(dat2.c_str(), dat2.size());
    EXPECT_NE(hash1, hash2);
}