//
// Created by jiahua on 19-6-28.
//

#include "rsdb/rsdb.h"
#include <thread>
#include <vector>
#include <atomic>
#include <gtest/gtest.h>

using namespace std;
using namespace rsdb;

constexpr int thread_cnt = 4;

struct ThreadTest {
    DB db;
    atomic<int> k;
    vector<vector<string>> keys;
    vector<thread> threads;

    static void generate_keys(vector<string> &keys, int start) {
        for (int i = 0; i < 1000; i++) {
            keys[i] = "key" + to_string(start + i);
        }
    }

    ThreadTest() : db("tmp_threads_file", OpenOptions()), k(2 * thread_cnt),
                   keys(4, vector<string>(1000)), threads(thread_cnt) {
        for (int i = 0; i < 4; i++) {
            generate_keys(keys[i], i * 1000);
        }
    }
};

TEST(multi_thread_test, multi_thread_test_basic) {
    ThreadTest test;
    for (int i = 0; i < thread_cnt; i++) {
        test.threads[i] = thread([&](DB &rdb, int idx) {
            std::string data;
            test.k--;
            while (test.k != thread_cnt);
            for (const auto &j : test.keys[idx])
                EXPECT_TRUE(rdb.Put(j, j, WriteOptions()));
            test.k--;
            while (test.k != 0);
            for (const auto &j : test.keys[idx]) {
                EXPECT_TRUE(rdb.Get(j, &data));
                EXPECT_EQ(data, j);
            }
        }, ref(test.db), i);
    }
    for (auto &t : test.threads) t.join();
}




