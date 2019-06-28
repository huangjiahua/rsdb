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

constexpr int thread_cnt = 1;

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
        for (const auto &v : keys[0])
            assert(db.Put(v, v, WriteOptions()));
    }
};

TEST(multithread_test, multithread_iterator_test) {
    ThreadTest test;
    for (int i = 0; i < test.threads.size(); i++) {
        test.threads[i] = thread([&](int idx) {
            auto iter = test.db.GetIterator();
            int j = 0;
            for (iter.SeekToFirst(); iter.Valid(); iter.Next(), j++) {
                EXPECT_EQ(iter.Key().ToString(), test.keys[0][j]);
                EXPECT_EQ(iter.Value().ToString(), test.keys[0][j]);
            }
        }, i);
    }
    for (auto &t : test.threads) t.join();
}

