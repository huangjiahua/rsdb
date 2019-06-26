//
// Created by jiahua on 19-6-26.
//

#include "rsdb/rsdb.h"
#include <string>
#include <iostream>

using namespace rsdb;
using namespace std;

int main() {
    DB db("iterator-test", OpenOptions());
    for (int i = 0; i < 10000; i++) {
        string key = to_string(i) + "key";
        string value = "value" + to_string(i);
        assert(db.Put(key, value, WriteOptions()));
    }

    for (int i = 3; i < 10000; i += 4) {
        string key = to_string(i) + "key";
        db.Delete(key);
    }

    for (int i = 3; i < 10000; i += 12) {
        string key = to_string(i) + "key";
        string value = "value" + to_string(i);
        db.Put(key, value, WriteOptions());
    }

    auto iter = db.GetIterator();
    for (iter.SeekToFirst(); iter.Valid(); iter.Next()) {
        cout << iter.Key().ToString() << " " << iter.Value().ToString() << endl;
    }

}

