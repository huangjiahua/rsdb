//
// created by jiahua
//
#include <iostream>
#include "rsdb/rsdb.h"

using namespace std;
using namespace rsdb;

int main() {
    DB db("tmp", rsdb::OpenOptions());
    bool status;
    cout << "Is DB set up? " << boolalpha << db.Valid() << endl;
    assert(db.Valid());

    for (int i = 0; i < 10000; i++) {
        std::string key = string("key") + to_string(i);
        cout << "Put k-v -> <" << key << ", value>" << endl;
        status = db.Put(key, string("value"), WriteOptions());
        assert(status);

        std::string value;
        status = db.Get(key, &value);
        assert(status);
        cout << "Get value -> " << value << endl;
    }


    return 0;
}