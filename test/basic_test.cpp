#include <iostream>
#include "rsdb/rsdb.h"

using namespace std;
using namespace rsdb;

int main() {
    DB db("tmp", rsdb::OpenOptions());
    bool status;
    cout << "Is DB set up? " << boolalpha << db.Valid() << endl;
    assert(db.Valid());

    cout << "Put k-v -> <key, value>" << endl;
    status = db.Put(string("key"), string("value"), WriteOptions());
    assert(status);

    std::string value;
    status = db.Get(string("key"), &value);
    assert(status);
    cout << "Get value -> " << value << endl;


    return 0;
}