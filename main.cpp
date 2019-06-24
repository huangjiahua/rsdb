#include <iostream>
#include "rsdb/rsdb.h"

int main() {
    std::cout << "Hello, World!" << std::endl;
    rsdb::DB db("tmp", rsdb::OpenOptions());
    std::cout << std::boolalpha << db.Valid() << std::endl;
    return 0;
}