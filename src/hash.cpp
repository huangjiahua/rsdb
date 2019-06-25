//
// Created by jiahua on 19-6-25.
//

#include "db_impl.h"
#include "hash.h"

#define    DCHARHASH(h, c)    ((h) = 0x63c63cd9*(h) + 0x9c39c33d + (c))

size_t Hash(const char *key, size_t len) {
    const uint8_t *e, *k;
    uint32_t h;
    uint8_t c;

    k = reinterpret_cast<const uint8_t *>(key);
    e = k + len;
    for (h = 0; k != e;) {
        c = *k++;
        if (!c && k > e)
            break;
        DCHARHASH(h, c);
    }
    return (h);
}




