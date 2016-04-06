#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <float.h>

#include <sys/time.h>
#include <sys/resource.h>

#include <Judy.h>
#include <judy_128_map.h>

#include "tdb_test.h"


__uint128_t uint128_key(uint64_t hi, uint64_t lo) {
    __uint128_t k = hi;
    k <<= 64;
    k |= lo;
    return k;
}

int main(int argc, char **argv)
{
    struct judy_128_map jm;

    j128m_init(&jm);


    PWord_t ptr = NULL;
    ptr = j128m_insert(&jm, uint128_key(1, 0)); *ptr = 10;
    ptr = j128m_insert(&jm, uint128_key(1, 1)); *ptr = 11;
    ptr = j128m_insert(&jm, uint128_key(1, 2)); *ptr = 12;
    ptr = j128m_insert(&jm, uint128_key(2, 0)); *ptr = 20;
    ptr = j128m_insert(&jm, uint128_key(2, 1)); *ptr = 21;
    ptr = j128m_insert(&jm, uint128_key(2, 2)); *ptr = 22;

    __uint128_t idx = 0;
    PWord_t pv = NULL;
    j128m_find(&jm, &pv, &idx);

    uint64_t expected[6] = {10, 11, 12,
                            20, 21, 22};

    int i = 0;
    while (pv != NULL) {
        assert(*pv == expected[i]);
        j128m_next(&jm, &pv, &idx);
        i++;
    }

    return 0;
}
