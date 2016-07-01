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
    ptr = j128m_insert(&jm, uint128_key(1, 1)); *ptr = 11;
    ptr = j128m_insert(&jm, uint128_key(1, 2)); *ptr = 12;
    ptr = j128m_insert(&jm, uint128_key(1, 3)); *ptr = 13;
    ptr = j128m_insert(&jm, uint128_key(2, 1)); *ptr = 21;
    ptr = j128m_insert(&jm, uint128_key(2, 2)); *ptr = 22;
    ptr = j128m_insert(&jm, uint128_key(2, 3)); *ptr = 23;

    __uint128_t idx = 0;
    PWord_t pv = NULL;
    j128m_find(&jm, &pv, &idx);

    uint64_t expected[6] = {11, 12, 13,
                            21, 22, 23};

    int i = 0;
    while (pv != NULL) {
        assert(*pv == expected[i]);
        j128m_next(&jm, &pv, &idx);
        i++;
    }
    assert(i == 6);

    assert(j128m_get(&jm, uint128_key(1, 1)) != NULL);
    assert(j128m_del(&jm, uint128_key(1, 1)) == 1);
    /* Causes double free error: assert(j128m_del(&jm, uint128_key(1, 1)) == 1); */
    assert(j128m_get(&jm, uint128_key(1, 1)) == NULL);

    return 0;
}
