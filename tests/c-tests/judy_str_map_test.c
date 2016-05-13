
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <float.h>

#define DSFMT_MEXP 521
#include <dsfmt/dSFMT.h>

#include <Judy.h>
#include <judy_str_map.h>

#include "tdb_test.h"

#define NUM_ITER 100000

static void *fold_fun(uint64_t id, const char *value, uint64_t len, void *state)
{
    /* test that IDs are returned in ascending order */
    uint64_t prev = *(uint64_t*)state;
    assert(prev < id);
    assert(len == 6);
    memcpy(state, &id, 8);
    return state;
}

int main(int argc, char **argv)
{
    struct judy_str_map jsm;
    dsfmt_t state;
    uint32_t i;
    Pvoid_t comp = NULL;
    Pvoid_t keys = NULL;
    Word_t count;

    jsm_init(&jsm);

    /* test insert */

    dsfmt_init_gen_rand(&state, 123);

    for (i = 1; i < NUM_ITER; i++){
        double rand = dsfmt_genrand_close_open(&state) * DBL_MAX;
        uint64_t val = jsm_insert(&jsm, (const char*)&rand, 6);
        Word_t *ptr;
        Word_t key;
        int tst;

        memcpy(&key, &rand, 8);
        J1S(tst, keys, key);
        assert(val > 0);
        assert(val <= i);
        JLI(ptr, comp, (Word_t)i);
        *ptr = val;
    }

    /* test num_keys */
    J1C(count, keys, 0, -1);
    assert(count == jsm_num_keys(&jsm));

    /* test get */

    dsfmt_init_gen_rand(&state, 123);

    for (i = 1; i < NUM_ITER; i++){
        double rand = dsfmt_genrand_close_open(&state) * DBL_MAX;
        Word_t *ptr;
        uint64_t val = jsm_get(&jsm, (const char*)&rand, 6);
        assert(val > 0);
        assert(val <= i);
        JLG(ptr, comp, (Word_t)i);
        assert(ptr != NULL);
        assert(*ptr == val);
    }

    /* test fold */
    uint64_t prev = 0;
    jsm_fold(&jsm, fold_fun, &prev);
    assert(prev == jsm_num_keys(&jsm));

    jsm_free(&jsm);
    return 0;
}
