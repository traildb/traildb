
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <float.h>

#include <sys/time.h>
#include <sys/resource.h>

#define DSFMT_MEXP 521
#include <dsfmt/dSFMT.h>

#include <Judy.h>
#include <judy_128_map.h>

#include "tdb_test.h"

#define MEM_LIMIT (128 * 1024)
#define NUM_ITER 1000000
#define NUM_SMALL_ITER 3
#define NUM_OOM_ITER 100000000

struct foldstate{
    __uint128_t prev_key;
    uint64_t count;
};

static void *fun(__uint128_t key, Word_t *value, void *state)
{
    struct foldstate *foldstate = (struct foldstate*)state;

    assert(foldstate->prev_key < key);
    foldstate->prev_key = key;
    key >>= 64;

    assert(*value - 1 == key);

    ++foldstate->count;

    return foldstate;
}

static __uint128_t gen_key(uint64_t idx, dsfmt_t *state)
{
    double rand = dsfmt_genrand_close_open(state) * DBL_MAX;
    __uint128_t ret = idx;
    ret <<= 64;
    memcpy(&ret, &rand, 8);
    return ret;
}

static int test_out_of_memory()
{
    uint64_t i;
    struct judy_128_map jm;
    dsfmt_t state;
    struct rlimit limit = {.rlim_cur = MEM_LIMIT, .rlim_max = MEM_LIMIT};
    assert(setrlimit(RLIMIT_AS, &limit) == 0);

    j128m_init(&jm);

    dsfmt_init_gen_rand(&state, 123);

    for (i = 0; i < NUM_OOM_ITER; i++){
        Word_t *ptr = j128m_insert(&jm, gen_key(i, &state));
        if (!ptr){
            /* we expect folding to consume no extra memory, so it should
               succeed even after insert fails with OOM */
            struct foldstate foldstate = {};
            j128m_fold(&jm, fun, &foldstate);
            assert(foldstate.count == i);
            assert(j128m_num_keys(&jm) == i);

            j128m_free(&jm);
            return 0;
        }
        *ptr = i + 1;
    }

    return 1;
}

__uint128_t mk_key(uint64_t hi, uint64_t lo)
{
    __uint128_t key;
    key = hi;
    key <<= 64;
    key |= lo;
    return key;
}

void print_key(__uint128_t key)
{
    uint64_t hi_key = (key >> 64);
    uint64_t lo_key = key;
    fprintf(stderr, "print_key: hi %lu, lo %lu\n", hi_key, lo_key);
}

static int test_find()
{
    struct judy_128_map jm;
    __uint128_t key;
    Word_t *pv;
    dsfmt_t state;

    j128m_init(&jm);
    dsfmt_init_gen_rand(&state, 123);

    __uint128_t keys[3];
    keys[0] = mk_key(10, 10);
    keys[1] = mk_key(10, 11);
    keys[2] = mk_key(11, 10);

    pv = j128m_insert(&jm, keys[0]); *pv = 1;
    pv = j128m_insert(&jm, keys[1]); *pv = 2;
    pv = j128m_insert(&jm, keys[2]); *pv = 3;

    /* Zero should always return the first key. (Keys in the array
       cannot be zero) */
    key = 0;
    j128m_find(&jm, &pv, &key);
    assert(*pv == 1);

    key = mk_key(9, 9);
    j128m_find(&jm, &pv, &key);
    assert(*pv == 1);
    assert(key == mk_key(10, 10));

    key = mk_key(10, 10);
    j128m_find(&jm, &pv, &key);
    assert(*pv == 1);
    assert(key == mk_key(10, 10));

    j128m_next(&jm, &pv, &key);
    assert(*pv == 2);
    assert(key == mk_key(10, 11));

    key = mk_key(11, 9);
    j128m_find(&jm, &pv, &key);
    assert(*pv == 3);
    assert(key == mk_key(11, 10));

    /* Searching past the last key should return NULL */
    key = mk_key(-1, -1);
    j128m_find(&jm, &pv, &key);
    assert(pv == NULL);
    assert(key == mk_key(-1, -1));

    j128m_next(&jm, &pv, &key);
    assert(pv == NULL);

    return 0;
}


int main(int argc, char **argv)
{
    struct foldstate foldstate = {};
    struct judy_128_map jm;
    dsfmt_t state;
    uint32_t i;

    j128m_init(&jm);

    /* test insert */

    dsfmt_init_gen_rand(&state, 123);

    for (i = 0; i < NUM_ITER; i++){
        Word_t *ptr = j128m_insert(&jm, gen_key(i, &state));
        *ptr = i + 1;
    }

    /* test num_keys */

    assert(j128m_num_keys(&jm) == NUM_ITER);

    /* test get */

    dsfmt_init_gen_rand(&state, 123);

    for (i = 0; i < NUM_ITER; i++){
        Word_t *ptr = j128m_get(&jm, gen_key(i, &state));
        assert(ptr != NULL);
        assert(*ptr == i + 1);
    }
    for (;i < NUM_ITER * 2; i++){
        Word_t *ptr = j128m_get(&jm, gen_key(i, &state));
        assert(ptr == NULL);
    }

    /* test fold */

    dsfmt_init_gen_rand(&state, 123);
    j128m_fold(&jm, fun, &foldstate);
    assert(foldstate.count == NUM_ITER);


    /* test find/next iteration */

    uint64_t count = 0;
    __uint128_t prev_key = 0;
    __uint128_t key = 0;
    Word_t *pv = NULL;
    j128m_find(&jm, &pv, &key);
    while (pv != NULL) {
        assert(prev_key < key);

        prev_key = key;

        __uint128_t local_key = key >> 64; /* Cannot modify key as
                                              we'll use it in
                                              j128m_next */
        assert(*pv - 1 == local_key);
        count++;
        j128m_next(&jm, &pv, &key);

    }
    assert(count == NUM_ITER);
    j128m_free(&jm);


    /* test out of memory condition */

    assert(test_out_of_memory() == 0);

    /* test find works on ordered keys */

    assert(test_find() == 0);

    return 0;
}
