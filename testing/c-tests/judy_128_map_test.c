
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

#define MEM_LIMIT (128 * 1024)
#define NUM_ITER 1000000
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
    j128m_free(&jm);

    /* test out of memory condition */

    assert(test_out_of_memory() == 0);

    return 0;
}
