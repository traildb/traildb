
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <float.h>

#define DSFMT_MEXP 521
#include <dsfmt/dSFMT.h>

#include <Judy.h>
#include <judy_128_map.h>


#define NUM_ITER 1000000

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

    /* test get */

    dsfmt_init_gen_rand(&state, 123);

    for (i = 0; i < NUM_ITER; i++){
        Word_t *ptr = j128m_get(&jm, gen_key(i, &state));
        assert(ptr != NULL);
        assert(*ptr == i + 1);
    }

    /* test fold */

    dsfmt_init_gen_rand(&state, 123);
    j128m_fold(&jm, fun, &foldstate);
    assert(foldstate.count == NUM_ITER);

    j128m_free(&jm);
    return 0;
}
