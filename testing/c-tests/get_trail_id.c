
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>
#include <float.h>

#include <Judy.h>

#include <traildb.h>

#define DSFMT_MEXP 521
#include <dsfmt/dSFMT.h>


#define NUM_ITER 100000

int main(int argc, char **argv)
{
    static uint8_t uuid[16];
    static dsfmt_t state;
    uint64_t i;
    Pvoid_t comp = NULL;
    Word_t n, key = 0;
    int tst;

    dsfmt_init_gen_rand(&state, 23489);
    tdb_cons* c = tdb_cons_init();
    assert(tdb_cons_open(c, argv[1], NULL, 0) == 0);

    for (i = 0; i < NUM_ITER; i++){
        double tmp = dsfmt_genrand_close_open(&state) * DBL_MAX;
        if (tmp){
            /* we want some collisions, hence only 3 byte keys */
            memcpy(uuid, &tmp, 3);
            memcpy(&key, &tmp, 3);
            assert(tdb_cons_add(c, uuid, i, NULL, NULL) == 0);
            J1S(tst, comp, key);
        }
    }

    assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);
    tdb* t = tdb_init();
    assert(tdb_open(t, argv[1]) == 0);

    J1C(n, comp, 0, -1)
    /* check that we have collisions */
    assert(n < NUM_ITER);
    assert(tdb_num_trails(t) == n);

    memset(uuid, 0, 16);
    assert(tdb_get_trail_id(t, uuid, &i) == TDB_ERR_UNKNOWN_UUID);

    key = n = 0;
    J1F(tst, comp, key);
    while (tst){
        memcpy(uuid, &key, 3);
        assert(tdb_get_trail_id(t, uuid, &i) == 0);
        assert(i == n++);
        J1N(tst, comp, key);
    }

    return 0;
}
