
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <float.h>

#define DSFMT_MEXP 521
#include <dsfmt/dSFMT.h>

#include <Judy.h>

#include <traildb.h>
#include "tdb_test.h"

#define NUM_TRAILS 100000
#define NUM_EVENTS 10

static void gen_random_uuid(uint8_t *uuid, dsfmt_t *state)
{
    double rand[2] = {dsfmt_genrand_close_open(state) * DBL_MAX,
                      dsfmt_genrand_close_open(state) * DBL_MAX};
    memcpy(uuid, rand, 16);
}

int main(int argc, char **argv)
{
    static const char **fields;
    static uint64_t *lengths;
    dsfmt_t state;
    Pvoid_t uuids = NULL;
    tdb_cons* c = tdb_cons_init();
    test_cons_settings(c);
    uint64_t i, j;
    __uint128_t prev_uuid = 0;
    Word_t key;
    int tst;

    assert(tdb_cons_open(c, getenv("TDB_TMP_DIR"), fields, 0) == 0);
    dsfmt_init_gen_rand(&state, 2489);

    for (i = 0; i < NUM_TRAILS; i++){
        uint8_t uuid[16];
        gen_random_uuid(uuid, &state);
        memcpy(&key, uuid, 8);

        J1S(tst, uuids, key);
        if (!tst){
            printf("half-word collision! change random seed!\n");
            return -1;
        }

        for (j = 0; j < NUM_EVENTS; j++)
            tdb_cons_add(c, uuid, i * 100 + j, fields, lengths);
    }
    J1C(key, uuids, 0, -1);
    assert(key == NUM_TRAILS);
    assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);

    tdb* t = tdb_init();
    assert(tdb_open(t, getenv("TDB_TMP_DIR")) == 0);

    assert(tdb_num_trails(t) == NUM_TRAILS);
    assert(tdb_num_events(t) == NUM_TRAILS * NUM_EVENTS);

    for (i = 0; i < NUM_TRAILS; i++){
        __uint128_t this_uuid;

        /* uuids must be monotonically increasing */
        memcpy(&this_uuid, tdb_get_uuid(t, i), 16);
        assert(this_uuid > prev_uuid);
        prev_uuid = this_uuid;

        /* remove this uuid from the uuid set and make sure it exists */
        memcpy(&key, &this_uuid, 8);
        J1U(tst, uuids, key);
        assert(tst == 1);
    }

    /* make sure we retrieved all uuids */
    J1C(key, uuids, 0, -1);
    assert(key == 0);

    return 0;
}
