#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <traildb.h>

static tdb *make_tdb(const char *root,
                     const uint64_t *tstamps,
                     uint32_t num,
                     int should_fail)
{
    static uint8_t uuid[16];
    const char *fields[] = {};
    tdb_cons* c = tdb_cons_init();
    uint64_t zero = 0;
    uint32_t i;

    assert(tdb_cons_open(c, root, fields, 0) == 0);

    for (i = 0; i < num; i++)
        assert(tdb_cons_add(c, uuid, tstamps[i], fields, &zero) == 0);

    assert(tdb_cons_finalize(c, 0) ==
           (should_fail ? TDB_ERR_TIMESTAMP_TOO_LARGE: 0));
    tdb_cons_close(c);

    if (!should_fail){
        tdb* t = tdb_init();
        assert(tdb_open(t, root) == 0);
        return t;
    }else
        return NULL;
}

int main(int argc, char** argv)
{
    tdb_item *items;
    uint64_t i, n, items_len = 0;

    /* small min_timestamp, large max_timedelta, sorted */
    const uint64_t TSTAMPS1[] = {1,
                                 2,
                                 3,
                                 UINT32_MAX,
                                 UINT32_MAX + 1LLU,
                                 TDB_MAX_TIMEDELTA - 10,
                                 TDB_MAX_TIMEDELTA - 9,
                                 TDB_MAX_TIMEDELTA - 8};

    /* large min_timestamp, small max_timedelta, reverse order */
    const uint64_t TSTAMPS2[] = {TDB_MAX_TIMEDELTA - 1,
                                 TDB_MAX_TIMEDELTA - 3,
                                 TDB_MAX_TIMEDELTA - 5};

    /* this should not fail */
    const uint64_t TSTAMPS3[] = {10, TDB_MAX_TIMEDELTA + 9};

    /* this should fail */
    const uint64_t TSTAMPS4[] = {10, TDB_MAX_TIMEDELTA + 11};

    /* this should fail */
    const uint64_t TSTAMPS5[] = {TDB_MAX_TIMEDELTA + 1};

    tdb *db = make_tdb(argv[1], TSTAMPS1, sizeof(TSTAMPS1) / sizeof(TSTAMPS1[0]), 0);
    assert(tdb_get_trail(db, 0, &items, &items_len, &n, 0) == 0);
    assert((n / 2) == sizeof(TSTAMPS1) / sizeof(TSTAMPS1[0]));
    for (i = 0; i < n; i += 2){
        assert(TSTAMPS1[i / 2] == items[i]);
    }
    tdb_close(db);

    db = make_tdb(argv[1], TSTAMPS2, sizeof(TSTAMPS2) / sizeof(TSTAMPS2[0]), 0);
    assert(tdb_get_trail(db, 0, &items, &items_len, &n, 0) == 0);
    assert((n / 2) == sizeof(TSTAMPS2) / sizeof(TSTAMPS2[0]));
    for (i = 0; i < n; i += 2){
        /* reverse order */
        assert(TSTAMPS2[(n / 2) - (i / 2 + 1)] == items[i]);
    }
    tdb_close(db);

    db = make_tdb(argv[1], TSTAMPS3, sizeof(TSTAMPS3) / sizeof(TSTAMPS3[0]), 0);
    assert(tdb_get_trail(db, 0, &items, &items_len, &n, 0) == 0);
    assert((n / 2) == sizeof(TSTAMPS3) / sizeof(TSTAMPS3[0]));
    for (i = 0; i < n; i += 2){
        assert(TSTAMPS3[i / 2] == items[i]);
    }
    tdb_close(db);

    make_tdb(argv[1], TSTAMPS4, sizeof(TSTAMPS4) / sizeof(TSTAMPS4[0]), -5);
    make_tdb(argv[1], TSTAMPS5, sizeof(TSTAMPS5) / sizeof(TSTAMPS5[0]), -5);

    return 0;
}
