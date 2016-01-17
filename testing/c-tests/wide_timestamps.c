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

    assert(tdb_cons_finalize(c) ==
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

    const tdb_event *event;

    uint64_t i, num_events = sizeof(TSTAMPS1) / sizeof(TSTAMPS1[0]);
    tdb *db = make_tdb(argv[1], TSTAMPS1, num_events, 0);
    tdb_cursor *cursor = tdb_cursor_new(db);
    assert(tdb_get_trail(cursor, 0) == 0);
    for (i = 0; (event = tdb_cursor_next(cursor)); i++)
        assert(event->timestamp == TSTAMPS1[i]);
    assert(i == num_events);
    tdb_close(db);
    tdb_cursor_free(cursor);

    num_events = sizeof(TSTAMPS2) / sizeof(TSTAMPS2[0]);
    db = make_tdb(argv[1], TSTAMPS2, num_events, 0);
    cursor = tdb_cursor_new(db);
    assert(tdb_get_trail(cursor, 0) == 0);
    for (i = 1; (event = tdb_cursor_next(cursor)); i++)
        /* reverse order */
        assert(event->timestamp == TSTAMPS2[num_events - i]);
    assert(i == num_events + 1);
    tdb_close(db);
    tdb_cursor_free(cursor);

    num_events = sizeof(TSTAMPS3) / sizeof(TSTAMPS3[0]);
    db = make_tdb(argv[1], TSTAMPS3, num_events, 0);
    cursor = tdb_cursor_new(db);
    assert(tdb_get_trail(cursor, 0) == 0);
    for (i = 0; (event = tdb_cursor_next(cursor)); i++)
        assert(event->timestamp == TSTAMPS3[i]);
    assert(i == num_events);
    tdb_close(db);
    tdb_cursor_free(cursor);

    make_tdb(argv[1], TSTAMPS4, sizeof(TSTAMPS4) / sizeof(TSTAMPS4[0]), 1);
    make_tdb(argv[1], TSTAMPS5, sizeof(TSTAMPS5) / sizeof(TSTAMPS5[0]), 1);

    return 0;
}
