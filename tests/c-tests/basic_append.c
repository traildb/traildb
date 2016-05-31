
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <traildb.h>
#include <tdb_io.h>

#include "tdb_test.h"

static void empty_tdb_append(const char *root)
{
    char path[TDB_MAX_PATH_SIZE];
    const char *fields[] = {};

    tdb_path(path, "%s.%u", root, 1);
    tdb_cons* c1 = tdb_cons_init();
    test_cons_settings(c1);
    assert(tdb_cons_open(c1, path, fields, 0) == 0);
    assert(tdb_cons_finalize(c1) == 0);
    tdb* t1 = tdb_init();
    assert(tdb_open(t1, path) == 0);

    tdb_path(path, "%s.%u", root, 2);
    tdb_cons* c2 = tdb_cons_init();
    test_cons_settings(c2);
    assert(tdb_cons_open(c2, path, fields, 0) == 0);
    assert(tdb_cons_append(c2, t1) == 0);
    assert(tdb_cons_finalize(c2) == 0);

    tdb* t2 = tdb_init();
    assert(tdb_open(t2, path) == 0);
    assert(tdb_num_trails(t2) == 0);
    assert(tdb_num_fields(t2) == 1);
}

static void mismatching_fields(const char *root)
{
    char path[TDB_MAX_PATH_SIZE];
    const char *fields1[] = {"a", "b", "c"};
    const char *fields2[] = {"d", "e"};

    tdb_path(path, "%s.%u", root, 1);
    tdb_cons* c1 = tdb_cons_init();
    test_cons_settings(c1);
    assert(tdb_cons_open(c1, path, fields1, 2) == 0);
    assert(tdb_cons_finalize(c1) == 0);
    tdb* t1 = tdb_init();
    assert(tdb_open(t1, path) == 0);

    /* mismatching number of fields - this should fail */
    tdb_path(path, "%s.%u", root, 2);
    tdb_cons* c2 = tdb_cons_init();
    test_cons_settings(c2);
    assert(tdb_cons_open(c2, path, fields1, 3) == 0);
    assert(tdb_cons_append(c2, t1) == TDB_ERR_APPEND_FIELDS_MISMATCH);
    assert(tdb_cons_finalize(c2) == 0);

    /* mismatching field names - this should fail */
    tdb_path(path, "%s.%u", root, 2);
    c2 = tdb_cons_init();
    test_cons_settings(c2);
    assert(tdb_cons_open(c2, path, fields2, 2) == 0);
    assert(tdb_cons_append(c2, t1) == TDB_ERR_APPEND_FIELDS_MISMATCH);
    assert(tdb_cons_finalize(c2) == 0);
}

struct event{
    uint32_t time;
    char value1[1];
    char value2[1];
};

static void simple_append(const char *root)
{
    static uint8_t uuid[16];
    char path[TDB_MAX_PATH_SIZE];
    const char *fields[] = {"f1", "f2"};
    const uint64_t lengths[] = {1, 1};
    char prev;
    uint64_t len, tstamp, i;

    struct event EVENTS1[] = {
        {5,   "a", "1"},
        {20,  "c", "2"},
        {40,  "e", "3"},
    };

    struct event EVENTS2[] = {
        {10,  "b", "2"},
        {30,  "d", "2"},
        {100, "a", "2"},
    };
    tdb_path(path, "%s.%u", root, 1);
    tdb_cons* c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, path, fields, 2) == 0);

    for (i = 0; i < 3; i++){
        const char *values[] = {EVENTS1[i].value1, EVENTS1[i].value2};
        assert(tdb_cons_add(c,
                            uuid,
                            EVENTS1[i].time,
                            values,
                            lengths) == 0);
    }

    assert(tdb_cons_finalize(c) == 0);
    tdb* db = tdb_init();
    assert(tdb_open(db, path) == 0);

    tdb_path(path, "%s.%u", root, 2);
    c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, path, fields, 2) == 0);

    for (i = 0; i < 2; i++){
        const char *values[] = {EVENTS2[i].value1, EVENTS2[i].value2};
        assert(tdb_cons_add(c,
                            uuid,
                            EVENTS2[i].time,
                            values,
                            lengths) == 0);
    }

    assert(tdb_cons_append(c, db) == 0);

    {
        const char *values[] = {EVENTS2[2].value1, EVENTS2[2].value2};
        uuid[0] = 1;
        assert(tdb_cons_add(c,
                            uuid,
                            EVENTS2[2].time,
                            values,
                            lengths) == 0);
    }

    assert(tdb_cons_finalize(c) == 0);
    tdb_close(db);

    db = tdb_init();
    assert(tdb_open(db, path) == 0);
    tdb_cursor *cursor = tdb_cursor_new(db);
    assert(tdb_lexicon_size(db, 1) == 6);
    assert(tdb_lexicon_size(db, 2) == 4);
    assert(tdb_num_trails(db) == 2);
    assert(tdb_get_trail(cursor, 0) == 0);
    tstamp = 0;
    prev = 0;
    /* we expect a sequence
        (5, a), (10, b), (20, c), (30, d), (40, e)
       i.e. both timestamaps and values are monotonically increasing,
       which we check below
    */
    const tdb_event *event;
    while ((event = tdb_cursor_next(cursor))){
        assert(tstamp < event->timestamp);
        tstamp = event->timestamp;
        char c = tdb_get_item_value(db, event->items[0], &len)[0];
        assert(prev < c);
        prev = c;
    }
    assert(tdb_get_trail(cursor, 1) == 0);
    event = tdb_cursor_next(cursor);
    assert(event->num_items == 2);
    assert(event->timestamp == EVENTS2[2].time);
    assert(tdb_get_item_value(db, event->items[0], &len)[0] == *EVENTS2[2].value1);

    tdb_close(db);
}

int main(int argc, char** argv)
{
    empty_tdb_append(getenv("TDB_TMP_DIR"));
    mismatching_fields(getenv("TDB_TMP_DIR"));
    simple_append(getenv("TDB_TMP_DIR"));
    return 0;
}
