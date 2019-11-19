
#include <stdio.h>
#include <assert.h>

#include <traildb.h>
#include <tdb_io.h>

#include "tdb_test.h"

struct event{
    uint32_t time;
    char value[1];
};

static void basic_sort(const char *root)
{
    static uint8_t uuid[16];
    char path[TDB_MAX_PATH_SIZE];
    const char *fields[] = {"f1"};
    const uint64_t lengths[] = {1};
    char prev;
    uint64_t len, tstamp, i;

    struct event EVENTS[] = {
        {40,  "i" },
        {3,   "a" },
        {20,  "e" },
        {41,  "m" },
        {10,  "c" },
        {40,  "j" },
        {10,  "d" },
        {20,  "f" },
        {41,  "n" },
        {41,  "o" },
        {30,  "g" },
        {5,   "b" },
        {30,  "h" },
        {40,  "k" },
        {40,  "l" },
    };
    tdb_path(path, "%s.%u", root, 1);
    tdb_cons* c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, path, fields, 1) == 0);

    for (i = 0; i < sizeof(EVENTS)/sizeof(struct event); i++){
        const char *values[] = {EVENTS[i].value};
        assert(tdb_cons_add(c,
                            uuid,
                            EVENTS[i].time,
                            values,
                            lengths) == 0);
    }

    assert(tdb_cons_finalize(c) == 0);

    tdb* db = tdb_init();
    assert(tdb_open(db, path) == 0);
    tdb_cursor *cursor = tdb_cursor_new(db);
    assert(tdb_lexicon_size(db, 1) == 16);
    assert(tdb_num_trails(db) == 1);
    assert(tdb_get_trail(cursor, 0) == 0);
    tstamp = 0;
    prev = 0;
    /* we expect a sequence
        (5, a), (10, b), (10, c), ....
       i.e. both timestamaps and values are monotonically increasing,
       which we check below
    */
    const tdb_event *event;
    while ((event = tdb_cursor_next(cursor))){
        assert(tstamp <= event->timestamp);
        tstamp = event->timestamp;
        char c = tdb_get_item_value(db, event->items[0], &len)[0];
        assert(prev < c);
        prev = c;
    }

    tdb_close(db);
}


int main(int argc, char** argv)
{
    basic_sort(getenv("TDB_TMP_DIR"));
}
