
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <traildb.h>
#include <util.h>

static void empty_tdb_append(const char *root)
{
    char path[MAX_PATH_SIZE];
    const char *fields[] = {};

    make_path(path, "%s.%u", root, 1);
    tdb_cons* c1 = tdb_cons_new(path, fields, 0);
    assert(c1 != NULL);
    assert(tdb_cons_finalize(c1, 0) == 0);
    tdb* t1 = tdb_open(path);
    assert(t1 != NULL);

    make_path(path, "%s.%u", root, 2);
    tdb_cons* c2 = tdb_cons_new(path, fields, 0);
    assert(c2 != NULL);
    assert(tdb_cons_append(c2, t1) == 0);
    assert(tdb_cons_finalize(c2, 0) == 0);

    tdb* t2 = tdb_open(path);
    assert(t2 != NULL);
    assert(tdb_num_trails(t2) == 0);
    assert(tdb_num_fields(t2) == 1);
}

static void mismatching_fields(const char *root)
{
    char path[MAX_PATH_SIZE];
    const char *fields1[] = {"a", "b", "c"};
    const char *fields2[] = {"d", "e"};

    make_path(path, "%s.%u", root, 1);
    tdb_cons* c1 = tdb_cons_new(path, fields1, 2);
    assert(c1 != NULL);
    assert(tdb_cons_finalize(c1, 0) == 0);
    tdb* t1 = tdb_open(path);
    assert(t1 != NULL);

    /* mismatching number of fields - this should fail */
    make_path(path, "%s.%u", root, 2);
    tdb_cons* c2 = tdb_cons_new(path, fields1, 3);
    assert(c2 != NULL);
    assert(tdb_cons_append(c2, t1) == -1);
    assert(tdb_cons_finalize(c2, 0) == 0);

    /* mismatching field names - this should fail */
    make_path(path, "%s.%u", root, 2);
    c2 = tdb_cons_new(path, fields2, 2);
    assert(c2 != NULL);
    assert(tdb_cons_append(c2, t1) == -2);
    assert(tdb_cons_finalize(c2, 0) == 0);
}

struct event{
    uint32_t time;
    char value1[1];
    char value2[1];
};

static void simple_append(const char *root)
{
    static uint8_t uuid[16];
    char path[MAX_PATH_SIZE];
    const char *fields[] = {"f1", "f2"};
    const uint64_t lengths[] = {1, 1};
    tdb_item *items;
    char prev;
    uint64_t len, tstamp, i, n, items_len = 0;

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
    make_path(path, "%s.%u", root, 1);
    tdb_cons* c = tdb_cons_new(path, fields, 2);

    for (i = 0; i < 3; i++){
        const char *values[] = {EVENTS1[i].value1, EVENTS1[i].value2};
        assert(tdb_cons_add(c,
                            uuid,
                            EVENTS1[i].time,
                            values,
                            lengths) == 0);
    }

    assert(tdb_cons_finalize(c, 0) == 0);
    tdb* db = tdb_open(path);
    assert(db != NULL);

    make_path(path, "%s.%u", root, 2);
    c = tdb_cons_new(path, fields, 2);

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

    assert(tdb_cons_finalize(c, 0) == 0);
    tdb_close(db);

    db = tdb_open(path);
    assert(db != NULL);
    assert(tdb_lexicon_size(db, 1) == 6);
    assert(tdb_lexicon_size(db, 2) == 4);
    assert(tdb_num_trails(db) == 2);
    assert(tdb_get_trail(db, 0, &items, &items_len, &n, 0) == 0);
    assert(n == 20);
    tstamp = 0;
    prev = 0;
    /* we expect a sequence
        (5, a), (10, b), (20, c), (30, d), (40, e)
       i.e. both timestamaps and values are monotonically increasing,
       which we check below
    */
    for (i = 0; i < n;){
        assert(tstamp < items[i]);
        tstamp = items[i++];
        char c = tdb_get_item_value(db, items[i++], &len)[0];
        assert(prev < c);
        prev = c;
        i += 2;
    }
    assert(tdb_get_trail(db, 1, &items, &items_len, &n, 0) == 0);
    assert(items[0] == 100);
    assert(tdb_get_item_value(db, items[1], &len)[0] == 'a');

    free(items);
    tdb_close(db);
}

int main(int argc, char** argv)
{
    empty_tdb_append(argv[1]);
    mismatching_fields(argv[1]);
    simple_append(argv[1]);
    return 0;
}
