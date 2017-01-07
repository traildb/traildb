
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <traildb.h>
#include <tdb_io.h>
#include "tdb_test.h"

#define NUM_TRAILS 1000

static tdb *create_test_tdb()
{
    static uint8_t uuid[16];
    const char *fields[] = {"a"};
    const char *values1[] = {"first"};
    const uint64_t lengths1[] = {5};
    const char *values2[] = {"second"};
    const uint64_t lengths2[] = {6};
    uint64_t i;

    tdb_cons* c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, getenv("TDB_TMP_DIR"), fields, 1) == 0);
    for (i = 0; i < NUM_TRAILS; i++){
        memcpy(uuid, &i, 8);
        tdb_cons_add(c, uuid, 0, values1, lengths1);
        tdb_cons_add(c, uuid, 0, values2, lengths2);
        tdb_cons_add(c, uuid, 0, values2, lengths2);
    }
    assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);

}

static tdb *open_test_tdb()
{
    tdb* t = tdb_init();
    assert(tdb_open(t, getenv("TDB_TMP_DIR")) == 0);
    return t;
}

static struct tdb_event_filter *create_event_filter(const tdb *t, int do_second)
{
    tdb_field field_a;
    assert(tdb_get_field(t, "a", &field_a) == 0);
    struct tdb_event_filter *f = tdb_event_filter_new();
    if (do_second)
        assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_a, "second", 6), 0) == 0);
    else
        assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_a, "first", 5), 0) == 0);

    return f;
}

static void test_trail_opt()
{
    tdb *t = open_test_tdb();
    struct tdb_event_filter *f = create_event_filter(t, 0);
    tdb_opt_value tmp, value = {.ptr = f};
    uint64_t i;

    /* only TDB_OPT_EVENT_FILTER is supported currently */
    assert(tdb_set_trail_opt(t, 0, TDB_OPT_ONLY_DIFF_ITEMS, value) == TDB_ERR_UNKNOWN_OPTION);
    /* invalid trail ids are no good */
    assert(tdb_set_trail_opt(t, NUM_TRAILS, TDB_OPT_EVENT_FILTER, value) == TDB_ERR_INVALID_TRAIL_ID);
    /* test set and get */
    assert(tdb_set_trail_opt(t, 0, TDB_OPT_EVENT_FILTER, value) == 0);
    assert(tdb_get_trail_opt(t, 0, TDB_OPT_EVENT_FILTER, &tmp) == 0);
    assert(tmp.ptr == value.ptr);
    for (i = 1; i < NUM_TRAILS; i++){
        assert(tdb_get_trail_opt(t, i, TDB_OPT_EVENT_FILTER, &tmp) == 0);
        assert(tmp.ptr == NULL);
    }
    /* test delete */
    tmp.ptr = NULL;
    assert(tdb_set_trail_opt(t, 0, TDB_OPT_EVENT_FILTER, tmp) == 0);
    assert(tdb_get_trail_opt(t, 0, TDB_OPT_EVENT_FILTER, &tmp) == 0);
    assert(tmp.ptr == NULL);

    tdb_event_filter_free(f);
    tdb_close(t);
}

static void test_queries()
{
    uint64_t i;
    tdb *t = open_test_tdb();
    struct tdb_event_filter *f = create_event_filter(t, 0);
    tdb_opt_value tmp, value = {.ptr = f};
    tdb_cursor *c = tdb_cursor_new(t);
    /* a simple trail filter */
    assert(tdb_set_trail_opt(t, 1, TDB_OPT_EVENT_FILTER, value) == 0);
    assert(tdb_get_trail(c, 0) == 0);
    assert(tdb_get_trail_length(c) == 3);
    assert(tdb_get_trail(c, 1) == 0);
    assert(tdb_get_trail_length(c) == 1);
    /* delete trail filter */
    tmp.ptr = NULL;
    assert(tdb_set_trail_opt(t, 1, TDB_OPT_EVENT_FILTER, tmp) == 0);
    assert(tdb_get_trail(c, 1) == 0);
    assert(tdb_get_trail_length(c) == 3);
    /* test cascading: tdb-level & trail-level */
    assert(tdb_set_opt(t, TDB_OPT_EVENT_FILTER, value) == 0);
    struct tdb_event_filter *f1 = create_event_filter(t, 1);
    tmp.ptr = f1;
    assert(tdb_set_trail_opt(t, 0, TDB_OPT_EVENT_FILTER, tmp) == 0);
    assert(tdb_set_trail_opt(t, NUM_TRAILS - 1, TDB_OPT_EVENT_FILTER, tmp) == 0);
    assert(tdb_get_trail(c, 0) == 0);
    assert(tdb_get_trail_length(c) == 2);
    for (i = 1; i < NUM_TRAILS - 1; i++){
        assert(tdb_get_trail(c, i) == 0);
        assert(tdb_get_trail_length(c) == 1);
    }
    assert(tdb_get_trail(c, NUM_TRAILS - 1) == 0);
    assert(tdb_get_trail_length(c) == 2);

    /* test cascading: cursor-level overrides all */
    struct tdb_event_filter *f0 = tdb_event_filter_new();
    assert(tdb_cursor_set_event_filter(c, f0) == 0);
    for (i = 0; i < NUM_TRAILS; i++){
        tdb_get_trail(c, i);
        assert(tdb_get_trail_length(c) == 0);
    }
    tdb_cursor_unset_event_filter(c);
    /* back to normal after unsetting */
    assert(tdb_get_trail(c, 0) == 0);
    assert(tdb_get_trail_length(c) == 2);
    for (i = 1; i < NUM_TRAILS - 1; i++){
        assert(tdb_get_trail(c, i) == 0);
        assert(tdb_get_trail_length(c) == 1);
    }
    assert(tdb_get_trail(c, NUM_TRAILS - 1) == 0);
    assert(tdb_get_trail_length(c) == 2);
}

static void test_whitelisting()
{
    uint64_t i;
    tdb *t = open_test_tdb();
    struct tdb_event_filter *none = tdb_event_filter_new_match_none();
    tdb_opt_value value = {.ptr = none};
    /* blacklist everything */
    assert(tdb_set_opt(t, TDB_OPT_EVENT_FILTER, value) == 0);
    tdb_cursor *c = tdb_cursor_new(t);
    for (i = 0; i < NUM_TRAILS; i++){
        assert(tdb_get_trail(c, i) == 0);
        assert(tdb_get_trail_length(c) == 0);
    }
    /* whitelist two trails, add a custom filter to a third */
    struct tdb_event_filter *all = tdb_event_filter_new_match_all();
    struct tdb_event_filter *f = create_event_filter(t, 0);
    value.ptr = all;
    assert(tdb_set_trail_opt(t, 100, TDB_OPT_EVENT_FILTER, value) == 0);
    assert(tdb_set_trail_opt(t, 200, TDB_OPT_EVENT_FILTER, value) == 0);
    value.ptr = f;
    assert(tdb_set_trail_opt(t, 300, TDB_OPT_EVENT_FILTER, value) == 0);
    for (i = 0; i < NUM_TRAILS; i++){
        assert(tdb_get_trail(c, i) == 0);
        if (i == 100 || i == 200)
            assert(tdb_get_trail_length(c) == 3);
        else if (i == 300)
            assert(tdb_get_trail_length(c) == 1);
        else
            assert(tdb_get_trail_length(c) == 0);
    }
}

static void test_append()
{
    uint64_t TRAIL_ID = 500;
    tdb *t = open_test_tdb();
    struct tdb_event_filter *f1 = tdb_event_filter_new();
    struct tdb_event_filter *f2 = create_event_filter(t, 0);
    tdb_opt_value value = {.ptr = f1};
    assert(tdb_set_opt(t, TDB_OPT_EVENT_FILTER, value) == 0);
    value.ptr = f2;
    assert(tdb_set_trail_opt(t, TRAIL_ID, TDB_OPT_EVENT_FILTER, value) == 0);

    char path[TDB_MAX_PATH_SIZE];
    const char *fields[] = {"a"};
    tdb_path(path, "%s.1", getenv("TDB_TMP_DIR"));
    tdb_cons* c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, path, fields, 1) == 0);
    assert(tdb_cons_append(c, t) == 0);
    assert(tdb_cons_finalize(c) == 0);

    tdb *t1 = tdb_init();
    assert(tdb_open(t1, path) == 0);
    assert(tdb_num_trails(t1) == 1);
    assert(memcmp(tdb_get_uuid(t1, 0), tdb_get_uuid(t, TRAIL_ID), 16) == 0);
}

int main(int argc, char** argv)
{
    create_test_tdb();

    test_trail_opt();
    test_queries();
    test_whitelisting();
    test_append();

    return 0;
}

