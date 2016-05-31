#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <traildb.h>
#include "tdb_test.h"

#define NUM_EVENTS 1234

static void simple_case(const char *root)
{
    static uint8_t uuid[16];
    const char *fields[] = {"a"};
    const char *values[] = {"foobar"};
    uint64_t lengths[] = {6};
    uint64_t i;
    tdb_opt_value value = {.value = 0};

    tdb_cons* c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, root, fields, 1) == 0);
    for (i = 0; i < NUM_EVENTS; i++)
        assert(tdb_cons_add(c, uuid, i, values, lengths) == 0);
    assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);

    tdb* t = tdb_init();
    assert(tdb_open(t, root) == 0);

    /* basic get_opt */
    assert(tdb_get_opt(t, 0, &value) == TDB_ERR_UNKNOWN_OPTION);
    assert(tdb_get_opt(t, TDB_OPT_ONLY_DIFF_ITEMS, &value) == 0);
    assert(value.value == TDB_FALSE.value);

    /* basic set_opt */
    assert(tdb_set_opt(t, 0, value) == TDB_ERR_UNKNOWN_OPTION);
    assert(tdb_set_opt(t, TDB_OPT_ONLY_DIFF_ITEMS, TDB_TRUE) == 0);
    assert(tdb_get_opt(t, TDB_OPT_ONLY_DIFF_ITEMS, &value) == 0);
    assert(value.value == TDB_TRUE.value);

    tdb_cursor *cursor = tdb_cursor_new(t);
    assert(tdb_get_trail(cursor, 0) == 0);
    const tdb_event *event = tdb_cursor_next(cursor);
    assert(event->timestamp == 0);
    assert(event->num_items == 1);
    assert(event->items[0] == tdb_get_item(t, 1, "foobar", 6));

    for (i = 0; (event = tdb_cursor_next(cursor)); i++){
        assert(event->timestamp == i + 1);
        /*
        this is the point of OPT_ONLY_DIFF_ITEMS:
        since the value doesn't change, no items are returned
        */
        assert(event->num_items == 0);
    }
    assert(i == NUM_EVENTS - 1);

    tdb_close(t);
    tdb_cursor_free(cursor);
}

static void two_field_case(const char *root)
{
    static uint8_t uuid[16];
    const char *fields[] = {"a", "b"};
    char val1 = 0;
    char val2 = 0;
    const char *values[] = {&val1, &val2};
    uint64_t lengths[] = {1, 1};
    uint64_t i;
    const tdb_event *event;

    tdb_cons* c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, root, fields, 2) == 0);
    for (i = 0; i < NUM_EVENTS; i++){
        val1 = i;
        assert(tdb_cons_add(c, uuid, i, values, lengths) == 0);
    }
    val2 = 1;
    for (i = 0; i < NUM_EVENTS; i++){
        val1 = i;
        assert(tdb_cons_add(c, uuid, NUM_EVENTS + i, values, lengths) == 0);
    }
    assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);

    tdb* t = tdb_init();
    assert(tdb_open(t, root) == 0);
    assert(tdb_set_opt(t, TDB_OPT_ONLY_DIFF_ITEMS, TDB_TRUE) == 0);
    tdb_cursor *cursor = tdb_cursor_new(t);
    assert(tdb_get_trail(cursor, 0) == 0);

    /* the first field changes at every event, the second field changes twice:
       in the beginning and at the midpoint */
    for (i = 0; (event = tdb_cursor_next(cursor)); i++){
        assert(event->timestamp == i);
        if (event->num_items == 2)
            assert(i == 0 || i == NUM_EVENTS);
        else{
            assert(event->num_items == 1);
            assert(tdb_item_field(event->items[0]) == 1);
        }
    }
    assert(i == NUM_EVENTS * 2);

    tdb_close(t);
    tdb_cursor_free(cursor);
}

int main(int argc, char** argv)
{
    simple_case(getenv("TDB_TMP_DIR"));
    two_field_case(getenv("TDB_TMP_DIR"));
    return 0;
}
