#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <traildb.h>
#include "tdb_test.h"

#define NUM_EVENTS 11

int main(int argc, char** argv)
{
    static uint8_t uuid[16];
    const char *fields[] = {"a", "b"};
    const char *values[] = {"foo", "ba"};
    const uint64_t lengths[] = {3, 2};
    tdb_opt_value val = {.value = UINT64_MAX};
    uint64_t i;
    tdb_cons* c = tdb_cons_init();
    assert(tdb_cons_open(c, argv[1], fields, 2) == 0);

    assert(tdb_cons_set_opt(c, TDB_OPT_ONLY_DIFF_ITEMS, TDB_TRUE) ==
           TDB_ERR_UNKNOWN_OPTION);

    assert(tdb_cons_set_opt(c, TDB_OPT_CONS_OUTPUT_FORMAT, val) ==
           TDB_ERR_INVALID_OPTION_VALUE);

    assert(tdb_cons_set_opt(c,
                            TDB_OPT_CONS_OUTPUT_FORMAT,
                            opt_val(TDB_OPT_CONS_OUTPUT_FORMAT_PACKAGE)) == 0);

    assert(tdb_cons_get_opt(c, TDB_OPT_ONLY_DIFF_ITEMS, &val) ==
           TDB_ERR_UNKNOWN_OPTION);

    assert(tdb_cons_get_opt(c, TDB_OPT_CONS_OUTPUT_FORMAT, &val) == 0);
    assert(val.value == TDB_OPT_CONS_OUTPUT_FORMAT_PACKAGE);

    for (i = 0; i < NUM_EVENTS; i++)
       assert(tdb_cons_add(c, uuid, i, values, lengths) == 0);

    assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);
    tdb* t = tdb_init();
    assert(tdb_open(t, argv[1]) == 0);

    tdb_cursor *cursor = tdb_cursor_new(t);
    const tdb_event *event;
    assert(tdb_get_trail(cursor, 0) == 0);

    for (i = 0; (event = tdb_cursor_next(cursor)); i++){
        assert(event->timestamp == i);
        assert(event->num_items == 2);
    }
    assert(i == NUM_EVENTS);
    tdb_close(t);
    tdb_cursor_free(cursor);
    return 0;
}
