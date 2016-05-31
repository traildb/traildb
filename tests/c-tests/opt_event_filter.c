
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <traildb.h>
#include <tdb_io.h>
#include "tdb_test.h"

int main(int argc, char** argv)
{
    char path[TDB_MAX_PATH_SIZE];
    static uint8_t uuid[16];
    const char *fields[] = {"a", "b"};
    const char *event1[] = {"foo", "bar"};
    const char *event2[] = {"foo", "sun"};
    const char *event3[] = {"foo", "bar"};
    const uint64_t lengths[] = {3, 3};
    tdb_opt_value value = {.value = 0};

    tdb_cons* c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, getenv("TDB_TMP_DIR"), fields, 2) == 0);

    assert(tdb_cons_add(c, uuid, 1, event1, lengths) == 0);
    assert(tdb_cons_add(c, uuid, 2, event2, lengths) == 0);
    assert(tdb_cons_add(c, uuid, 3, event3, lengths) == 0);

    assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);

    tdb* t = tdb_init();
    assert(tdb_open(t, getenv("TDB_TMP_DIR")) == 0);

    tdb_field field_b;
    assert(tdb_get_field(t, "b", &field_b) == 0);

    /* compare this to the materialized view below */
    assert(tdb_lexicon_size(t, field_b) == 3);

    struct tdb_event_filter *f = tdb_event_filter_new();
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_b, "bar", 3), 0) == 0);

    /* test get_opt (default should be NULL) */
    assert(tdb_get_opt(t, TDB_OPT_EVENT_FILTER, &value) == 0);
    assert(value.ptr == NULL);

    value.ptr = f;
    assert(tdb_set_opt(t, TDB_OPT_EVENT_FILTER, value) == 0);

    /* the cursor should have the filter set automatically */
    tdb_cursor *cursor = tdb_cursor_new(t);
    assert(tdb_get_trail(cursor, 0) == 0);
    assert(tdb_get_trail_length(cursor) == 2);

    /* test get_opt (real value) */
    assert(tdb_get_opt(t, TDB_OPT_EVENT_FILTER, &value) == 0);
    assert(value.ptr == f);

    /* test filtered tdb_cons_append (materialized view) */
    tdb_path(path, "%s/%u", getenv("TDB_TMP_DIR"), 1);
    c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, path, fields, 2) == 0);
    assert(tdb_cons_append(c, t) == 0);
    assert(tdb_cons_finalize(c) == 0);

    tdb_cons_close(c);
    tdb_close(t);
    tdb_cursor_free(cursor);
    tdb_event_filter_free(f);

    /* the new tdb should contain only two events that match the filter */
    t = tdb_init();
    assert(tdb_open(t, path) == 0);

    /*
    it is important the lexicon size is reduced: it shows
    that we used tdb_cons_append_subset_lexicon() instead of
    tdb_cons_append_full_lexicon() in tdb_cons.c properly.
    */
    assert(tdb_get_field(t, "b", &field_b) == 0);
    assert(tdb_lexicon_size(t, field_b) == 2);

    cursor = tdb_cursor_new(t);
    assert(tdb_get_trail(cursor, 0) == 0);
    assert(tdb_get_trail_length(cursor) == 2);

    tdb_close(t);
    tdb_cursor_free(cursor);

    return 0;
}
