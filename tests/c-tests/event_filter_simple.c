#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <traildb.h>
#include "tdb_test.h"

/* this is a macro so assert() can report the line number correctly */
#define assert_num_events(c, f, expected){\
    tdb_cursor_set_event_filter(c, f);\
    assert(tdb_get_trail(c, 0) == 0);\
    assert(tdb_get_trail_length(c) == expected);\
}

int main(int argc, char **argv)
{
    static uint8_t uuid[16];
    const char *fields[] = {"a", "b", "c"};
    const char *event1[] = {"foo", "bar", ""};
    const char *event2[] = {"foo", "sun", ""};
    const char *event3[] = {"foo", "run", ""};
    const char *event4[] = {"", "sun", "nam"};
    const uint64_t lengths1[] = {3, 3, 0};
    const uint64_t lengths2[] = {0, 3, 3};
    int i;

    tdb_cons* c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, getenv("TDB_TMP_DIR"), fields, 3) == 0);

    assert(tdb_cons_add(c, uuid, 1, event1, lengths1) == 0);
    assert(tdb_cons_add(c, uuid, 2, event2, lengths1) == 0);
    assert(tdb_cons_add(c, uuid, 3, event3, lengths1) == 0);
    assert(tdb_cons_add(c, uuid, 4, event4, lengths2) == 0);

    assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);

    tdb* t = tdb_init();
    assert(tdb_open(t, getenv("TDB_TMP_DIR")) == 0);

    tdb_field field_a, field_b, field_c;
    assert(tdb_get_field(t, "a", &field_a) == 0);
    assert(tdb_get_field(t, "b", &field_b) == 0);
    assert(tdb_get_field(t, "c", &field_c) == 0);

    tdb_cursor *cursor = tdb_cursor_new(t);
    struct tdb_event_filter *f = tdb_event_filter_new();

    /* SIMPLE OR-QUERIES */

    /* empty clause shouldn't match to any event */
    assert_num_events(cursor, f, 0);

    /* events with a=foo: 1, 2, 3 */
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_a, "foo", 3), 0) == 0);
    assert_num_events(cursor, f, 3);

    /* events with a=foo OR b=sun: 1, 2, 3, 4 */
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_b, "sun", 3), 0) == 0);
    assert_num_events(cursor, f, 4);

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* empty values in proper fields should be handled as any other values */
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_c, "", 0), 0) == 0);
    assert_num_events(cursor, f, 3);

    /* adding another fully overlapping (duplicate) term shouldn't change anything */
    for (i = 0; i < 1000; i++)
        assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_a, "foo", 3), 0) == 0);
    assert_num_events(cursor, f, 3);

    /* SIMPLE NEGATIONS */

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* NOT a=foo matches one event */
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_a, "foo", 3), 1) == 0);
    assert_num_events(cursor, f, 1);

    /* NOT c="" matches one event - empty fields are ok */
    tdb_event_filter_free(f);
    f = tdb_event_filter_new();
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_c, "", 0), 1) == 0);
    assert_num_events(cursor, f, 1);

    /* NOT b=sun OR a=foo matches three events */
    tdb_event_filter_free(f);
    f = tdb_event_filter_new();
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_b, "sun", 3), 1) == 0);
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_a, "foo", 3), 0) == 0);
    assert_num_events(cursor, f, 3);

    /* SIMPLE AND-QUERIES */

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* (a=foo) AND () results to nothing */
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_a, "foo", 3), 0) == 0);
    tdb_event_filter_new_clause(f);
    assert_num_events(cursor, f, 0);

    /* (b=sun) AND (c=nam) matches only one event */
    tdb_event_filter_free(f);
    f = tdb_event_filter_new();
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_b, "sun", 3), 0) == 0);
    tdb_event_filter_new_clause(f);
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_c, "nam", 3), 0) == 0);
    assert_num_events(cursor, f, 1);

    /* (b=sun) AND (c=nam OR a='') matches the same event */
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_a, "", 0), 0) == 0);
    assert_num_events(cursor, f, 1);

    /* (b=sun) AND (NOT a=foo) */
    tdb_event_filter_free(f);
    f = tdb_event_filter_new();
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_b, "sun", 3), 0) == 0);
    tdb_event_filter_new_clause(f);
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_a, "foo", 3), 0) == 0);
    assert_num_events(cursor, f, 1);

    /* (a=foo) AND (b=bar) AND (c='') matches one event */
    tdb_event_filter_free(f);
    f = tdb_event_filter_new();
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_a, "foo", 3), 0) == 0);
    tdb_event_filter_new_clause(f);
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_b, "bar", 3), 0) == 0);
    tdb_event_filter_new_clause(f);
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, field_c, "", 0), 0) == 0);
    assert_num_events(cursor, f, 1);

    /* SPECIAL NULL VALUE */

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* null event matches nothing */
    assert(tdb_event_filter_add_term(f, 0, 0) == 0);
    assert_num_events(cursor, f, 0);

    /* negation of null matches everything */
    assert(tdb_event_filter_add_term(f, 0, 1) == 0);
    assert_num_events(cursor, f, 4);

    /* CONVENIENCE FUNCTIONS */
    tdb_event_filter_free(f);
    f = tdb_event_filter_new_match_all();
    assert_num_events(cursor, f, 4);

    tdb_event_filter_free(f);
    f = tdb_event_filter_new_match_none();
    assert_num_events(cursor, f, 0);

    /* UNSETTING */

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();
    tdb_cursor_set_event_filter(cursor, f);
    assert(tdb_get_trail(cursor, 0) == 0);
    assert(tdb_get_trail_length(cursor) == 0);
    tdb_cursor_unset_event_filter(cursor);
    assert(tdb_get_trail(cursor, 0) == 0);
    assert(tdb_get_trail_length(cursor) == 4);

    /*
       ONLY_DIFF
       filters for ONLY_DIFF mode are not supported:
       The sematics are too ill-defined
    */
    tdb_event_filter_free(f);
    f = tdb_event_filter_new();
    tdb_cursor_free(cursor);
    assert(tdb_set_opt(t, TDB_OPT_ONLY_DIFF_ITEMS, TDB_TRUE) == 0);
    cursor = tdb_cursor_new(t);
    assert(tdb_cursor_set_event_filter(cursor, f) == TDB_ERR_ONLY_DIFF_FILTER);

    tdb_close(t);
    tdb_cursor_free(cursor);
    return 0;
}
