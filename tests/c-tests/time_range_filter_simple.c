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
    const uint64_t start_time = 1;
    const uint64_t end_time = 4;
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

    tdb_cursor *cursor = tdb_cursor_new(t);
    struct tdb_event_filter *f = tdb_event_filter_new();

    /* SIMPLE TIME-RANGE QUERIES */

    /* empty clause shouldn't match to any event */
    assert_num_events(cursor, f, 0);

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* time range with single timestep */
    assert(tdb_event_filter_add_time_range(f, start_time, start_time) == 0);
    assert_num_events(cursor, f, 0);

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* test all events in range */
    assert(tdb_event_filter_add_time_range(f, start_time - 1, end_time + 1) == 0);
    assert_num_events(cursor, f, 4);

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* test that end is exclusive */
    assert(tdb_event_filter_add_time_range(f, start_time - 1, end_time) == 0);
    assert_num_events(cursor, f, 3);

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* test that end is inclusive */
    assert(tdb_event_filter_add_time_range(f, start_time, end_time + 1) == 0);
    assert_num_events(cursor, f, 4);

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* single out one event in middle */
    assert(tdb_event_filter_add_time_range(f, 2, 3) == 0);
    assert_num_events(cursor, f, 1);

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* take front half of events */
    assert(tdb_event_filter_add_time_range(f, start_time, 3) == 0);
    assert_num_events(cursor, f, 2);

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* take last half of events */
    assert(tdb_event_filter_add_time_range(f, 3, end_time + 1) == 0);
    assert_num_events(cursor, f, 2);

    tdb_event_filter_free(f);

    /* OR'ing time ranges */
    f = tdb_event_filter_new();

    /* overlapping time-range filters that cover entire range */
    assert(tdb_event_filter_add_time_range(f, start_time - 1, end_time + 1) == 0);
    assert(tdb_event_filter_add_time_range(f, start_time - 1, end_time + 1) == 0);
    assert_num_events(cursor, f, 4);
    
    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* overlap entire range with single event */
    assert(tdb_event_filter_add_time_range(f, start_time - 1, end_time + 1) == 0);
    assert(tdb_event_filter_add_time_range(f, 2, 3) == 0);
    assert_num_events(cursor, f, 4);

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();
    
    /* two non-overlapping ranges (first and last items) */
    assert(tdb_event_filter_add_time_range(f, start_time, start_time + 1) == 0);
    assert(tdb_event_filter_add_time_range(f, end_time, end_time + 1) == 0);
    assert_num_events(cursor, f, 2);

    tdb_event_filter_free(f);

    /* AND'ing time ranges */
    f = tdb_event_filter_new();

    /* overlapping time-range filters that cover entire range */
    assert(tdb_event_filter_add_time_range(f, start_time - 1, end_time + 1) == 0);
    assert(tdb_event_filter_new_clause(f) == 0);
    assert(tdb_event_filter_add_time_range(f, start_time - 1, end_time + 1) == 0);
    assert_num_events(cursor, f, 4);
    
    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* overlap entire range with single event */
    assert(tdb_event_filter_add_time_range(f, start_time - 1, end_time + 1) == 0);
    assert(tdb_event_filter_new_clause(f) == 0);
    assert(tdb_event_filter_add_time_range(f, 2, 3) == 0);
    assert_num_events(cursor, f, 1);

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();
    
    /* two non-overlapping ranges (first and last items) */
    assert(tdb_event_filter_add_time_range(f, start_time, start_time + 1) == 0);
    assert(tdb_event_filter_new_clause(f) == 0);
    assert(tdb_event_filter_add_time_range(f, end_time, end_time + 1) == 0);
    assert_num_events(cursor, f, 0);
   
    tdb_event_filter_free(f);

    tdb_close(t);
    tdb_cursor_free(cursor);
    return 0;
}
