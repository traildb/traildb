#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <traildb.h>
#include "tdb_test.h"

#define MAX_NUMBER 1000

static uint64_t count_events(const tdb *t, tdb_cursor *cursor)
{
    uint64_t trail_id, count = 0;
    for (trail_id = 0; trail_id < tdb_num_trails(t); trail_id++){
        assert(tdb_get_trail(cursor, trail_id) == 0);
        count += tdb_get_trail_length(cursor);
    }
    return count;
}

static uint64_t check_odd_or_even(const tdb *t, tdb_cursor *cursor, int expect_odd)
{
    uint64_t trail_id, count = 0;
    tdb_field f_is_odd;
    assert(tdb_get_field(t, "is_odd", &f_is_odd) == 0);

    for (trail_id = 0; trail_id < tdb_num_trails(t); trail_id++){
        const tdb_event *event;
        assert(tdb_get_trail(cursor, trail_id) == 0);
        while ((event = tdb_cursor_next(cursor))){
            uint32_t val;
            uint64_t tmp;
            memcpy(&val, tdb_get_item_value(t, event->items[0], &tmp), 4);
            assert((val & 1) == expect_odd);
            ++count;
        }
    }
    return count;
}

int main(int argc, char **argv)
{
    static uint8_t uuid[16];
    const char *fields[] = {"number", "is_odd"};
    uint32_t i = 0;

    tdb_cons* c = tdb_cons_init();
    assert(tdb_cons_open(c, argv[1], fields, 2) == 0);

    for (i = 0; i < MAX_NUMBER; i++){
        char yes = 't';
        const char *event[2];
        uint64_t lenghts[] = {4, 0};
        event[0] = (const char*)&i;
        if (i & 1){
            event[1] = &yes;
            lenghts[1] = 1;
        }
        memcpy(uuid, &i, 4);
        assert(tdb_cons_add(c, uuid, 1, event, lenghts) == 0);
    }

    assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);

    tdb* t = tdb_init();
    assert(tdb_open(t, argv[1]) == 0);

    tdb_field f_number, f_is_odd;
    assert(tdb_get_field(t, "number", &f_number) == 0);
    assert(tdb_get_field(t, "is_odd", &f_is_odd) == 0);

    tdb_cursor *cursor = tdb_cursor_new(t);

    /* are odd=t actually all odd numbers? */
    struct tdb_event_filter *f = tdb_event_filter_new();
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, f_is_odd, "t", 1), 0) == 0);
    tdb_cursor_set_event_filter(cursor, f);
    assert(check_odd_or_even(t, cursor, 1) == MAX_NUMBER / 2);

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* are NOT odd=t actually all even numbers? */
    assert(tdb_event_filter_add_term(f, tdb_get_item(t, f_is_odd, "t", 1), 1) == 0);
    tdb_cursor_set_event_filter(cursor, f);
    assert(check_odd_or_even(t, cursor, 0) == MAX_NUMBER / 2);

    tdb_event_filter_free(f);
    f = tdb_event_filter_new();

    /* create a large union of all numbers, one number at a time.
       Check that the number of results is expected */
    for (i = 0; i < MAX_NUMBER; i++){
        assert(tdb_event_filter_add_term(f, tdb_get_item(t, f_number, (const char*)&i, 4), 0) == 0);
        tdb_cursor_set_event_filter(cursor, f);
        assert(count_events(t, cursor) == i + 1);
    }

    /* a long conjunction of empty clauses is valid but matches nothing */
    tdb_event_filter_free(f);
    f = tdb_event_filter_new();
    for (i = 0; i < 1000; i++)
        tdb_event_filter_new_clause(f);

    tdb_cursor_set_event_filter(cursor, f);
    assert(count_events(t, cursor) == 0);

    tdb_close(t);
    tdb_cursor_free(cursor);
    return 0;
}
