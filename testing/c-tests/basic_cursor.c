
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <traildb.h>
#include "tdb_test.h"

#define NUM_TRAILS 1000

int main(int argc, char** argv)
{
    static uint8_t uuid[16];
    const char *fields[] = {"a", "b"};
    uint64_t lengths[] = {1, 1};
    char val1 = 0;
    char val2 = 0;
    char *values[] = {&val1, &val2};
    uint64_t i, j, num_events = 0;

    tdb_cons* c = tdb_cons_init();
    test_cons_settings(c);

    assert(tdb_cons_open(c, argv[1], fields, 2) == 0);

    for (i = 1; i < NUM_TRAILS + 1; i++){
        int x = (i % 5) + 1;
        memcpy(uuid, &i, 8);
        /* make sure at least some trails are longer than
           CURSOR_BUFFER_NUM_EVENTS */
        for (j = 0; j < i * 10; j++){
            /* let's add some complexity in edge encoding */
            val1 = (j % 10) ? 1: 0;
            val2 = (j % x) ? 1: 0;
            assert(tdb_cons_add(c, uuid, j, (const char**)values, lengths) == 0);
            ++num_events;
        }
    }
    assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);

    tdb* t = tdb_init();
    assert(tdb_open(t, argv[1]) == 0);
    tdb_cursor *cursor = tdb_cursor_new(t);

    assert(tdb_num_events(t) == num_events);

    for (num_events = 0, i = 0; i < NUM_TRAILS; i++){
        const tdb_event *event;
        int x = ((i + 1) % 5) + 1;

        assert(tdb_get_trail(cursor, i) == 0);

        /* test tdb_get_trail_length */

        uint64_t n = tdb_get_trail_length(cursor);
        assert(n == (i + 1) * 10);
        num_events += n;
        assert(tdb_get_trail_length(cursor) == 0);

        /* test cursor */

        assert(tdb_get_trail(cursor, i) == 0);
        j = 0;
        while ((event = tdb_cursor_next(cursor))){
            const char *v;
            uint64_t len;

            assert(event->timestamp == j);
            assert(event->num_items == 2);
            val1 = (j % 10) ? 1: 0;
            val2 = (j % x) ? 1: 0;

            v = tdb_get_item_value(t, event->items[0], &len);
            assert(len == 1);
            assert(*v == val1);

            v = tdb_get_item_value(t, event->items[1], &len);
            assert(len == 1);
            assert(*v == val2);

            ++j;
        }
    }

    assert(tdb_num_events(t) == num_events);

    tdb_cursor_free(cursor);
    tdb_close(t);
    return 0;
}
