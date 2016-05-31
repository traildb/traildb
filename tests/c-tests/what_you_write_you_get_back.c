#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <traildb.h>

#include "tdb_test.h"

#define NUM_EVENTS 3

static uint8_t uuid[16];

#define MAX_VALUE_SIZE  1000000

static char buffer1[MAX_VALUE_SIZE];
static char buffer2[MAX_VALUE_SIZE];
static char buffer3[MAX_VALUE_SIZE];

const uint64_t LENGTHS[] = {0, 1, 2, 1000, MAX_VALUE_SIZE};

int main(int argc, char** argv)
{
    uint64_t j, i = 0;
    tdb_field field;
    const char *fields[] = {"a", "b", "c"};
    const char *values[] = {buffer1, buffer2, buffer3};
    uint64_t lengths[3];

    tdb_cons* c = tdb_cons_init();
    assert(tdb_cons_open(c, getenv("TDB_TMP_DIR"), fields, 3) == 0);
    test_cons_settings(c);

    for (i = 0; i < sizeof(LENGTHS) / sizeof(LENGTHS[0]); i++){
        lengths[0] = lengths[1] = lengths[2] = LENGTHS[i];
        if (LENGTHS[i] > 0)
            memset(buffer1, i, LENGTHS[i]);
        memset(buffer2, i + 10, LENGTHS[i]);
        memset(buffer3, i + 20, LENGTHS[i]);
        memset(uuid, i, sizeof(uuid));
        for (j = 0; j < NUM_EVENTS; j++)
            tdb_cons_add(c, uuid, i, values, lengths);
    }
    assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);

    tdb* t = tdb_init();
    assert(tdb_open(t, getenv("TDB_TMP_DIR")) == 0);
    tdb_cursor *cursor = tdb_cursor_new(t);

    for (i = 0; i < sizeof(LENGTHS) / sizeof(LENGTHS[0]); i++){
        const tdb_event *event;
        uint64_t trail_id;

        memset(uuid, i, sizeof(uuid));
        assert(tdb_get_trail_id(t, uuid, &trail_id) == 0);
        assert(tdb_get_trail(cursor, trail_id) == 0);

        if (LENGTHS[i] > 0)
            memset(buffer1, i, LENGTHS[i]);
        memset(buffer2, i + 10, LENGTHS[i]);
        memset(buffer3, i + 20, LENGTHS[i]);

        for (j = 0; (event = tdb_cursor_next(cursor)); j++){
            assert(event->timestamp == i);
            assert(event->num_items == 3);
            for (field = 0; field < 3; field++){
                uint64_t len;
                const char *p = tdb_get_item_value(t,
                                                   event->items[field],
                                                   &len);
                assert(p != NULL);
                assert(len == LENGTHS[i]);
                assert(memcmp(values[field], p, len) == 0);
            }
        }
        assert(j == NUM_EVENTS);
    }

    tdb_cursor_free(cursor);
    tdb_close(t);
    return 0;
}

