
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <traildb.h>
#include <tdb_io.h>

#include "tdb_test.h"

#define NUM_COMMON_TRAILS 10
#define MAX_TRAIL_LENGTH 100000

static tdb *create_db(const char *root, uint32_t id, uint32_t len)
{
    char path[TDB_MAX_PATH_SIZE];
    const char *fields[] = {"id"};
    const char *values[] = {(const char*)&id};
    uint64_t lengths[] = {4};
    uint32_t i, j;
    static uint8_t uuid[16];
    uint64_t timestamp = 100;

    tdb_path(path, "%s.%u", root, id);
    tdb_cons* cons = tdb_cons_init();
    test_cons_settings(cons);
    assert(tdb_cons_open(cons, path, fields, 1) == 0);

    for (i = 1000; i < 1000 + NUM_COMMON_TRAILS; i++){
        memcpy(uuid, &i, 4);
        for (timestamp = 1000, j = 0; j < len; j++){
            assert(tdb_cons_add(cons, uuid, timestamp, values, lengths) == 0);
            timestamp += id;
        }
    }
    assert(tdb_cons_finalize(cons) == 0);
    tdb_cons_close(cons);
    tdb* db = tdb_init();
    assert(tdb_open(db, path) == 0);
    return db;
}

static int timestamp_cmp(const void *a, const void *b)
{
    const uint64_t *aa = (const uint64_t*)a;
    const uint64_t *bb = (const uint64_t*)b;

    if (*aa > *bb)
        return 1;
    else if (*aa < *bb)
        return -1;
    else
        return 0;
}

static void test_multi_cursor_next(tdb_multi_cursor *mcursor,
                                   const uint64_t *correct_timestamps,
                                   uint64_t num_correct)
{
    const tdb_multi_event *mevent;
    uint64_t i = 0;

    tdb_multi_cursor_reset(mcursor);

    while ((mevent = tdb_multi_cursor_next(mcursor))){
        assert(i < num_correct);
        assert(mevent->event->timestamp == correct_timestamps[i]);

        /* test that mevent->db is set correctly */
        assert(mevent->event->num_items == 1);

        uint64_t len;
        const char *val = tdb_get_item_value(mevent->db,
                                             mevent->event->items[0],
                                             &len);
        assert(len == 4);
        uint32_t id;
        memcpy(&id, val, 4);
        assert(id == mevent->cursor_idx + 1);

        /* test multi_cursor_peek */
        mevent = tdb_multi_cursor_peek(mcursor);
        assert(mevent == NULL ||
               mevent->event->timestamp >= correct_timestamps[i]);

        ++i;
    }
    assert(i == num_correct);
}

static void test_multi_cursor_next_batch(tdb_multi_cursor *mcursor,
                                         tdb_multi_event *events,
                                         uint64_t max_events,
                                         const uint64_t *correct_timestamps,
                                         uint64_t num_correct)
{
    uint64_t i = 0;
    uint64_t n, j, total = 0;
    tdb_multi_cursor_reset(mcursor);

    while ((n = tdb_multi_cursor_next_batch(mcursor, events, max_events))){
        total += n;
        assert(total <= num_correct);
        for (j = 0; j < n; j++, i++)
            assert(events[j].event->timestamp == correct_timestamps[i]);
    }
    assert(total == num_correct);
}

static void test_multicursor(tdb **dbs, uint32_t num_dbs)
{
    uint32_t trail_id, i, n;
    tdb_cursor **cursors = malloc(num_dbs * sizeof(tdb_cursor*));
    uint64_t *correct_timestamps = malloc(MAX_TRAIL_LENGTH * 8);
    tdb_multi_event *mevents = malloc(MAX_TRAIL_LENGTH *
                                      sizeof(tdb_multi_event));

    assert(cursors != NULL);
    assert(correct_timestamps != NULL);
    assert(mevents != NULL);

    for (i = 0; i < num_dbs; i++)
        cursors[i] = tdb_cursor_new(dbs[i]);

    tdb_multi_cursor *mcursor = tdb_multi_cursor_new(cursors, num_dbs);
    assert(mcursor != NULL);

    for (trail_id = 0; trail_id < NUM_COMMON_TRAILS; trail_id++){
        /*
        produce a ground truth using normal cursors and
        sorted results
        */
        for (n = 0, i = 0; i < num_dbs; i++){
            const tdb_event *event;
            tdb_get_trail(cursors[i], trail_id);

            while ((event = tdb_cursor_next(cursors[i])))
                correct_timestamps[n++] = event->timestamp;
        }
        qsort(correct_timestamps, n, 8, timestamp_cmp);

        /* test tdb_multi_cursor_next */
        for (i = 0; i < num_dbs; i++)
            tdb_get_trail(cursors[i], trail_id);
        test_multi_cursor_next(mcursor, correct_timestamps, n);

        /* test tdb_multi_cursor_next_batch (minimum batch) */
        for (i = 0; i < num_dbs; i++)
            tdb_get_trail(cursors[i], trail_id);
        test_multi_cursor_next_batch(mcursor,
                                     mevents,
                                     1,
                                     correct_timestamps,
                                     n);

        /* test tdb_multi_cursor_next_batch (medium batch) */
        for (i = 0; i < num_dbs; i++)
            tdb_get_trail(cursors[i], trail_id);
        test_multi_cursor_next_batch(mcursor,
                                     mevents,
                                     100,
                                     correct_timestamps,
                                     n);

        /* test tdb_multi_cursor_next_batch (large batch) */
        for (i = 0; i < num_dbs; i++)
            tdb_get_trail(cursors[i], trail_id);
        test_multi_cursor_next_batch(mcursor,
                                     mevents,
                                     1000000,
                                     correct_timestamps,
                                     n);
    }

    for (i = 0; i < num_dbs; i++)
        tdb_cursor_free(cursors[i]);
    tdb_multi_cursor_free(mcursor);
    free(mevents);
    free(correct_timestamps);
    free(cursors);
}

int main(int argc, char** argv)
{
    const char *root = getenv("TDB_TMP_DIR");
    tdb *dbs[] = {create_db(root, 1, 10000),
                  create_db(root, 2, 1000),
                  create_db(root, 3, 100),
                  create_db(root, 4, 0),
                  create_db(root, 5, 50),
                  create_db(root, 6, 500)};
    const uint32_t NUM_DBS = sizeof(dbs) / sizeof(dbs[0]);
    const uint64_t BUFFER_SIZES[] = {1, 10, 1001, 1000000};
    uint64_t i, j;

    for (i = 0; i < sizeof(BUFFER_SIZES) / sizeof(BUFFER_SIZES[0]); i++){
        tdb_opt_value val = {.value = BUFFER_SIZES[i]};
        for (j = 0; j < NUM_DBS; j++)
            assert(tdb_set_opt(dbs[j], TDB_OPT_CURSOR_EVENT_BUFFER_SIZE, val) == 0);
        test_multicursor(dbs, NUM_DBS);
    }

    return 0;
}
