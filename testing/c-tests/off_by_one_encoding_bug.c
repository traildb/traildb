
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <assert.h>

#include <traildb.h>

struct event{
    uint32_t time;
    char value[2];
};

struct event EVENTS[] = {
    {0,   "a1"},
    {10,  "a1"},
    {100, "a1"},
    {200, "a2"},
    {300, "a2"},
    {400, "a3"},
    {500, "a2"},
    {600, "a3"},
    {700, "a2"}
};

int main(int argc, char** argv)
{
    static uint8_t uuid[16];
    const char *fields[] = {"a", "b"};
    uint64_t lengths[] = {3, 2};
    tdb_item *items;
    uint64_t n, items_len = 0;
    uint64_t i;

    tdb_cons* c = tdb_cons_init();
    assert(tdb_cons_open(c, argv[1], fields, 2) == 0);

    for (i = 0; i < sizeof(EVENTS) / sizeof(struct event); i++){
        const char *values[] = {"cli", EVENTS[i].value};
        memcpy(uuid, &i, 4);
        assert(tdb_cons_add(c, uuid, EVENTS[i].time, values, lengths) == 0);
    }

    assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);

    tdb* t = tdb_init();
    assert(tdb_open(t, argv[1]) == 0);

    for (i = 0; i < sizeof(EVENTS) / sizeof(struct event); i++){
        uint64_t trail_id;
        uint64_t len;
        const char *val;

        memcpy(uuid, &i, 4);
        assert(tdb_get_trail_id(t, uuid, &trail_id) == 0);
        assert(tdb_get_trail(t, trail_id, &items, &items_len, &n, 0) == 0);
        assert(n == 4);

        val = tdb_get_item_value(t, items[2], &len);
        assert(len == 2);

        assert(items[0] == EVENTS[i].time);
        assert(memcmp(EVENTS[i].value, val, len) == 0);
    }

    tdb_close(t);
    return 0;
}

