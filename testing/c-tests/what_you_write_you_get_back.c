#include <traildb.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#define NUM_EVENTS 3

static uint8_t cookie[16];

static char buffer1[TDB_MAX_VALUE_SIZE];
static char buffer2[TDB_MAX_VALUE_SIZE];
static char buffer3[TDB_MAX_VALUE_SIZE];

const uint32_t LENGTHS[] = {0, 1, 2, 1000, TDB_MAX_VALUE_SIZE};

int main(int argc, char** argv)
{
    char j, i = 0;
    tdb_field field;
    const char *values[] = {buffer1, buffer2, buffer3};
    uint32_t lengths[3];
    tdb_item *items;
    uint32_t n, items_len = 0;

    tdb_cons* c = tdb_cons_new(argv[1], "a\0b\0c\0", 3);
    assert(c && "Expected tdb_cons_new() to succeed.");

    for (i = 0; i < sizeof(LENGTHS) / 4; i++){
        lengths[0] = lengths[1] = lengths[2] = LENGTHS[i];
        if (LENGTHS[i] > 0)
            memset(buffer1, i, LENGTHS[i]);
        memset(buffer2, i + 10, LENGTHS[i]);
        memset(buffer3, i + 20, LENGTHS[i]);
        memset(cookie, i, sizeof(cookie));
        for (j = 0; j < NUM_EVENTS; j++)
            tdb_cons_add(c, cookie, i, values, lengths);
    }
    assert( tdb_cons_finalize(c, 0) == 0 );
    tdb_cons_free(c);

    tdb* t = tdb_open(argv[1]);
    if (!t){
        fprintf(stderr, "tdb_open() failed.\n");
        return -1;
    }

    for (i = 0; i < sizeof(LENGTHS) / 4; i++){
        memset(cookie, i, sizeof(cookie));
        uint64_t cookie_id = tdb_get_cookie_id(t, cookie);
        int r;
        assert(cookie_id != -1);
        r = tdb_get_trail(t, cookie_id, &items, &items_len, &n, 0);
        assert(r == 0 && "Expected tdb_get_trail() to succeed.");
        assert(n / 5 == NUM_EVENTS && "Invalid number of events returned.");

        if (LENGTHS[i] > 0)
            memset(buffer1, i, LENGTHS[i]);
        memset(buffer2, i + 10, LENGTHS[i]);
        memset(buffer3, i + 20, LENGTHS[i]);

        for (j = 0; j < n;){
            assert(items[j++] == i && "Unexpected timestamp.");
            for (field = 0; field < 3; field++){
                uint32_t len;
                const char *p = tdb_get_item_value(t, items[j++], &len);
                assert(p != NULL);
                assert(len == LENGTHS[i]);
                assert(memcmp(values[field], p, len) == 0);
            }
            ++j;
        }
    }

    tdb_close(t);
    return 0;
}

