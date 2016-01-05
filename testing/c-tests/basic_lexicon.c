#include <traildb.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

static uint8_t uuid[16];

int main(int argc, char** argv)
{
    const char *fields[] = {"a", "b"};
    const char *values1[] = {"red", "toy"};
    const uint64_t lengths1[] = {3, 3};
    const char *values2[] = {"blue", "ball"};
    const uint64_t lengths2[] = {4, 4};
    const char *values3[] = {"0123456789", "toy"};
    const uint64_t lengths3[] = {10, 3};
    const char *p;
    uint64_t len;

    tdb_cons* c = tdb_cons_init();
    assert(tdb_cons_open(c, argv[1], fields, 2) == 0);

    tdb_cons_add(c, uuid, 0, values1, lengths1);
    tdb_cons_add(c, uuid, 0, values2, lengths2);
    tdb_cons_add(c, uuid, 0, values3, lengths3);

    assert( tdb_cons_finalize(c, 0) == 0 );
    tdb_cons_close(c);

    tdb *t = tdb_init();
    assert(tdb_open(t, argv[1]) == 0);

    assert(tdb_lexicon_size(t, 0) == 0);
    assert(tdb_lexicon_size(t, 1) == 4);
    assert(tdb_lexicon_size(t, 2) == 3);

    p = tdb_get_value(t, 1, 0, &len);
    assert(len == 0);
    assert(memcmp(p, "", len) == 0);

    p = tdb_get_value(t, 1, 1, &len);
    assert(len == 3);
    assert(memcmp(p, "red", len) == 0);

    p = tdb_get_value(t, 1, 2, &len);
    assert(len == 4);
    assert(memcmp(p, "blue", len) == 0);

    p = tdb_get_value(t, 1, 3, &len);
    assert(len == 10);
    assert(memcmp(p, "0123456789", len) == 0);

    p = tdb_get_value(t, 2, 0, &len);
    assert(len == 0);
    assert(memcmp(p, "", len) == 0);

    p = tdb_get_value(t, 2, 1, &len);
    assert(len == 3);
    assert(memcmp(p, "toy", len) == 0);

    p = tdb_get_value(t, 2, 2, &len);
    assert(len == 4);
    assert(memcmp(p, "ball", len) == 0);

    return 0;
}

