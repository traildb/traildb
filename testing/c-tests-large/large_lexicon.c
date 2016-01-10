
#include <traildb.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#define NUM_ITEMS 1000000000

int main(int argc, char** argv)
{
    static uint8_t uuid[16];
    const char *fields[] = {"large", "small"};
    uint64_t lengths[] = {NUM_ITEMS * 8LLU, 3};
    uint64_t *massive = malloc(lengths[0]);
    const char *values[] = {(const char*)massive, "fuu"};
    uint64_t i, len;
    const char *p;

    assert(massive != NULL);

    for (i = 0; i < NUM_ITEMS; i++)
        massive[i] = i;

    tdb_cons* c = tdb_cons_init();
    assert(tdb_cons_open(c, argv[1], fields, 2) == 0);

    assert(tdb_cons_add(c, uuid, 0, values, lengths) == 0);
    massive[0] = 1;
    assert(tdb_cons_add(c, uuid, 0, values, lengths) == 0);
    lengths[0] = 8;
    massive[0] = 7686;
    assert(tdb_cons_add(c, uuid, 0, values, lengths) == 0);

    assert(tdb_cons_finalize(c, 0) == 0);
    tdb_cons_close(c);

    tdb *t = tdb_init();
    assert(tdb_open(t, argv[1]) == 0);

    assert(tdb_lexicon_size(t, 1) == 4);
    assert(tdb_lexicon_size(t, 2) == 2);

    p = tdb_get_value(t, 1, 0, &len);
    assert(len == 0);
    assert(memcmp(p, "", len) == 0);

    massive[0] = 0;
    p = tdb_get_value(t, 1, 1, &len);
    assert(len == NUM_ITEMS * 8LLU);
    assert(memcmp(p, massive, len) == 0);

    massive[0] = 1;
    p = tdb_get_value(t, 1, 2, &len);
    assert(len == NUM_ITEMS * 8LLU);
    assert(memcmp(p, massive, len) == 0);

    massive[0] = 7686;
    p = tdb_get_value(t, 1, 3, &len);
    assert(len == 8);
    assert(memcmp(p, massive, len) == 0);

    p = tdb_get_value(t, 2, 1, &len);
    assert(len == 3);
    assert(memcmp(p, "fuu", len) == 0);

    tdb_close(t);
    return 0;
}
