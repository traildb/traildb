#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <traildb.h>

int main(int argc, char** argv)
{
    uint8_t uuid[16];
    const char *fields[] = {};
    tdb_cons* c = tdb_cons_new(argv[1], fields, 0);
    assert(c && "Expected tdb_cons_new() to succeed for zero fields.");
    uint32_t i, j, cmp, sum = 0;
    uint32_t zero = 0;
    uint32_t *items;
    uint32_t items_len = 0;

    memset(uuid, 0, 16);

    for (i = 0; i < 1000; i++){
        memcpy(uuid, &i, 4);
        for (j = 0; j < 10 + i; j++){
            sum += j;
            assert(tdb_cons_add(c, uuid, j, fields, &zero) == 0);
        }
    }

    assert( tdb_cons_finalize(c, 0) == 0 );
    tdb_cons_free(c);

    tdb* t = tdb_open(argv[1]);
    if ( !t ) { fprintf(stderr, "tdb_open() failed.\n"); return -1; }

    assert(tdb_num_fields(t) == 1);

    assert(tdb_get_field(t, "world") == -1);
    assert(tdb_get_field(t, "hello") == -1);
    assert(tdb_get_field(t, "what") == -1);
    assert(tdb_get_field(t, "is") == -1);
    assert(tdb_get_field(t, "this") == -1);
    assert(tdb_get_field(t, "time") == 0);
    assert(tdb_get_field(t, "blah") == -1);
    assert(tdb_get_field(t, "bloh") == -1);

    for (i = 0; i < tdb_num_trails(t); i++){
        assert(tdb_get_trail(t, i, &items, &items_len, &j, 0) == 0);
        while (j--)
            cmp += items[j];
    }
    assert(cmp == sum);

    tdb_close(t);
    return 0;
}

