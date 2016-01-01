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
    uint64_t i, j, cmp, sum = 0;
    uint64_t zero = 0;
    tdb_item *items;
    uint64_t items_len = 0;
    tdb_field field;

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

    assert(tdb_get_field(t, "world", &field) == -1);
    assert(tdb_get_field(t, "hello", &field) == -1);
    assert(tdb_get_field(t, "what", &field) == -1);
    assert(tdb_get_field(t, "is", &field) == -1);
    assert(tdb_get_field(t, "this", &field) == -1);
    assert(tdb_get_field(t, "time", &field) == 0);
    assert(field == 0);
    assert(tdb_get_field(t, "blah", &field) == -1);
    assert(tdb_get_field(t, "bloh", &field) == -1);

    for (cmp = 0, i = 0; i < tdb_num_trails(t); i++){
        assert(tdb_get_trail(t, i, &items, &items_len, &j, 0) == 0);
        while (j--)
            cmp += items[j];
    }
    assert(cmp == sum);

    tdb_close(t);
    return 0;
}

