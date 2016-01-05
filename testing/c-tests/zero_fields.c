#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <traildb.h>

int main(int argc, char** argv)
{
    uint8_t uuid[16];
    const char *fields[] = {};
    tdb_cons* c = tdb_cons_init();
    assert(tdb_cons_open(c, argv[1], fields, 0) == 0);
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

    assert(tdb_cons_finalize(c, 0) == 0);
    tdb_cons_close(c);

    tdb* t = tdb_init();
    assert(tdb_open(t, argv[1]) == 0);

    assert(tdb_num_fields(t) == 1);

    assert(tdb_get_field(t, "world", &field) == TDB_ERR_UNKNOWN_FIELD);
    assert(tdb_get_field(t, "hello", &field) == TDB_ERR_UNKNOWN_FIELD);
    assert(tdb_get_field(t, "what", &field) == TDB_ERR_UNKNOWN_FIELD);
    assert(tdb_get_field(t, "is", &field) == TDB_ERR_UNKNOWN_FIELD);
    assert(tdb_get_field(t, "this", &field) == TDB_ERR_UNKNOWN_FIELD);
    assert(tdb_get_field(t, "time", &field) == 0);
    assert(field == 0);
    assert(tdb_get_field(t, "blah", &field) == TDB_ERR_UNKNOWN_FIELD);
    assert(tdb_get_field(t, "bloh", &field) == TDB_ERR_UNKNOWN_FIELD);

    for (cmp = 0, i = 0; i < tdb_num_trails(t); i++){
        assert(tdb_get_trail(t, i, &items, &items_len, &j, 0) == 0);
        while (j--)
            cmp += items[j];
    }
    assert(cmp == sum);

    tdb_close(t);
    return 0;
}

