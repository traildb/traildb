#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <traildb.h>
#include "tdb_test.h"

#define NUM_FIELDS 1000

int main(int argc, char** argv)
{
    static uint8_t uuid[16];
    char* fields = calloc(1, NUM_FIELDS * 11);
    const char **fields_ptr = malloc(NUM_FIELDS * sizeof(char*));
    uint64_t *lengths = malloc(NUM_FIELDS * 8);
    int i;
    for (i = 0; i < NUM_FIELDS; i++){
        fields_ptr[i] = &fields[i * 11];
        sprintf(&fields[i * 11], "%u", i);
        lengths[i] = 10;
    }

    tdb_cons *c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, getenv("TDB_TMP_DIR"), fields_ptr, NUM_FIELDS) == 0);
    assert(tdb_cons_add(c, uuid, 0, fields_ptr, lengths) == 0);
    assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);

    tdb* t = tdb_init();
    assert(tdb_open(t, getenv("TDB_TMP_DIR")) == 0);
    for (i = 0; i < NUM_FIELDS; i++){
        assert(tdb_get_item(t, i + 1, "", 0) == tdb_make_item(i + 1, 0));
        assert(tdb_get_item(t, i + 1, fields_ptr[i], lengths[i]) ==
               tdb_make_item(i + 1, 1));
    }
    return 0;
}

