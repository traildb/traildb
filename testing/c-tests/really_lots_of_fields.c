/* DESCRIPTION: Tests that creating a tdb with huge number of fields works ok. */

#include <traildb.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>

int main(int argc, char** argv)
{
    char* fields = malloc((TDB_MAX_NUM_FIELDS + 2) * 11);
    const char **fields_ptr = malloc((TDB_MAX_NUM_FIELDS + 2) * sizeof(char*));
    int i;
    for (i = 0; i < TDB_MAX_NUM_FIELDS + 2; i++ ){
        fields_ptr[i] = &fields[i * 11];
        sprintf(&fields[i * 11], "%u", i);
    }

    tdb_cons* c = tdb_cons_init();
    assert(tdb_cons_open(c,
                         argv[1],
                         fields_ptr,
                         TDB_MAX_NUM_FIELDS + 1) == TDB_ERR_TOO_MANY_FIELDS);
    tdb_cons_close(c);

    c = tdb_cons_init();
    assert(tdb_cons_open(c, argv[1], fields_ptr, TDB_MAX_NUM_FIELDS) == 0);
    assert(tdb_cons_finalize(c) == 0);

    tdb* t = tdb_init();
    assert(tdb_open(t, argv[1]) == 0);
    assert(strcmp(tdb_get_field_name(t, 0), "time") == 0);
    for (i = 1; i < TDB_MAX_NUM_FIELDS + 1; i++)
        assert(strcmp(tdb_get_field_name(t, i), fields_ptr[i - 1]) == 0);
    return 0;
}

