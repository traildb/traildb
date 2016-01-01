/* DESCRIPTION: Tests that field names are assigned properly. */

#include <traildb.h>
#include <stdio.h>
#include <assert.h>

int main(int argc, char** argv)
{
    const char *fields[] = {"hello", "world", "what", "is", "this"};
    tdb_cons* c = tdb_cons_new(argv[1], fields, 5);
    tdb_field field;

    if ( !c ) { fprintf(stderr, "tdb_cons_new() failed.\n"); return -1; }

    if ( tdb_cons_finalize(c, 0) ) {
        fprintf(stderr, "tdb_cons_finalize() returned non-zero.\n");
        return -1;
    }

    tdb_cons_free(c);

    tdb* t = tdb_open(argv[1]);
    if ( !t ) { fprintf(stderr, "tdb_open() failed.\n"); return -1; }

    assert(tdb_get_field(t, "world", &field) == 0);
    assert(field == 2);
    assert(tdb_get_field(t, "hello", &field) == 0);
    assert(field == 1);
    assert(tdb_get_field(t, "what", &field) == 0);
    assert(field == 3);
    assert(tdb_get_field(t, "is", &field) == 0);
    assert(field == 4);
    assert(tdb_get_field(t, "this", &field) == 0);
    assert(field == 5);
    assert(tdb_get_field(t, "time", &field) == 0);
    assert(field == 0);
    assert(tdb_get_field(t, "blah", &field) == -1);
    assert(tdb_get_field(t, "bloh", &field) == -1);

    tdb_close(t);
    return 0;
}


