/* DESCRIPTION: Tests that field names are assigned properly. */

#include <traildb.h>
#include <stdio.h>
#include <assert.h>

int main(int argc, char** argv)
{
    tdb_cons* c = tdb_cons_new(argv[1], "hello\0world\0what\0is\0this\0", 5);
    if ( !c ) { fprintf(stderr, "tdb_cons_new() failed.\n"); return -1; }

    if ( tdb_cons_finalize(c, 0) ) {
        fprintf(stderr, "tdb_cons_finalize() returned non-zero.\n");
        return -1;
    }
    
    tdb_cons_free(c);

    tdb* t = tdb_open(argv[1]);
    if ( !t ) { fprintf(stderr, "tdb_open() failed.\n"); return -1; }

    assert(tdb_get_field(t, "world") == 2);
    assert(tdb_get_field(t, "hello") == 1);
    assert(tdb_get_field(t, "what") == 3);
    assert(tdb_get_field(t, "is") == 4);
    assert(tdb_get_field(t, "this") == 5);
    assert(tdb_get_field(t, "time") == 0);
    assert(tdb_get_field(t, "blah") == -1);
    assert(tdb_get_field(t, "bloh") == -1);

    tdb_close(t);
    return 0;
}


