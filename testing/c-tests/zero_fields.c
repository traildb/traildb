#include <traildb.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

int main(int argc, char** argv)
{
    tdb_cons* c = tdb_cons_new(argv[1], "", 0);
    assert(c && "Expected tdb_cons_new() to succeed for zero fields.");
    assert( tdb_cons_finalize(c, 0) == 0 );
    tdb_cons_free(c);

    tdb* t = tdb_open(argv[1]);
    if ( !t ) { fprintf(stderr, "tdb_open() failed.\n"); return -1; }

    assert(tdb_get_field(t, "world") == -1);
    assert(tdb_get_field(t, "hello") == -1);
    assert(tdb_get_field(t, "what") == -1);
    assert(tdb_get_field(t, "is") == -1);
    assert(tdb_get_field(t, "this") == -1);
    assert(tdb_get_field(t, "time") == 0);
    assert(tdb_get_field(t, "blah") == -1);
    assert(tdb_get_field(t, "bloh") == -1);

    tdb_close(t);
    return 0;
}

