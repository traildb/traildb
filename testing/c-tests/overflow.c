/* DESCRIPTION: Tests that overflow works. */

#include <traildb.h>
#include <stdio.h>
#include <assert.h>

int main(int argc, char** argv)
{
    const char *fields[] = {"normal", "too_many", "yay"};
    tdb_cons* c = tdb_cons_new(argv[1], fields, 3);
    if ( !c ) { fprintf(stderr, "tdb_cons_new() failed.\n"); return -1; }

    for ( int i1 = 0; i1 < TDB_OVERFLOW_VALUE*2; ++i1 ) {
        char buf[200];
        sprintf(buf, "hello%caa%d%cwut%d%c", 0, i1, 0, i1 % 30, 0);
        tdb_cons_add(c, (const uint8_t*) "AAAAAAAAAAAAAAAA", 1, buf);
    }

    if ( tdb_cons_finalize(c, 0) ) {
        fprintf(stderr, "tdb_cons_finalize() returned non-zero.\n");
        return -1;
    }
    
    tdb_cons_free(c);

    tdb* t = tdb_open(argv[1]);
    if ( !t ) { fprintf(stderr, "tdb_open() failed.\n"); return -1; }
    tdb_willneed(t);

    assert(tdb_field_has_overflow_vals(t, 0) == 0);
    assert(tdb_field_has_overflow_vals(t, 1) == 0);
    assert(tdb_field_has_overflow_vals(t, 2) != 0);
    assert(tdb_field_has_overflow_vals(t, 3) == 0);

    tdb_close(t);
    return 0;
}


