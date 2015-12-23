/* DESCRIPTION: Tests that empty creation of traildb works. */

#include <traildb.h>
#include <stdio.h>

int main(int argc, char** argv)
{
    const char *fields[] = {""};
    tdb_cons* c = tdb_cons_new(argv[1], fields, 0);
    if ( !c ) { fprintf(stderr, "tdb_cons_new() failed.\n"); return -1; }

    if ( tdb_cons_finalize(c, 0) ) {
        fprintf(stderr, "tdb_cons_finalize() returned non-zero.\n");
        return -1;
    }

    tdb_cons_free(c);
    return 0;
}

