/* DESCRIPTION: Tests that invalid field names are rejected. */

#include <traildb.h>
#include <stdio.h>
#include <assert.h>

int main(int argc, char** argv)
{
    tdb_cons* c = tdb_cons_new(argv[1], "///\0what//you\0", 2);
    if ( !c ) { return 0; }

    assert(0 && "Expected that tdb_cons_new() fails with slashes in field names.");
    return -1;
}

