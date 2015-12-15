/* DESCRIPTION: Tests that creating a tdb with huge number of fields fails. */

#include <traildb.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

int main(int argc, char** argv)
{
    char* fields = malloc(100000000);
    for ( int i1 = 0; i1 < TDB_MAX_NUM_FIELDS+1; ++i1 ) {
        sprintf(&fields[i1*10], "%d", i1);
        for ( int i2 = 0; i2 < 10; ++i2 ) {
            if ( !fields[i1*10+i2] ) {
                fields[i1*10+i2] = 'a';
            }
        }
        fields[i1*10+9] = 0;
    }

    tdb_cons* c = tdb_cons_new(argv[1], fields, TDB_MAX_NUM_FIELDS+1);
    assert( !c && "tdb_cons_new() succeeded for more than TDB_MAX_NUM_FIELDS fields." );

    return 0;
}

