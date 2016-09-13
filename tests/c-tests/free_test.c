
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <float.h>

// explicitly set to non-maximum value
#define DSFMT_MEXP 521
#include <traildb.h>

int main(int argc, char **argv)
{
    //test all our free functions with NULL and make sure we don't segfault
    tdb_event_filter_free(NULL);
    tdb_cons_close(NULL);
    tdb_curor_free(NULL);
    tdb_multi_cursor_free(NULL);
    return 0;
}
a
