#include <traildb.h>
#include <assert.h>

#include "tdb_test.h"

int main(int argc, char** argv)
{
    tdb* t = tdb_init();
    assert(t && "Expected tdb_init() to succeed.");

    tdb_close(t);

    return 0;
}

