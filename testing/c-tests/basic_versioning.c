#include <stdlib.h>
#include <assert.h>

#include <traildb.h>

int main(int argc, char** argv)
{
    const char *fields[] = {};
    tdb_cons* c = tdb_cons_new(argv[1], fields, 0);
    assert(c != NULL);
    assert(tdb_cons_finalize(c, 0) == 0);
    tdb* t = tdb_open(argv[1]);
    assert(t != NULL);
    assert(tdb_version(t) == TDB_VERSION_LATEST);

    return 0;
}
