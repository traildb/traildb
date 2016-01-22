#include <traildb.h>
#include <assert.h>

int main(int argc, char** argv)
{
    ((void) argc);
    ((void) argv);

    tdb* t = tdb_init();
    assert(t && "Expected tdb_init() to succeed.");
    tdb_dontneed(t);
    tdb_close(t);

    return 0;
}

