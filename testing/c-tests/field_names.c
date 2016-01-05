#include <stdio.h>
#include <assert.h>

#include <traildb.h>

int main(int argc, char** argv)
{
    const char *fields[] = {"hello", "world", "what", "is", "this"};
    tdb_field field;
    tdb_cons* c = tdb_cons_init();
    assert(tdb_cons_open(c, argv[1], fields, 5) == 0);
    assert(tdb_cons_finalize(c, 0) == 0);

    tdb* t = tdb_init();
    assert(tdb_open(t, argv[1]) == 0);

    assert(tdb_get_field(t, "world", &field) == 0);
    assert(field == 2);
    assert(tdb_get_field(t, "hello", &field) == 0);
    assert(field == 1);
    assert(tdb_get_field(t, "what", &field) == 0);
    assert(field == 3);
    assert(tdb_get_field(t, "is", &field) == 0);
    assert(field == 4);
    assert(tdb_get_field(t, "this", &field) == 0);
    assert(field == 5);
    assert(tdb_get_field(t, "time", &field) == 0);
    assert(field == 0);
    assert(tdb_get_field(t, "blah", &field) == TDB_ERR_UNKNOWN_FIELD);
    assert(tdb_get_field(t, "bloh", &field) == TDB_ERR_UNKNOWN_FIELD);

    tdb_close(t);
    return 0;
}


