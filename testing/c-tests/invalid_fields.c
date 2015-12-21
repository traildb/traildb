/* DESCRIPTION: Tests that invalid field names are rejected. */

#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <assert.h>

#include <traildb.h>

int main(int argc, char** argv)
{
    static char buf[TDB_MAX_FIELDNAME_LENGTH + 1];
    const char *bad[TDB_MAX_NUM_FIELDS + 1];
    uint32_t i;

    bad[0] = bad[1] = buf;

    /* too many fields */
    tdb_cons* c = tdb_cons_new(argv[1], bad, TDB_MAX_NUM_FIELDS + 1);
    assert(c == NULL);

    /* an empty field name */
    buf[0] = 0;
    c = tdb_cons_new(argv[1], bad, 1);
    assert(c == NULL);

    /* duplicate field names */
    buf[0] = 'a';
    buf[1] = 0;
    c = tdb_cons_new(argv[1], bad, 2);
    assert(c == NULL);

    /* time is a reserved field name */
    sprintf(buf, "time");
    c = tdb_cons_new(argv[1], bad, 1);
    assert(c == NULL);

    /* a field name too long */
    memset(buf, 'a', TDB_MAX_FIELDNAME_LENGTH);
    buf[TDB_MAX_FIELDNAME_LENGTH] = 0;
    c = tdb_cons_new(argv[1], bad, 1);
    assert(c == NULL);

    /* an invalid character */
    for (i = 0; i < 256; i++){
        if (!index(TDB_FIELDNAME_CHARS, i)){
            buf[0] = i;
            buf[1] = 0;
            c = tdb_cons_new(argv[1], bad, 1);
            assert(c == NULL);
        }
    }

    /* double-check these special characters -
       they should have been checked above already */
    buf[0] = '/';
    buf[1] = 0;
    c = tdb_cons_new(argv[1], bad, 1);
    assert(c == NULL);

    buf[0] = '.';
    buf[1] = 0;
    c = tdb_cons_new(argv[1], bad, 1);
    assert(c == NULL);

    return 0;
}

