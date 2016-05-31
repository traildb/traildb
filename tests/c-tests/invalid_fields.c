/* DESCRIPTION: Tests that invalid field names are rejected. */

#include <string.h>
#include <strings.h>
#include <stdio.h>
#include <assert.h>

#include <traildb.h>
#include "tdb_test.h"

int main(int argc, char** argv)
{
    static char buf[TDB_MAX_FIELDNAME_LENGTH + 1];
    const char *bad[TDB_MAX_NUM_FIELDS + 1];
    uint64_t i;

    bad[0] = bad[1] = buf;

    /* too many fields */
    tdb_cons* c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c,
                        getenv("TDB_TMP_DIR"),
                        bad,
                        TDB_MAX_NUM_FIELDS + 1) == TDB_ERR_TOO_MANY_FIELDS);
    tdb_cons_close(c);

    /* an empty field name */
    buf[0] = 0;
    c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, getenv("TDB_TMP_DIR"), bad, 1) == TDB_ERR_INVALID_FIELDNAME);
    tdb_cons_close(c);

    /* duplicate field names */
    buf[0] = 'a';
    buf[1] = 0;
    c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, getenv("TDB_TMP_DIR"), bad, 2) == TDB_ERR_DUPLICATE_FIELDS);
    tdb_cons_close(c);

    /* time is a reserved field name */
    sprintf(buf, "time");
    c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, getenv("TDB_TMP_DIR"), bad, 1) == TDB_ERR_INVALID_FIELDNAME);
    tdb_cons_close(c);

    /* a field name too long */
    memset(buf, 'a', TDB_MAX_FIELDNAME_LENGTH);
    buf[TDB_MAX_FIELDNAME_LENGTH] = 0;
    c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, getenv("TDB_TMP_DIR"), bad, 1) == TDB_ERR_INVALID_FIELDNAME);
    tdb_cons_close(c);

    /* an invalid character */
    for (i = 0; i < 256; i++){
        if (!index(TDB_FIELDNAME_CHARS, i)){
            buf[0] = i;
            buf[1] = 0;
            c = tdb_cons_init();
            test_cons_settings(c);
            assert(tdb_cons_open(c,
                                getenv("TDB_TMP_DIR"),
                                bad,
                                1) == TDB_ERR_INVALID_FIELDNAME);
            tdb_cons_close(c);
        }
    }

    /* double-check these special characters -
       they should have been checked above already */
    buf[0] = '/';
    buf[1] = 0;
    c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, getenv("TDB_TMP_DIR"), bad, 1) == TDB_ERR_INVALID_FIELDNAME);
    tdb_cons_close(c);

    buf[0] = '.';
    buf[1] = 0;
    c = tdb_cons_init();
    test_cons_settings(c);
    assert(tdb_cons_open(c, getenv("TDB_TMP_DIR"), bad, 1) == TDB_ERR_INVALID_FIELDNAME);
    tdb_cons_close(c);

    return 0;
}

