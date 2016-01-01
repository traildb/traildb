
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <tdb_types.h>
#include <tdb_limits.h>

static void check_item(tdb_field field, tdb_val val)
{
    tdb_item item = tdb_make_item(field, val);

    if (field <= TDB_FIELD32_MAX && val <= TDB_VAL32_MAX){
        assert(item <= UINT32_MAX);
        assert(tdb_item_is32(item) == 1);
        assert(tdb_item_field32(item) == field);
        assert(tdb_item_val32(item) == val);
    }else
        assert(tdb_item_is32(item) == 0);

    assert(tdb_item_val(item) == val);
    assert(tdb_item_field(item) == field);
}

static void test_vals(tdb_field field)
{
    tdb_val val;

    /* test small vals */

    for (val = 0; val < 1000; val++)
        check_item(field, val);

    /* test medium vals */

    for (val = TDB_VAL32_MAX - 1000; val < TDB_VAL32_MAX + 1000; val++)
        check_item(field, val);

    /* test large vals */

    for (val = TDB_MAX_NUM_VALUES - 1000; val < TDB_MAX_NUM_VALUES; val++)
        check_item(field, val);
}

int main(int argc, char** argv)
{
    tdb_field field;

    /* test small field IDs */

    for (field = 0; field < TDB_FIELD32_MAX + 1000; field++)
        test_vals(field);

    /* test large field IDs */

    for (field = TDB_MAX_NUM_FIELDS - 1000; field < TDB_MAX_NUM_FIELDS; field++)
        test_vals(field);

    return 0;
}

