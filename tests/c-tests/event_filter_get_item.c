
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <traildb.h>
#include "tdb_test.h"

int main(int argc, char **argv)
{
    uint64_t i, j;
    tdb_item item;
    int is_negative;

    /* empty filter */
    struct tdb_event_filter *f = tdb_event_filter_new();
    assert(tdb_event_filter_num_clauses(f) == 1);
    assert(tdb_event_filter_get_item(f, 0, 0, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_item(f, 0, 1, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_item(f, 1, 0, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_item(f, 100, 100, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    tdb_event_filter_free(f);

    /* one clause */
    f = tdb_event_filter_new();
    tdb_event_filter_add_term(f, 3, 0);
    tdb_event_filter_add_term(f, 4, 1);
    assert(tdb_event_filter_num_clauses(f) == 1);
    is_negative = item = 5;
    assert(tdb_event_filter_get_item(f, 0, 0, &item, &is_negative) == 0);
    assert(item == 3);
    assert(is_negative == 0);
    assert(tdb_event_filter_get_item(f, 0, 1, &item, &is_negative) == 0);
    assert(item == 4);
    assert(is_negative == 1);
    assert(tdb_event_filter_get_item(f, 1, 0, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    tdb_event_filter_free(f);

    /* many terms */
    f = tdb_event_filter_new();
    for (i = 0; i < 100000; i++)
        assert(tdb_event_filter_add_term(f, i + 1000, i % 2) == 0);
    for (i = 0; i < 100000; i++){
        assert(tdb_event_filter_get_item(f, 0, i, &item, &is_negative) == 0);
        assert(item == i + 1000);
        assert(is_negative == i % 2);
    }
    tdb_event_filter_free(f);

    /* many sparse clauses */
    f = tdb_event_filter_new();
    assert(tdb_event_filter_add_term(f, 5, 1) == 0);
    for (i = 0; i < 1000; i++){
        assert(tdb_event_filter_new_clause(f) == 0);
        if (i == 500){
            assert(tdb_event_filter_add_term(f, 15, 0) == 0);
            assert(tdb_event_filter_add_term(f, 16, 1) == 0);
        }
    }
    assert(tdb_event_filter_num_clauses(f) == 1001);
    for (i = 1; i < 1000; i++){
        if (i == 501)
            continue;
        assert(tdb_event_filter_get_item(f, i, 0, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
        assert(item == 0);
        assert(is_negative == 0);
    }
    assert(tdb_event_filter_get_item(f, 0, 0, &item, &is_negative) == 0);
    assert(item == 5);
    assert(is_negative == 1);
    assert(tdb_event_filter_get_item(f, 501, 0, &item, &is_negative) == 0);
    assert(item == 15);
    assert(is_negative == 0);
    assert(tdb_event_filter_get_item(f, 501, 1, &item, &is_negative) == 0);
    assert(item == 16);
    assert(is_negative == 1);

    tdb_event_filter_free(f);

    /* examples in the docs */
    f = tdb_event_filter_new();
    uint64_t real_num = 0, num = 0;
    for (i = 0; i < 100; i++){
        assert(tdb_event_filter_new_clause(f) == 0);
        for (j = 0; j < i; j++)
            assert(tdb_event_filter_add_term(f, real_num++, 0) == 0);
    }
    for (i = 0; i < tdb_event_filter_num_clauses(f); i++){
        j = 0;
        while (!tdb_event_filter_get_item(f, i, j, &item, &is_negative)){
            assert(item == num);
            assert(is_negative == 0);
            ++num;
            ++j;
        }
    }
    assert(num == real_num);

    return 0;
}
