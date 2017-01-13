#include <stdio.h>
#include <stdlib.h>
#include <assert.h>

#include <traildb.h>
#include "tdb_test.h"

int main(int argc, char **argv)
{
    uint64_t start, end, num_terms;
    tdb_item item;
    int is_negative;
    tdb_event_filter_term_type term_type;
    
    /* EMPTY FILTER */
    struct tdb_event_filter *f = tdb_event_filter_new();
    assert(tdb_event_filter_num_clauses(f) == 1);
    assert(tdb_event_filter_num_terms(f, 0, &num_terms) == TDB_ERR_OK);
    assert(num_terms == 0);
    assert(tdb_event_filter_get_item(f, 0, 0, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_item(f, 0, 1, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_item(f, 1, 0, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_item(f, 100, 100, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_time_range(f, 0, 0, &start, &end) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_time_range(f, 0, 1, &start, &end) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_time_range(f, 1, 0, &start, &end) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_time_range(f, 100, 100, &start, &end) == TDB_ERR_NO_SUCH_ITEM);

    assert(tdb_event_filter_new_clause(f) == TDB_ERR_OK);
    assert(tdb_event_filter_num_clauses(f) == 2);
    assert(tdb_event_filter_new_clause(f) == TDB_ERR_OK);
    assert(tdb_event_filter_num_clauses(f) == 3);
    assert(tdb_event_filter_new_clause(f) == TDB_ERR_OK);
    assert(tdb_event_filter_num_clauses(f) == 4);
    
    tdb_event_filter_free(f);

    /* ONE TERM */
    f = tdb_event_filter_new();

    /* time range */
    assert(tdb_event_filter_add_time_range(f, 3, 5) == TDB_ERR_OK);

    /* time range - num clauses and terms */
    assert(tdb_event_filter_num_clauses(f) == 1);
    assert(tdb_event_filter_num_terms(f, 0, &num_terms) == TDB_ERR_OK);
    assert(num_terms == 1);
    assert(tdb_event_filter_num_terms(f, 1, &num_terms) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_num_terms(f, 100, &num_terms) == TDB_ERR_NO_SUCH_ITEM);
    
    /* time range - get term type */
    assert(tdb_event_filter_get_term_type(f, 0, 0, &term_type) == TDB_ERR_OK);
    assert(term_type == TDB_EVENT_FILTER_TIME_RANGE_TERM);
    assert(tdb_event_filter_get_term_type(f, 0, 1, &term_type) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_term_type(f, 1, 1, &term_type) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_term_type(f, 1, 0, &term_type) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_term_type(f, 100, 100, &term_type) == TDB_ERR_NO_SUCH_ITEM);

    /* time range - get item */
    assert(tdb_event_filter_get_item(f, 0, 0, &item, &is_negative) == TDB_ERR_INCORRECT_TERM_TYPE);
    assert(tdb_event_filter_get_item(f, 0, 1, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_item(f, 1, 0, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_item(f, 1, 1, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_item(f, 100, 100, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);

    /* time range - get time range */
    assert(tdb_event_filter_get_time_range(f, 0, 0, &start, &end) == TDB_ERR_OK);
    assert(start == 3);
    assert(end == 5);
    assert(tdb_event_filter_get_time_range(f, 0, 1, &start, &end) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_time_range(f, 1, 0, &start, &end) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_time_range(f, 1, 1, &start, &end) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_time_range(f, 100, 100, &start, &end) == TDB_ERR_NO_SUCH_ITEM);

    tdb_event_filter_free(f);
    
    /* item */
    f = tdb_event_filter_new();
    assert(tdb_event_filter_add_term(f, 3, 0) == TDB_ERR_OK);

    /* item - num clauses and terms */
    assert(tdb_event_filter_num_clauses(f) == 1);
    assert(tdb_event_filter_num_terms(f, 0, &num_terms) == TDB_ERR_OK);
    assert(num_terms == 1);
    assert(tdb_event_filter_num_terms(f, 1, &num_terms) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_num_terms(f, 100, &num_terms) == TDB_ERR_NO_SUCH_ITEM);

    /* item - get term type */
    assert(tdb_event_filter_get_term_type(f, 0, 0, &term_type) == TDB_ERR_OK);
    assert(term_type == TDB_EVENT_FILTER_MATCH_TERM);
    assert(tdb_event_filter_get_term_type(f, 0, 1, &term_type) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_term_type(f, 1, 1, &term_type) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_term_type(f, 1, 0, &term_type) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_term_type(f, 100, 100, &term_type) == TDB_ERR_NO_SUCH_ITEM);

    /* item - get time range */
    assert(tdb_event_filter_get_time_range(f, 0, 0, &start, &end) == TDB_ERR_INCORRECT_TERM_TYPE);
    assert(tdb_event_filter_get_time_range(f, 0, 1, &start, &end) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_time_range(f, 1, 0, &start, &end) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_time_range(f, 1, 1, &start, &end) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_time_range(f, 100, 100, &start, &end) == TDB_ERR_NO_SUCH_ITEM);

    /* item - get item */
    assert(tdb_event_filter_get_item(f, 0, 0, &item, &is_negative) == TDB_ERR_OK);
    assert(item == 3);
    assert(is_negative == 0);

    tdb_event_filter_free(f);

    /* TWO TERMS */
        
    /* time range, item */
    f = tdb_event_filter_new();
    assert(tdb_event_filter_add_time_range(f, 3, 5) == TDB_ERR_OK);
    assert(tdb_event_filter_add_term(f, 3, 0) == TDB_ERR_OK);

    /* time range, item - num clauses and terms */
    assert(tdb_event_filter_num_clauses(f) == 1);
    assert(tdb_event_filter_num_terms(f, 0, &num_terms) == TDB_ERR_OK);
    assert(num_terms == 2);
    assert(tdb_event_filter_num_terms(f, 1, &num_terms) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_num_terms(f, 100, &num_terms) == TDB_ERR_NO_SUCH_ITEM);

    /* time range, item - get term types */
    assert(tdb_event_filter_get_term_type(f, 0, 0, &term_type) == TDB_ERR_OK);
    assert(term_type == TDB_EVENT_FILTER_TIME_RANGE_TERM);
    assert(tdb_event_filter_get_term_type(f, 0, 1, &term_type) == TDB_ERR_OK);
    assert(term_type == TDB_EVENT_FILTER_MATCH_TERM);

    /* time range, item - get time range, get item */
    assert(tdb_event_filter_get_time_range(f, 0, 0, &start, &end) == TDB_ERR_OK);
    assert(start == 3);
    assert(end == 5);
    assert(tdb_event_filter_get_time_range(f, 0, 1, &start, &end) == TDB_ERR_INCORRECT_TERM_TYPE);

    assert(tdb_event_filter_get_item(f, 0, 0, &item, &is_negative) == TDB_ERR_INCORRECT_TERM_TYPE);
    assert(tdb_event_filter_get_item(f, 0, 1, &item, &is_negative) == TDB_ERR_OK);
    assert(item == 3);
    assert(is_negative == 0);
    
    tdb_event_filter_free(f);
    
    /* item, time range */
    f = tdb_event_filter_new();
    assert(tdb_event_filter_add_term(f, 3, 0) == TDB_ERR_OK);
    assert(tdb_event_filter_add_time_range(f, 3, 5) == TDB_ERR_OK);

    /* item, time range  - num clauses and terms */
    assert(tdb_event_filter_num_clauses(f) == 1);
    assert(tdb_event_filter_num_terms(f, 0, &num_terms) == TDB_ERR_OK);
    assert(num_terms == 2);
    assert(tdb_event_filter_num_terms(f, 1, &num_terms) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_num_terms(f, 100, &num_terms) == TDB_ERR_NO_SUCH_ITEM);

    /* item, time range  - get term types */
    assert(tdb_event_filter_get_term_type(f, 0, 0, &term_type) == TDB_ERR_OK);
    assert(term_type == TDB_EVENT_FILTER_MATCH_TERM);
    assert(tdb_event_filter_get_term_type(f, 0, 1, &term_type) == TDB_ERR_OK);
    assert(term_type == TDB_EVENT_FILTER_TIME_RANGE_TERM);

    /* time range, item - get time range, get item */
    assert(tdb_event_filter_get_time_range(f, 0, 0, &start, &end) == TDB_ERR_INCORRECT_TERM_TYPE);
    assert(tdb_event_filter_get_time_range(f, 0, 1, &start, &end) == TDB_ERR_OK);
    assert(start == 3);
    assert(end == 5);

    assert(tdb_event_filter_get_item(f, 0, 0, &item, &is_negative) == TDB_ERR_OK);
    assert(item == 3);
    assert(is_negative == 0);
    assert(tdb_event_filter_get_item(f, 0, 1, &item, &is_negative) == TDB_ERR_INCORRECT_TERM_TYPE);

    tdb_event_filter_free(f);

    /* TWO CLAUSES */

    /* empty, time range */
    f = tdb_event_filter_new();
    assert(tdb_event_filter_new_clause(f) == TDB_ERR_OK);
    assert(tdb_event_filter_add_time_range(f, 3, 5) == TDB_ERR_OK);

    /* empty, time range - num of clauses and terms */
    assert(tdb_event_filter_num_clauses(f) == 2);
    assert(tdb_event_filter_num_terms(f, 0, &num_terms) == TDB_ERR_OK);
    assert(num_terms == 0);
    assert(tdb_event_filter_num_terms(f, 1, &num_terms) == TDB_ERR_OK);
    assert(num_terms == 1);
    assert(tdb_event_filter_num_terms(f, 2, &num_terms) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_num_terms(f, 100, &num_terms) == TDB_ERR_NO_SUCH_ITEM);

    /* empty, time range  - get term types */
    assert(tdb_event_filter_get_term_type(f, 0, 0, &term_type) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_term_type(f, 1, 0, &term_type) == TDB_ERR_OK);
    assert(term_type == TDB_EVENT_FILTER_TIME_RANGE_TERM);
    assert(tdb_event_filter_get_term_type(f, 1, 1, &term_type) == TDB_ERR_NO_SUCH_ITEM);

    /* empty, time range - get time range, get item */
    assert(tdb_event_filter_get_time_range(f, 0, 0, &start, &end) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_time_range(f, 1, 0, &start, &end) == TDB_ERR_OK);
    assert(start == 3);
    assert(end == 5);
    assert(tdb_event_filter_get_time_range(f, 1, 1, &start, &end) == TDB_ERR_NO_SUCH_ITEM);


    assert(tdb_event_filter_get_item(f, 0, 0, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_item(f, 1, 0, &item, &is_negative) == TDB_ERR_INCORRECT_TERM_TYPE);
    assert(tdb_event_filter_get_item(f, 1, 1, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    
    tdb_event_filter_free(f);

    /* time range, empty */
    f = tdb_event_filter_new();
    assert(tdb_event_filter_add_time_range(f, 3, 5) == TDB_ERR_OK);
    assert(tdb_event_filter_new_clause(f) == TDB_ERR_OK);

    /* time range, empty - num of clauses and terms */
    assert(tdb_event_filter_num_clauses(f) == 2);
    assert(tdb_event_filter_num_terms(f, 0, &num_terms) == TDB_ERR_OK);
    assert(num_terms == 1);
    assert(tdb_event_filter_num_terms(f, 1, &num_terms) == TDB_ERR_OK);
    assert(num_terms == 0);
    assert(tdb_event_filter_num_terms(f, 2, &num_terms) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_num_terms(f, 100, &num_terms) == TDB_ERR_NO_SUCH_ITEM);

    /* time range, empty  - get term types */
    assert(tdb_event_filter_get_term_type(f, 0, 0, &term_type) == TDB_ERR_OK);
    assert(term_type == TDB_EVENT_FILTER_TIME_RANGE_TERM);
    assert(tdb_event_filter_get_term_type(f, 0, 1, &term_type) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_term_type(f, 1, 0, &term_type) == TDB_ERR_NO_SUCH_ITEM);

    /* time range, empty - get time range, get item */
    assert(tdb_event_filter_get_time_range(f, 0, 0, &start, &end) == TDB_ERR_OK);
    assert(start == 3);
    assert(end == 5);
    assert(tdb_event_filter_get_time_range(f, 0, 1, &start, &end) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_time_range(f, 1, 0, &start, &end) == TDB_ERR_NO_SUCH_ITEM);

    assert(tdb_event_filter_get_item(f, 0, 0, &item, &is_negative) == TDB_ERR_INCORRECT_TERM_TYPE);    
    assert(tdb_event_filter_get_item(f, 0, 1, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    assert(tdb_event_filter_get_item(f, 1, 1, &item, &is_negative) == TDB_ERR_NO_SUCH_ITEM);
    
    tdb_event_filter_free(f);
    
    return 0;
}
