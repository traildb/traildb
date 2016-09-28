
#ifndef __TRAILDB_H__
#define __TRAILDB_H__

#include <stdlib.h>
#include <stdint.h>

#include "tdb_limits.h"
#include "tdb_types.h"
#include "tdb_error.h"

#define TDB_VERSION_V0 0LLU
#define TDB_VERSION_V0_1 1LLU
#define TDB_VERSION_LATEST TDB_VERSION_V0_1

/*
-----------------------
Construct a new TrailDB
-----------------------
*/

/* Init a new constructor handle */
tdb_cons *tdb_cons_init(void);

/* Open a new constructor with a schema */
tdb_error tdb_cons_open(tdb_cons *cons,
                        const char *root,
                        const char **ofield_names,
                        uint64_t num_ofields);

/* Close a constructor handle */
void tdb_cons_close(tdb_cons *cons);

/* Set constructor options */
tdb_error tdb_cons_set_opt(tdb_cons *cons,
                           tdb_opt_key key,
                           tdb_opt_value value);

/* Get constructor options */
tdb_error tdb_cons_get_opt(tdb_cons *cons,
                           tdb_opt_key key,
                           tdb_opt_value *value);

/* Add an event in the constructor */
tdb_error tdb_cons_add(tdb_cons *cons,
                       const uint8_t uuid[16],
                       const uint64_t timestamp,
                       const char **values,
                       const uint64_t *value_lengths);

/* Merge an existing TrailDB to this constructor */
tdb_error tdb_cons_append(tdb_cons *cons, const tdb *db);

/* Finalize a constructor */
tdb_error tdb_cons_finalize(tdb_cons *cons);

/*
---------------------------------
Open TrailDBs and access metadata
---------------------------------
*/

/* Init a new TrailDB handle */
tdb *tdb_init(void);

/* Open a TrailDB */
tdb_error tdb_open(tdb *db, const char *root);

/* Close a TrailDB */
void tdb_close(tdb *db);

/* Inform the operating system that memory can be paged for this TrailDB */
void tdb_dontneed(const tdb *db);

/* Inform the operating system that this TrailDB will be needed soon */
void tdb_willneed(const tdb *db);

/* Get the number of trails */
uint64_t tdb_num_trails(const tdb *db);

/* Get the number of events */
uint64_t tdb_num_events(const tdb *db);

/* Get the number of fields */
uint64_t tdb_num_fields(const tdb *db);

/* Get the oldest timestamp */
uint64_t tdb_min_timestamp(const tdb *db);

/* Get the newest timestamp */
uint64_t tdb_max_timestamp(const tdb *db);

/* Get the version of this TrailDB */
uint64_t tdb_version(const tdb *db);

/* Translate an error code to a string */
const char *tdb_error_str(tdb_error errcode);

/* Set a TrailDB option */
tdb_error tdb_set_opt(tdb *db, tdb_opt_key key, tdb_opt_value value);

/* Get a TrailDB option */
tdb_error tdb_get_opt(tdb *db, tdb_opt_key key, tdb_opt_value *value);

/*
----------------------------------
Translate items to values and back
----------------------------------
*/

/* Get the number of distinct values in the given field */
uint64_t tdb_lexicon_size(const tdb *db, tdb_field field);

/* Get the field ID given a field name */
tdb_error tdb_get_field(const tdb *db,
                        const char *field_name,
                        tdb_field *field);

/* Get the field name given a field ID */
const char *tdb_get_field_name(const tdb *db, tdb_field field);

/* Get item corresponding to a value */
tdb_item tdb_get_item(const tdb *db,
                      tdb_field field,
                      const char *value,
                      uint64_t value_length);

/* Get value corresponding to a field, value ID pair */
const char *tdb_get_value(const tdb *db,
                          tdb_field field,
                          tdb_val val,
                          uint64_t *value_length);

/* Get value given an item */
const char *tdb_get_item_value(const tdb *db,
                               tdb_item item,
                               uint64_t *value_length);

/*
------------
Handle UUIDs
------------
*/

/* Get UUID given a Trail ID */
const uint8_t *tdb_get_uuid(const tdb *db, uint64_t trail_id);

/* Get Trail ID given a UUID */
tdb_error tdb_get_trail_id(const tdb *db,
                           const uint8_t uuid[16],
                           uint64_t *trail_id);

/* Translate a hex-encoded UUID to a raw 16-byte UUID */
tdb_error tdb_uuid_raw(const uint8_t hexuuid[32], uint8_t uuid[16]);

/* Translate a raw 16-byte UUID to a hex-encoded UUID */
void tdb_uuid_hex(const uint8_t uuid[16], uint8_t hexuuid[32]);

/*
------------
Event filter
------------
*/

/* Create a new event filter */
struct tdb_event_filter *tdb_event_filter_new(void);

/* Add a new term (item) in an OR-clause */
tdb_error tdb_event_filter_add_term(struct tdb_event_filter *filter,
                                    tdb_item term,
                                    int is_negative);

/* Add a new clause, connected by AND to the previous clauses */
tdb_error tdb_event_filter_new_clause(struct tdb_event_filter *filter);

/* Free an event filter */
void tdb_event_filter_free(struct tdb_event_filter *filter);

/* Get an item in a clause */
tdb_error tdb_event_filter_get_item(const struct tdb_event_filter *filter,
                                    uint64_t clause_index,
                                    uint64_t item_index,
                                    tdb_item *item,
                                    int *is_negative);

/* Get the number of clauses in this filter */
uint64_t tdb_event_filter_num_clauses(const struct tdb_event_filter *filter);


/*
------------
Trail cursor
------------
*/

/* Create a new cursor */
tdb_cursor *tdb_cursor_new(const tdb *db);

/* Free a cursor */
void tdb_cursor_free(tdb_cursor *cursor);

/* Reset the cursor to the given Trail ID */
tdb_error tdb_get_trail(tdb_cursor *cursor, uint64_t trail_id);

/* Get the number of events remaining in this cursor */
uint64_t tdb_get_trail_length(tdb_cursor *cursor);

/* Set an event filter for this cursor */
tdb_error tdb_cursor_set_event_filter(tdb_cursor *cursor,
                                      const struct tdb_event_filter *filter);

/* Unset an event filter */
void tdb_cursor_unset_event_filter(tdb_cursor *cursor);

/* Internal function used by tdb_cursor_next() */
int _tdb_cursor_next_batch(tdb_cursor *cursor);

/*
------------
Trails Iter
------------
*/

/* Create a new iter */
tdb_iter *tdb_iter_new(const tdb *db, const struct tdb_event_filter *filter);

/* Load the next cursor / id into the iter */
tdb_iter *tdb_iter_next(tdb_iter *iter);

/* Free the iter */
void tdb_iter_free(tdb_iter *iter);

/*
------------
Multi cursor
------------
*/

/* Create a new multicursor */
tdb_multi_cursor *tdb_multi_cursor_new(tdb_cursor **cursors,
                                       uint64_t num_cursors);

/*
Reset the multicursor to reflect the underlying status of individual
cursors. Call after tdb_get_trail() or tdb_cursor_next()
*/
void tdb_multi_cursor_reset(tdb_multi_cursor *mc);

/* Return next event in the timestamp order from the underlying cursors */
const tdb_multi_event *tdb_multi_cursor_next(tdb_multi_cursor *mcursor);

/*
Return a batch of maximum max_events in the timestamp order from the
underlying cursors
*/
uint64_t tdb_multi_cursor_next_batch(tdb_multi_cursor *mcursor,
                                     tdb_multi_event *events,
                                     uint64_t max_events);

/* Peek the next event in the cursor */
const tdb_multi_event *tdb_multi_cursor_peek(tdb_multi_cursor *mcursor);

/* Free multicursors */
void tdb_multi_cursor_free(tdb_multi_cursor *mcursor);

/*
Return the next event from the cursor

tdb_cursor_next() is defined here so it can be inlined

the pragma is a workaround for older GCCs that have this issue:
https://gcc.gnu.org/bugzilla/show_bug.cgi?id=54113
*/
#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-prototypes"
__attribute__((visibility("default"))) inline const tdb_event *tdb_cursor_next(tdb_cursor *cursor)
{
    if (cursor->num_events_left > 0 || _tdb_cursor_next_batch(cursor)){
        const tdb_event *e = (const tdb_event*)cursor->next_event;
        cursor->next_event += sizeof(tdb_event) +
                              e->num_items * sizeof(tdb_item);
        --cursor->num_events_left;
        return e;
    }else
        return NULL;
}

/*
Peek the next event in the cursor
*/
__attribute__((visibility("default"))) inline const tdb_event *tdb_cursor_peek(tdb_cursor *cursor)
{
    if (cursor->num_events_left > 0 || _tdb_cursor_next_batch(cursor)){
        return (const tdb_event*)cursor->next_event;
    }else
        return NULL;
}


#pragma GCC diagnostic pop

#endif /* __TRAILDB_H__ */
