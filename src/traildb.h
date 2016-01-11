
#ifndef __TRAILDB_H__
#define __TRAILDB_H__

#include <stdlib.h>
#include <stdint.h>

#include "tdb_limits.h"
#include "tdb_types.h"
#include "tdb_error.h"

/* TODO add to configure.ac: use jemalloc if available */

#define TDB_VERSION_V0 0LLU
#define TDB_VERSION_V0_1 1LLU
#define TDB_VERSION_LATEST TDB_VERSION_V0_1

tdb_cons *tdb_cons_init(void);
tdb_error tdb_cons_open(tdb_cons *cons,
                        const char *root,
                        const char **ofield_names,
                        uint64_t num_ofields);

void tdb_cons_close(tdb_cons *cons);

tdb_error tdb_cons_add(tdb_cons *cons,
                       const uint8_t uuid[16],
                       const uint64_t timestamp,
                       const char **values,
                       const uint64_t *value_lengths);

tdb_error tdb_cons_append(tdb_cons *cons, const tdb *db);
tdb_error tdb_cons_finalize(tdb_cons *cons);

tdb_error tdb_uuid_raw(const uint8_t hexuuid[32], uint8_t uuid[16]);
tdb_error tdb_uuid_hex(const uint8_t uuid[16], uint8_t hexuuid[32]);

/* TODO: add uint64_t flags to tdb_new() */
tdb *tdb_init(void);
tdb_error tdb_open(tdb *db, const char *root);
void tdb_close(tdb *db);
void tdb_dontneed(const tdb *db);
void tdb_willneed(const tdb *db);

#if 0
/* TODO add these */
/* one value should be null */
int tdb_set_opt(tdb *db, enum option, const void *value);
const void *tdb_get_opt(tdb *db, enum option);
#endif

uint64_t tdb_lexicon_size(const tdb *db, tdb_field field);

tdb_error tdb_get_field(const tdb *db,
                        const char *field_name,
                        tdb_field *field);

const char *tdb_get_field_name(const tdb *db, tdb_field field);

/* TODO make this return tdb_error */
tdb_item tdb_get_item(const tdb *db,
                      tdb_field field,
                      const char *value,
                      uint64_t value_length);

const char *tdb_get_value(const tdb *db,
                          tdb_field field,
                          tdb_val val,
                          uint64_t *value_length);

const char *tdb_get_item_value(const tdb *db,
                               tdb_item item,
                               uint64_t *value_length);

const uint8_t *tdb_get_uuid(const tdb *db, uint64_t trail_id);

tdb_error tdb_get_trail_id(const tdb *db,
                           const uint8_t uuid[16],
                           uint64_t *trail_id);

tdb_error tdb_set_filter(tdb *db,
                         const tdb_item *filter,
                         uint64_t filter_len);

const tdb_item *tdb_get_filter(const tdb *db, uint64_t *filter_len);

const char *tdb_error_str(tdb_error errcode);

uint64_t tdb_num_trails(const tdb *db);
uint64_t tdb_num_events(const tdb *db);
uint64_t tdb_num_fields(const tdb *db);
uint64_t tdb_min_timestamp(const tdb *db);
uint64_t tdb_max_timestamp(const tdb *db);

uint64_t tdb_version(const tdb *db);

tdb_cursor *tdb_cursor_new(const tdb *db);
void tdb_cursor_free(tdb_cursor *cursor);

tdb_error tdb_get_trail(tdb_cursor *cursor, uint64_t trail_id);

int _tdb_cursor_next_batch(tdb_cursor *cursor);

static inline const tdb_event *tdb_cursor_next(tdb_cursor *cursor)
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

#if 0
tdb_error tdb_decode_trail(const tdb *db,
                           uint64_t trail_id,
                           tdb_item *dst,
                           uint64_t dst_size,
                           uint64_t *num_items,
                           int edge_encoded);

tdb_error tdb_decode_trail_filtered(const tdb *db,
                                    uint64_t trail_id,
                                    tdb_item *dst,
                                    uint64_t dst_size,
                                    uint64_t *num_items,
                                    int edge_encoded,
                                    const tdb_item *filter,
                                    uint64_t filter_len);

tdb_error tdb_get_trail(const tdb *db,
                        uint64_t trail_id,
                        tdb_item **items,
                        uint64_t *items_buf_len,
                        uint64_t *num_items,
                        int edge_encoded);

/*TODO
change filtered to take a pointer to a ctx struct, including
edge_encoded. This could be libcurl-style setopt. After adding the
struct, we can remove _filtered versions of these functions
*/
tdb_error tdb_get_trail_filtered(const tdb *db,
                                 uint64_t trail_id,
                                 tdb_item **items,
                                 uint64_t *items_buf_len,
                                 uint64_t *num_items,
                                 int edge_encoded,
                                 const tdb_item *filter,
                                 uint64_t filter_len);
#endif

#endif /* __TRAILDB_H__ */
