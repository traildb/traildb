
#ifndef __TRAILDB_H__
#define __TRAILDB_H__

#include <stdlib.h>
#include <stdint.h>

#include "tdb_limits.h"
#include "tdb_types.h"
#include "tdb_error.h"

/* TODO add to configure.ac: use jemalloc if available */
/* TODO add a unique identifer for each traildb (sha?) */

#define TDB_VERSION_V0 0LLU
#define TDB_VERSION_V0_1 1LLU
#define TDB_VERSION_LATEST TDB_VERSION_V0_1

tdb_cons *tdb_cons_init(void);
tdb_error tdb_cons_open(tdb_cons *cons,
                        const char *root,
                        const char **ofield_names,
                        uint64_t num_ofields);

void tdb_cons_close(tdb_cons *cons);

tdb_error tdb_cons_set_opt(tdb_cons *cons,
                           tdb_opt_key key,
                           tdb_opt_value value);

tdb_error tdb_cons_get_opt(tdb_cons *cons,
                           tdb_opt_key key,
                           tdb_opt_value *value);

tdb_error tdb_cons_add(tdb_cons *cons,
                       const uint8_t uuid[16],
                       const uint64_t timestamp,
                       const char **values,
                       const uint64_t *value_lengths);

tdb_error tdb_cons_append(tdb_cons *cons, const tdb *db);
tdb_error tdb_cons_finalize(tdb_cons *cons);

tdb_error tdb_uuid_raw(const uint8_t hexuuid[32], uint8_t uuid[16]);
void tdb_uuid_hex(const uint8_t uuid[16], uint8_t hexuuid[32]);

tdb *tdb_init(void);
tdb_error tdb_open(tdb *db, const char *root);
void tdb_close(tdb *db);
void tdb_dontneed(const tdb *db);
void tdb_willneed(const tdb *db);

tdb_error tdb_set_opt(tdb *db, tdb_opt_key key, tdb_opt_value value);
tdb_error tdb_get_opt(tdb *db, tdb_opt_key key, tdb_opt_value *value);

uint64_t tdb_lexicon_size(const tdb *db, tdb_field field);

tdb_error tdb_get_field(const tdb *db,
                        const char *field_name,
                        tdb_field *field);

const char *tdb_get_field_name(const tdb *db, tdb_field field);

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

uint64_t tdb_get_trail_length(tdb_cursor *cursor);

int _tdb_cursor_next_batch(tdb_cursor *cursor);

#pragma GCC diagnostic push
#pragma GCC diagnostic ignored "-Wmissing-prototypes"
inline const tdb_event *tdb_cursor_next(tdb_cursor *cursor)
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
#pragma GCC diagnostic pop

#endif /* __TRAILDB_H__ */
