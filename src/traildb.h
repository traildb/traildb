
#ifndef __TRAILDB_H__
#define __TRAILDB_H__

#include <stdint.h>

#include "tdb_limits.h"
#include "tdb_types.h"
#include "tdb_error.h"

/* add to configure.ac: use jemalloc if available */

#define TDB_VERSION_V0 0LLU
#define TDB_VERSION_V0_1 1LLU
#define TDB_VERSION_LATEST TDB_VERSION_V0_1

typedef struct _tdb_cons tdb_cons;
typedef struct _tdb tdb;

/* TODO: move flags from finalize() to new() */
tdb_cons *tdb_cons_init(void);
int tdb_cons_open(tdb_cons *cons,
                  const char *root,
                  const char **ofield_names,
                  uint64_t num_ofields);

void tdb_cons_close(tdb_cons *cons);

int tdb_cons_add(tdb_cons *cons,
                 const uint8_t uuid[16],
                 const uint64_t timestamp,
                 const char **values,
                 const uint64_t *value_lengths);

/* TODO: rename to tdb_cons_merge() */
int tdb_cons_append(tdb_cons *cons, const tdb *db);
int tdb_cons_finalize(tdb_cons *cons, uint64_t flags);

int tdb_uuid_raw(const uint8_t hexuuid[32], uint8_t uuid[16]);
int tdb_uuid_hex(const uint8_t uuid[16], uint8_t hexuuid[32]);

/* TODO: add uint64_t flags to tdb_new() */
tdb *tdb_init(void);
int tdb_open(tdb *db, const char *root);
void tdb_close(tdb *db);
void tdb_dontneed(const tdb *db);
void tdb_willneed(const tdb *db);

uint64_t tdb_lexicon_size(const tdb *db, tdb_field field);

int tdb_get_field(const tdb *db, const char *field_name, tdb_field *field);
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

int64_t tdb_get_trail_id(const tdb *db, const uint8_t uuid[16]);

/* TODO deprecate this after binary search */
int tdb_has_uuid_index(const tdb *db);

int tdb_set_filter(tdb *db, const tdb_item *filter, uint64_t filter_len);
const tdb_item *tdb_get_filter(const tdb *db, uint64_t *filter_len);

const char *tdb_error(int errcode);

uint64_t tdb_num_trails(const tdb *db);
uint64_t tdb_num_events(const tdb *db);
uint64_t tdb_num_fields(const tdb *db);
uint64_t tdb_min_timestamp(const tdb *db);
uint64_t tdb_max_timestamp(const tdb *db);

uint64_t tdb_version(const tdb *db);

/* part of public api, to find uuids in partitions */
/* TODO deprecate this? */
#if 0
static inline unsigned int tdb_djb2(const uint8_t *str) {
  unsigned int hash = 5381, c;
  while ((c = *str++))
    hash = ((hash << 5) + hash) + c;
  return hash;
}
#endif

/* TODO pointers to tdb_decode could benefit from 'restrict' */
int tdb_decode_trail(const tdb *db,
                     uint64_t trail_id,
                     tdb_item *dst,
                     uint64_t dst_size,
                     uint64_t *num_items,
                     int edge_encoded);

int tdb_decode_trail_filtered(const tdb *db,
                              uint64_t trail_id,
                              tdb_item *dst,
                              uint64_t dst_size,
                              uint64_t *num_items,
                              int edge_encoded,
                              const tdb_item *filter,
                              uint64_t filter_len);

int tdb_get_trail(const tdb *db,
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
int tdb_get_trail_filtered(const tdb *db,
                           uint64_t trail_id,
                           tdb_item **items,
                           uint64_t *items_buf_len,
                           uint64_t *num_items,
                           int edge_encoded,
                           const tdb_item *filter,
                           uint64_t filter_len);

#endif /* __TRAILDB_H__ */
