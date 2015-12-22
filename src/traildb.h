
#ifndef __TRAILDB_H__
#define __TRAILDB_H__

#include <stdint.h>

#define TDB_MAX_PATH_SIZE   2048
#define TDB_MAX_FIELDNAME_LENGTH 512
#define TDB_MAX_ERROR_SIZE  (TDB_MAX_PATH_SIZE + 512)
#define TDB_MAX_NUM_TRAILS  (1LLU << 60)  // Lexicon needs C * 16 space
#define TDB_MAX_NUM_EVENTS  (1LLU << 54)  // Merge needs E * F * 4 space
#define TDB_MAX_NUM_FIELDS ((1LLU << 8) - 2)
#define TDB_MAX_NUM_VALUES ((1LLU << 24) - 2)
#define TDB_OVERFLOW_VALUE ((1LLU << 24) - 1)
#define TDB_MAX_VALUE_SIZE  (1LLU << 10)
#define TDB_MAX_LEXICON_SIZE UINT32_MAX
#define TDB_MAX_TIMEDELTA  ((1LLU << 24) - 2) // ~194 days
#define TDB_FAR_TIMEDELTA  ((1LLU << 24) - 1)
#define TDB_FAR_TIMESTAMP    UINT32_MAX


/* support a character set that allows easy urlencoding */
#define TDB_FIELDNAME_CHARS "_-%"\
                            "abcdefghijklmnopqrstuvwxyz"\
                            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"\
                            "0123456789"

#define TDB_OVERFLOW_STR   "OVERFLOW"
#define TDB_OVERFLOW_LSEP  '['
#define TDB_OVERFLOW_RSEP  ']'

#define TDB_VERSION_V0 0
#define TDB_VERSION_V0_1 1
#define TDB_VERSION_LATEST TDB_VERSION_V0_1

/*
   Internally we deal with ids:
    (id) trail_id  -> (bytes) uuid
    (id) field     -> (bytes) field_name
    (id) val       -> (bytes) value

   The complete picture looks like:

    uuid      -> trail_id
    trail_id  -> [event, ...]
    event     := [timestamp, item, ...]
    item      := (field, val)
    field     -> field_name
    val       -> value
*/

typedef uint8_t  tdb_field; //  8 bits
typedef uint32_t tdb_val;   // 24 bits
typedef uint32_t tdb_item;  // val, field
typedef struct _tdb_cons tdb_cons;
typedef struct _tdb tdb;

#define tdb_item_field(item) (item & 255)
#define tdb_item_val(item)   (item >> 8)

/* TODO: move flags from finalize() to new() */
tdb_cons *tdb_cons_new(const char *root,
                       const char **ofield_names,
                       uint32_t num_ofields);
void tdb_cons_free(tdb_cons *cons);

int tdb_cons_add(tdb_cons *cons,
                 const uint8_t uuid[16],
                 const uint32_t timestamp,
                 const char **values,
                 const uint32_t *value_lengths);

/* TODO: rename to tdb_cons_merge() */
int tdb_cons_append(tdb_cons *cons, const tdb *db);
int tdb_cons_finalize(tdb_cons *cons, uint64_t flags);

int tdb_uuid_raw(const uint8_t hexuuid[32], uint8_t uuid[16]);
int tdb_uuid_hex(const uint8_t uuid[16], uint8_t hexuuid[32]);

/* TODO: separate tdb_new() from tdb_open() */
/* TODO: add uint64_t flags to tdb_new() */
tdb *tdb_open(const char *root);
void tdb_close(tdb *db);
void tdb_dontneed(tdb *db);
void tdb_willneed(tdb *db);

uint32_t tdb_lexicon_size(const tdb *db, tdb_field field);

int tdb_get_field(tdb *db, const char *field_name);
const char *tdb_get_field_name(tdb *db, tdb_field field);

/* TODO deprecate this after wide fields */
int tdb_field_has_overflow_vals(tdb *db, tdb_field field);

tdb_item tdb_get_item(const tdb *db,
                      tdb_field field,
                      const char *value,
                      uint32_t value_length);

const char *tdb_get_value(const tdb *db,
                          tdb_field field,
                          tdb_val val,
                          uint32_t *value_length);

const char *tdb_get_item_value(const tdb *db,
                               tdb_item item,
                               uint32_t *value_length);

const uint8_t *tdb_get_uuid(const tdb *db, uint64_t trail_id);
int64_t tdb_get_trail_id(const tdb *db, const uint8_t uuid[16]);

int tdb_has_uuid_index(const tdb *db);

int tdb_set_filter(tdb *db, const uint32_t *filter, uint32_t filter_len);
const uint32_t *tdb_get_filter(const tdb *db, uint32_t *filter_len);

const char *tdb_error(const tdb *db);

uint64_t tdb_num_trails(const tdb *db);
uint64_t tdb_num_events(const tdb *db);
uint32_t tdb_num_fields(const tdb *db);
uint32_t tdb_min_timestamp(const tdb *db);
uint32_t tdb_max_timestamp(const tdb *db);

/* part of public api, to find uuids in partitions */
/* TODO deprecate this? */
static inline unsigned int tdb_djb2(const uint8_t *str) {
  unsigned int hash = 5381, c;
  while ((c = *str++))
    hash = ((hash << 5) + hash) + c;
  return hash;
}

/* TODO replace edge_encoded with uint64_t flags */
uint32_t tdb_decode_trail(const tdb *db,
                          uint64_t trail_id,
                          uint32_t *dst,
                          uint32_t dst_size,
                          int edge_encoded);

uint32_t tdb_decode_trail_filtered(const tdb *db,
                                   uint64_t trail_id,
                                   uint32_t *dst,
                                   uint32_t dst_size,
                                   int edge_encoded,
                                   const uint32_t *filter,
                                   uint32_t filter_len);

int tdb_get_trail(const tdb *db,
                  uint64_t trail_id,
                  tdb_item **items,
                  uint32_t *items_buf_len,
                  uint32_t *num_items,
                  int edge_encoded);

/*TODO  change filtered to take a filter struct */
int tdb_get_trail_filtered(const tdb *db,
                           uint64_t trail_id,
                           tdb_item **items,
                           uint32_t *items_buf_len,
                           uint32_t *num_items,
                           int edge_encoded,
                           const uint32_t *filter,
                           uint32_t filter_len);

#endif /* __TRAILDB_H__ */
