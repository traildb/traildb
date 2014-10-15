
#ifndef __TRAILDB_H__
#define __TRAILDB_H__

#include <stdint.h>

#define TDB_MAX_PATH_SIZE  1024
#define TDB_MAX_ERROR_SIZE (TDB_MAX_PATH_SIZE + 512)
#define TDB_MAX_FIELD_SIZE 1024
#define TDB_MAX_NUM_FIELDS 255
#define TDB_MAX_NUM_VALUES ((1 << 24) - 1)

/*
   Internally we deal with ids:
    (id) cookie_id -> (bytes) cookie
    (id) field     -> (bytes) field_name
    (id) val       -> (bytes) value

   The complete picture looks like:

    cookie    -> cookie_id
    cookie_id -> [event, ...]
    event     := [item, ...]
    item      := (field, val)
    field     -> field_name
    val       -> value
*/

typedef const uint8_t * tdb_cookie; /* 16 bytes */
typedef uint8_t         tdb_field;
typedef uint32_t        tdb_val;
typedef uint32_t        tdb_item;

#define tdb_item_field(item) (item & 255)
#define tdb_item_val(item)   (item >> 8)

typedef struct {
    const char *data;
    uint64_t size;
} tdb_file;

typedef struct {
    uint32_t min_timestamp;
    uint32_t max_timestamp;
    uint32_t max_timestamp_delta;
    uint64_t num_cookies;
    uint64_t num_events;
    uint32_t num_fields;
    uint32_t *previous_values;

    tdb_file cookies;
    tdb_file cookie_index;
    tdb_file codebook;
    tdb_file trails;
    tdb_file *lexicons;

    const char **field_names;
    struct field_stats *field_stats;

    int error_code;
    char error[TDB_MAX_ERROR_SIZE];
} tdb;

typedef struct {
    uint32_t size;
    const uint32_t *toc;
    const char *data;
} tdb_lexicon;

typedef struct {
    uint64_t item_zero;
    uint32_t num_items;
    uint32_t timestamp;
    uint64_t prev_event_idx;
} tdb_event;

typedef struct {
    uint64_t item_zero;
    uint32_t num_items;
    uint32_t timestamp;
    uint64_t cookie_id;
} tdb_cookie_event;

tdb *tdb_open(const char *root);
void tdb_close(tdb *db);

int tdb_lexicon_read(tdb *db, tdb_lexicon *lex, tdb_field field);
int tdb_lexicon_size(tdb *db, tdb_field field, uint32_t *size);

int tdb_get_field(tdb *db, const char *field_name);
const char *tdb_get_field_name(tdb *db, tdb_field field);

tdb_item tdb_get_item(tdb *db, tdb_field field, const char *value);
const char *tdb_get_value(tdb *db, tdb_field field, tdb_val val);
const char *tdb_get_item_value(tdb *db, tdb_item item);

tdb_cookie tdb_get_cookie(tdb *db, uint64_t cookie_id);
int64_t tdb_get_cookie_id(tdb *db, const tdb_cookie cookie);
int tdb_has_cookie_index(tdb *db);

const char *tdb_error(const tdb *db);

uint64_t tdb_num_cookies(const tdb *db);
uint64_t tdb_num_events(const tdb *db);
uint32_t tdb_num_fields(const tdb *db);
uint32_t tdb_min_timestamp(const tdb *db);
uint32_t tdb_max_timestamp(const tdb *db);

uint32_t tdb_decode_trail(tdb *db,
                          uint64_t cookie_id,
                          uint32_t *dst,
                          uint32_t dst_size,
                          int raw_values);

#endif /* __TRAILDB_H__ */
