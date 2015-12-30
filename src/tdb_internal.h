
#ifndef __TDB_INTERNAL_H__
#define __TDB_INTERNAL_H__

#include <stdint.h>

#include <Judy.h>

#include "traildb.h"
#include "arena.h"
#include "judy_str_map.h"
#include "judy_128_map.h"
#include "tdb_profile.h"

/*
These are defined by autoconf

Nothing has been tested on 32-bit systems so it is
better to fail loudly for now.
*/
#if SIZEOF_OFF_T != 8 || SIZEOF_SIZE_T != 8
    #error "sizeof(off_t) and sizeof(size_t) must be 8"
#endif

typedef struct {
    uint64_t item_zero;
    uint32_t num_items;
    uint32_t timestamp;
    uint64_t prev_event_idx;
} tdb_cons_event;

typedef struct {
    uint64_t item_zero;
    uint32_t num_items;
    uint32_t timestamp;
    uint64_t trail_id;
} tdb_event;

struct _tdb_cons {
    char *root;
    struct arena events;
    struct arena items;

    char **ofield_names;

    uint32_t min_timestamp;
    uint32_t num_ofields;

    struct judy_128_map trails;
    struct judy_str_map *lexicons;

    char tempfile[TDB_MAX_PATH_SIZE];
};

struct tdb_file {
    char *data;
    uint64_t size;
};

struct tdb_lexicon {
    uint64_t version;
    uint32_t size;
    const uint32_t *toc;
    const char *data;
};

struct _tdb {
    uint32_t min_timestamp;
    uint32_t max_timestamp;
    uint32_t max_timestamp_delta;
    uint64_t num_trails;
    uint64_t num_events;
    uint32_t num_fields;
    uint32_t *previous_items;

    struct tdb_file uuids;
    struct tdb_file uuid_index;
    struct tdb_file codebook;
    struct tdb_file trails;
    struct tdb_file toc;
    struct tdb_file *lexicons;

    char **field_names;
    struct field_stats *field_stats;

    uint32_t *filter;
    uint32_t filter_len;

    int error_code;
    char error[TDB_MAX_ERROR_SIZE];

    uint64_t version;
};

#if 0
struct _tdb_split {
    tdb_cons **cons;
    tdb_fold_fn split_fn;
    unsigned int num_parts;
    char *values;
};
#endif

void tdb_lexicon_read(const tdb *db, tdb_field field, struct tdb_lexicon *lex);

const char *tdb_lexicon_get(const struct tdb_lexicon *lex,
                            uint32_t i,
                            uint32_t *length);

void tdb_err(tdb *db, char *fmt, ...);
void tdb_path(char path[TDB_MAX_PATH_SIZE], char *fmt, ...);
int tdb_mmap(const char *path, struct tdb_file *dst, tdb *db);

int tdb_encode(tdb_cons *cons, tdb_item *items);

uint32_t edge_encode_items(const tdb_item *items,
                           uint32_t **encoded,
                           uint32_t *encoded_size,
                           tdb_item *prev_items,
                           const tdb_event *ev);

uint64_t tdb_get_trail_offs(const tdb *db, uint64_t trail_id);

#endif /* __TDB_INTERNAL_H__ */
