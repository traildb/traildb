
#ifndef __TDB_INTERNAL_H__
#define __TDB_INTERNAL_H__

#include <stdint.h>

#include "traildb.h"
#include "arena.h"
#include "judy_str_map.h"
#include "judy_128_map.h"
#include "tdb_profile.h"
#include "tdb_io.h"

/*
These are defined by autoconf

Nothing has been tested on 32-bit systems so it is
better to fail loudly for now.
*/
#if SIZEOF_OFF_T != 8 || SIZEOF_SIZE_T != 8
    #error "sizeof(off_t) and sizeof(size_t) must be 8"
#endif

struct tdb_cons_event{
    uint64_t item_zero;
    uint64_t num_items;
    uint64_t timestamp;
    uint64_t prev_event_idx;
};

struct tdb_grouped_event{
    uint64_t item_zero;
    uint64_t num_items;
    uint64_t timestamp;
    uint64_t trail_id;
};

struct _tdb_cons {
    char *root;
    struct arena events;
    struct arena items;

    char **ofield_names;

    uint64_t min_timestamp;
    uint64_t num_ofields;

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
    uint64_t size;
    uint64_t width;
    union {
        const uint32_t *toc32;
        const uint64_t *toc64;
    } toc;
    const char *data;
};

struct _tdb {
    uint64_t min_timestamp;
    uint64_t max_timestamp;
    uint64_t max_timestamp_delta;
    uint64_t num_trails;
    uint64_t num_events;
    uint64_t num_fields;

    struct tdb_file uuids;
    struct tdb_file uuid_index;
    struct tdb_file codebook;
    struct tdb_file trails;
    struct tdb_file toc;
    struct tdb_file *lexicons;

    char **field_names;
    struct field_stats *field_stats;

    /* move these to a separate tdb_opt object */
    tdb_item *filter;
    /* TODO add and check MAX_FILTER_LEN */
    uint64_t filter_len;

    tdb_item *previous_items;

    uint64_t version;
};

void tdb_lexicon_read(const tdb *db, tdb_field field, struct tdb_lexicon *lex);

const char *tdb_lexicon_get(const struct tdb_lexicon *lex,
                            tdb_val i,
                            uint64_t *length);

tdb_error tdb_encode(tdb_cons *cons, tdb_item *items);

tdb_error edge_encode_items(const tdb_item *items,
                            tdb_item **encoded,
                            uint64_t *num_encoded,
                            uint64_t *encoded_size,
                            tdb_item *prev_items,
                            const struct tdb_grouped_event *ev);

int tdb_mmap(const char *path, struct tdb_file *dst);

int is_fieldname_invalid(const char* field);

#endif /* __TDB_INTERNAL_H__ */
