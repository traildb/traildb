
#ifndef __TDB_INTERNAL_H__
#define __TDB_INTERNAL_H__

#include <stdint.h>

#include "traildb.h"
#include "arena.h"
#include "judy_str_map.h"
#include "judy_128_map.h"
#include "tdb_profile.h"
#include "tdb_io.h"

#define TDB_EXPORT __attribute__((visibility("default")))

/*
These are defined by autoconf

Nothing has been tested on 32-bit systems so it is
better to fail loudly for now.
*/

struct tdb_cons_event{
    uint64_t item_zero;
    uint64_t num_items;
    uint64_t timestamp;
    uint64_t prev_event_idx;
};

/*
 Used to build up a CNF expression for filtering events
 */
struct tdb_event_filter{
    uint64_t count; /* number of terms in current clause */
    uint64_t size; /* amount of allocated space for items = sizeof(tdb_item) * size */
    uint64_t clause_len_idx; /* idx for storing number of terms in current clause */
    tdb_item *items; /* array of filters for CNF expression. Each clause in the list
                        starts with a number indicating the number of terms in the clause,
                        following by the terms.

                        Each term starts with a term type flag. For a matching term, a
                        tdb_item containing the field and value to match follows. For
                        time-range filters, the term type flag is followed by two entries,
                        the start and end timestamps. */
                        
};

/* 
 Flags for types of term comparisons.  We currently support two types of terms: 
 matching terms and time-range filters.  Terms fall into two types: items
 and timestamps.  If the TIME_RANGE flag is not set, then the item is
 interpreted as a tdb_item and matched for equality (based on the NEGATED) flag.
 If the TIME_RANGE flag is set, then the item is interpreted as an uint64_t
 timestamp and NEGATED is ignored.
*/
typedef enum {
    TDB_EVENT_NEGATED = 1,
    TDB_EVENT_TIME_RANGE = 2
} tdb_event_op_flags;

struct tdb_decode_state{
    const tdb *db;

    /* internal buffer */
    void *events_buffer;
    uint64_t events_buffer_len;

    /* trail state */
    uint64_t trail_id;
    const char *data;
    uint64_t size;
    uint64_t offset;
    uint64_t tstamp;

    /* options */
    const tdb_item *filter;
    uint64_t filter_len;
    uint64_t filter_size;

    int edge_encoded;

    tdb_item previous_items[0];
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

    /* options */

    uint64_t output_format;
};

struct tdb_file {
    char *ptr;
    const char *data;
    uint64_t size;
    uint64_t mmap_size;
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
    struct tdb_file codebook;
    struct tdb_file trails;
    struct tdb_file toc;
    struct tdb_file *lexicons;

    char **field_names;
    struct field_stats *field_stats;

    uint64_t version;

    /* tdb_package */

    FILE *package_handle;
    void *package_toc;

    /* options */

    /* TDB_OPT_CURSOR_EVENT_BUFFER_SIZE */
    uint64_t opt_cursor_event_buffer_size;
    /* TDB_OPT_ONLY_DIFF_ITEMS */
    int opt_edge_encoded;
    /* TDB_OPT_EVENT_FILTER */
    const struct tdb_event_filter *opt_event_filter;

};

void tdb_lexicon_read(const tdb *db, tdb_field field, struct tdb_lexicon *lex);

const char *tdb_lexicon_get(const struct tdb_lexicon *lex,
                            tdb_val i,
                            uint64_t *length);

tdb_error tdb_encode(tdb_cons *cons, const tdb_item *items);

tdb_error edge_encode_items(const tdb_item *items,
                            tdb_item **encoded,
                            uint64_t *num_encoded,
                            uint64_t *encoded_size,
                            tdb_item *prev_items,
                            const struct tdb_grouped_event *ev);

int file_mmap(const char *path,
              const char *root,
              struct tdb_file *dst,
              const tdb *db);

int is_fieldname_invalid(const char* field);

#endif /* __TDB_INTERNAL_H__ */
