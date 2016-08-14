
#define _DEFAULT_SOURCE /* strdup(), mkstemp() */

#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <assert.h>
#include <unistd.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include <Judy.h>

#include <traildb.h>
#include <tdb_io.h>
#include <tdb_bits.h>
#include <xxhash/xxhash.h>

#include "thread_util.h"
#include "tdbcli.h"

#include "tdb_index.h"

/*

# TDB Index

Tdb_index is a simple mapping

tdb_item -> [trail_id, ...]

indicating trails that have at least one occurrence of the item. An
inverted index like this is especially useful for queries that match a
small number of trails based on infrequent items. By using the index,
one can avoid checking every trail in the db ("full table scan").
However, a TrailDB can contain tens of millions of items and trails, so
creating and storing this mapping can be expensive.

To make the index faster and smaller, instead of using the mapping above
we partition TrailDB to 2^16 = 65536 pages. Each page contains min(1,
num_trails / 2^16) trails. Hence the optimized index mapping becomes

tdb_item -> [page_id, ...]

By definition, page_id can be represented with a uint16_t that saves
space compared to a uint64_t trail_id. To use the index, we need to
check every trail in the page for possible matches. This is less costly
than it sounds: processing TrailDBs is typically bounded by disk or
memory bandwidth. To access a single trail, we need to read at least one
OS page (4KB, or more in the case of SSD pages) of data anyways. Hence,
the optimized page-level index should perform almost as well as the
item-level index, while being much cheaper to construct and store.

## Index Binary Format

HEADER
    [ header           (sizeof(struct header)) ]
    [ field 0 offset   (8 bytes) ]
    ...
    [ field M offset   ) ]
FIELD SECTION
    PAGES
        [ item 0 num_pages] [ item 0 page_id, ... (list of 2 byte values) ]
        ...
        [ item K num_pages ] [ item K page_id, ... ]
    OFFSETS
        [ are offsets 4 or 8 bytes (4 bytes) ]
        [ item 0 offset (4 or 8 bytes) ]
        ...
        [ item K offset ]

To find the list of candidate trails for an item X, we need to perform
the following steps:

1. Find the correct FIELD SECTION for X using the HEADER.
2. Use OFFSETS to find the list of pages in the FIELD SECTION.
3. Read the list of pages and expand each page to all trail_ids it contains.

Thus, looking up the list of pages for an item is an O(1) operation.

## Optimizing Index Construction

### Optimization 1) Multi-threading

We construct the mapping

tdb_item -> [page_id, ...]

by iterating over all trails in the TrailDB. We can shard a TrailDB to
K shards and perform this operation in K threads in parallel. Since we
don't know which items occur in which shards, we need to maintain a
dynamic mapping (JudyL), keyed by tdb_item, in each shard. The value of
JudyL needs to be a dynamically growing list of some kind.

### Optimization 2) Dense packing of small lists

A straightforward implementation of a dictionary of lists incurs a
large number of small allocations that are relatively expensive. We can
optimize away a good number of these allocations if we assume that there
is a long tail of infrequently occurring items which is often the case
with real-world TrailDBs.

JudyL is a mapping uint64_t -> uint64_t. Thus, we can store a list of
maximum four 16-bit page_ids in a single value of the mapping. We call
this specially packed mapping `small_items`. If an item has more than
four page ids, we spill over the rest of pages to a separate mapping,
`large_items`, which is a straightforward but more expensive,
JudyL -> Judy1 -> PageID mapping.

Each thread constructs its `small_items` and `large_items` mappings
independently for the pages in its shard. Once all the threads have
finished, we can merge results as an O(N) operation, since all page_ids
are already stored in the sorted order.

### Optimization 3) Deduplication of lists

Typically there are many items in the mapping that have exactly the same
value, i.e. the list of pages where the item occurs. Storing duplicate
lists is redundant. We can save space by storing only distinct lists
and updating OFFSETS to point at the shared list. This optimization is
applied only to values of `small_items` i.e. only to lists of up to four
pages.
*/

/* UINT16_MAX as unsigned long long */
#define UINT16_MAX_LLU 65535LLU
/* Number of distinct page_ids - we reserve page_id=0 for special use */
#define INDEX_NUM_PAGES (UINT16_MAX - 1)
/* Version identifier for forward compatibility */
#define INDEX_VERSION 1

struct job_arg{
    /* input */
    const char *tdb_path;
    uint64_t trails_per_page;
    uint64_t start_trail;
    uint64_t end_trail;

    /* output */
    Pvoid_t small_items;
    Pvoid_t large_items;
};

struct header{
    uint64_t version;
    uint64_t checksum;
    uint64_t trails_per_page;
    uint64_t field_offsets[0];
} __attribute__((packed));

struct tdb_index{
    const struct header *head;
    const void *data;
    uint64_t size;
    uint64_t num_trails;
};

/*
Pack a 16-bit page identifier `page` in the 64-bit value `old_val`.

Return 0 if the value is already full.
*/
static inline uint64_t add_small(Word_t *old_val, uint64_t page)
{
    uint64_t v0 = *old_val & UINT16_MAX_LLU;
    uint64_t v1 = *old_val & (UINT16_MAX_LLU << 16LLU);
    uint64_t v2 = *old_val & (UINT16_MAX_LLU << 32LLU);
    uint64_t v3 = *old_val & (UINT16_MAX_LLU << 48LLU);

    page &= UINT16_MAX_LLU;

    if (!v0){
        *old_val = page;
        return 0;
    }else if (!v1){
        if (v0 != page)
            *old_val |= page << 16LLU;
    }else if (!v2){
        if (v1 != (page << 16LLU))
            *old_val |= page << 32LLU;
    }else if (!v3){
        if (v2 != (page << 32LLU))
            *old_val |= page << 48LLU;
    }else{
        if (v3 != (page << 48LLU))
            return 1;
    }
    return 0;
}

/*
Unpack four 16-bit page IDs from a 64-bit value `old_val`.
*/
static inline void get_small(Word_t old_val,
                             uint16_t *v0,
                             uint16_t *v1,
                             uint16_t *v2,
                             uint16_t *v3)
{
    *v0 = old_val & UINT16_MAX_LLU;
    *v1 = ((old_val & (UINT16_MAX_LLU << 16LLU)) >> 16LLU) & UINT16_MAX;
    *v2 = ((old_val & (UINT16_MAX_LLU << 32LLU)) >> 32LLU) & UINT16_MAX;
    *v3 = ((old_val & (UINT16_MAX_LLU << 48LLU)) >> 48LLU) & UINT16_MAX;
}

/*
Construct

tdb_item -> [page_id, ...]

mapping for a single shard of pages.
*/
static void *job_index_shard(void *arg0)
{
    struct job_arg *arg = (struct job_arg*)arg0;
    uint64_t i, j;
    tdb_error err;

    tdb* db = tdb_init();
    if ((err = tdb_open(db, arg->tdb_path))){
        DIE("Opening TrailDB failed: %s", tdb_error_str(err));
    }

    tdb_cursor *cursor = tdb_cursor_new(db);

    for (i = arg->start_trail; i < arg->end_trail; i++){
        const uint16_t page = 1 + i / arg->trails_per_page;
        const tdb_event *event;

        if (tdb_get_trail(cursor, i))
            DIE("get_trail %"PRIu64" failed", i);

        while ((event = tdb_cursor_next(cursor))){
            for (j = 0; j < event->num_items; j++){
                Word_t *ptr;
                JLI(ptr, arg->small_items, event->items[j]);
                if (add_small(ptr, page)){
                    int tst;
                    JLI(ptr, arg->large_items, event->items[j]);
                    Pvoid_t large = (Pvoid_t)*ptr;
                    J1S(tst, large, page);
                    *ptr = (Word_t)large;
                }
            }
        }
    }

    tdb_cursor_free(cursor);
    tdb_close(db);
    return NULL;
}

/*
Write one FIELD SECTION (see above for details), given
the mapping:

tdb_item -> [page_id, ...]
*/
static uint64_t write_field(FILE *out,
                            const tdb *db,
                            tdb_field field,
                            const struct job_arg *args,
                            uint64_t num_shards,
                            Pvoid_t *dedup0)
{
    const uint64_t num_items = tdb_lexicon_size(db, field);
    uint64_t i, j;
    long offset;
    Pvoid_t dedup = *dedup0;
    int __attribute__((unused)) ret; /* for TDB_WRITE */

    struct shard_data{
        uint16_t v0, v1, v2, v3;
        Word_t *small_ptr;
        Word_t *large_ptr;
    } *shards;

    uint64_t *offsets;

    if ((offset = ftell(out)) == -1)
        DIE("Getting file offset failed");

    if (!(offsets = calloc(num_items, 8)))
        DIE("Could not allocate offsets for %"PRIu64" items in field %u",
            num_items,
            field);

    if (!(shards = malloc(num_shards * sizeof(struct shard_data))))
        DIE("Could not allocate shard data");

    /* write pages for each item in this field */
    for (i = 0; i < num_items; i++){
        const tdb_item item = tdb_make_item(field, i);
        uint16_t key[] = {0, 0, 0, 0};
        int key_idx = 0;
        uint64_t prev = 0;

        /* get data for `item` from each shard - order matters! */
        memset(shards, 0, num_shards * sizeof(struct shard_data));
        for (j = 0; j < num_shards; j++){
            JLG(shards[j].small_ptr, args[j].small_items, item);
            /* collect small values of this mapping */
            if (shards[j].small_ptr){
                get_small(*shards[j].small_ptr,
                          &shards[j].v0,
                          &shards[j].v1,
                          &shards[j].v2,
                          &shards[j].v3);
                if (shards[j].v0){
                    if (key_idx < 4)
                        key[key_idx] = shards[j].v0;
                    ++key_idx;
                }
                if (shards[j].v1){
                    if (key_idx < 4)
                        key[key_idx] = shards[j].v1;
                    ++key_idx;
                }
                if (shards[j].v2){
                    if (key_idx < 4)
                        key[key_idx] = shards[j].v2;
                    ++key_idx;
                }
                if (shards[j].v3){
                    if (key_idx < 4)
                        key[key_idx] = shards[j].v3;
                    ++key_idx;
                    JLG(shards[j].large_ptr, args[j].large_items, item);
                    if (shards[j].large_ptr)
                        key_idx = 5;
                }
            }
        }

        if (key_idx < 5){
            /*
            if this item doesn't have large items, it is eligible
            for deduplication (optimization 3 above). If this exact
            list is already stored, we can just update the offset of
            the item and continue.
            */
            Word_t dedup_key;
            Word_t *ptr;
            memcpy(&dedup_key, key, 8);
            JLI(ptr, dedup, dedup_key);
            if (*ptr){
                offsets[i] = *ptr;
                continue;
            }else
                *ptr = offset;
        }

        /*
        write the list of page ids, starting from the
        contents of the small_items.
        */
        offsets[i] = offset;
        /* write placeholder for the num pages */
        TDB_WRITE(out, &prev, 2);
        offset += 2;
        for (j = 0; j < num_shards; j++){
            if (shards[j].v0){
                assert(shards[j].v0 > prev);
                prev = shards[j].v0;
                TDB_WRITE(out, &prev, 2);
                offset += 2;
            }
            if (shards[j].v1){
                assert(shards[j].v1 > prev);
                prev = shards[j].v1;
                TDB_WRITE(out, &prev, 2);
                offset += 2;
            }
            if (shards[j].v2){
                assert(shards[j].v2 > prev);
                prev = shards[j].v2;
                TDB_WRITE(out, &prev, 2);
                offset += 2;
            }
            if (shards[j].v3){
                assert(shards[j].v3 > prev);
                prev = shards[j].v3;
                TDB_WRITE(out, &prev, 2);
                offset += 2;
            }
            if (shards[j].large_ptr){
                /*
                write the list of page ids, continue with
                large_items
                */
                const Pvoid_t large = (const Pvoid_t)*shards[j].large_ptr;
                Word_t key = 0;
                int tst;

                J1F(tst, large, key);
                while (tst){
                    assert(key > prev);
                    prev = key;
                    TDB_WRITE(out, &prev, 2);
                    offset += 2;
                    J1N(tst, large, key);
                }
            }
        }
        TDB_SEEK(out, offsets[i]);
        uint32_t num_pages = (uint32_t)(((offset - offsets[i]) / 2) - 1);
        TDB_WRITE(out, &num_pages, 2);
        TDB_SEEK(out, offset);
    }

    /*
    if all offsets fit in uint32_t, write them as 4 byte values,
    otherwise use 8 bytes. Indicate the choice in the first 4 bytes.
    */
    if (offset > UINT32_MAX){
        const uint32_t EIGHT = 8;
        TDB_WRITE(out, &EIGHT, 4);
        TDB_WRITE(out, offsets, num_items * 8);
    }else{
        const uint32_t FOUR = 4;
        TDB_WRITE(out, &FOUR, 4);
        for (i = 0; i < num_items; i++)
            TDB_WRITE(out, &offsets[i], 4);
    }

    free(offsets);
    free(shards);
    *dedup0 = dedup;
    return offset;
done:
    DIE("IO error when writing index (disk full?)");
}

/*
Produce a sanity check of a checksum that can be used to make sure that
the index matches with the db it was based on.
*/
static uint64_t db_checksum(const tdb *db)
{
    XXH64_state_t hash_state;
    uint64_t data[] = {tdb_num_trails(db),
                       tdb_num_events(db),
                       tdb_num_fields(db),
                       tdb_min_timestamp(db),
                       tdb_max_timestamp(db),
                       tdb_version(db)};
    XXH64_reset(&hash_state, 2016);
    XXH64_update(&hash_state, data, sizeof(data));
    return XXH64_digest(&hash_state);
}

/*
Initialize the index blob.
*/
static FILE *init_file(const tdb *db,
                       char *path,
                       uint64_t trails_per_page)
{
    FILE *out;
    int fd;
    struct header head = {.version = INDEX_VERSION,
                          .checksum = db_checksum(db),
                          .trails_per_page = trails_per_page};
    int __attribute__((unused)) ret; /* TDB_WRITE */

    if ((fd = mkstemp(path)) == -1)
        DIE("Could not open temp file at %s", path);

    if (!(out = fdopen(fd, "w")))
        DIE("Could not create an index at %s", path);

    TDB_WRITE(out, &head, sizeof(struct header));
    return out;
done:
    DIE("Writing index header failed. Disk full?");
}

/*
Write the index to disk. See above for the specification of
the binary format.
*/
static void write_index(FILE *out,
                        const tdb *db,
                        const struct job_arg *args,
                        uint64_t num_shards)
{
    Pvoid_t dedup = NULL;
    Word_t tmp;
    uint64_t offset, i;
    uint64_t *offsets;
    const uint64_t num_fields = tdb_num_fields(db);
    int __attribute__((unused)) ret; /* for TDB_WRITE */

    if ((offset = ftell(out)) == -1)
        DIE("Getting file offset failed");

    if (!(offsets = calloc(num_fields, 8)))
        DIE("Could not allocate field offsets");

    TDB_WRITE(out, offsets, num_fields * 8);

    for (i = 1; i < tdb_num_fields(db); i++)
        offsets[i] = write_field(out, db, i, args, num_shards, &dedup);

    TDB_SEEK(out, offset);
    TDB_WRITE(out, offsets, num_fields * 8);

    free(offsets);
    JLFA(tmp, dedup);
    return;
done:
    DIE("Writing index failed. Disk full?");
}

/*
Get pages for the given item.
*/
static const uint16_t *get_index_pages(const struct tdb_index *index,
                                       tdb_item item,
                                       uint16_t *num_pages)
{
    tdb_val idx = tdb_item_val(item);
    uint64_t field_offset = index->head->field_offsets[tdb_item_field(item)];
    const void *data = index->data + field_offset;
    uint32_t width;

    if (!field_offset)
        return NULL;

    memcpy(&width, data, 4);
    if (width == 4){
        const uint32_t *offsets = data + 4;
        memcpy(num_pages, index->data + offsets[idx], 2);
        return (const uint16_t*)(index->data + offsets[idx] + 2);
    }else if (width == 8){
        const uint64_t *offsets = data + 4;
        memcpy(num_pages, index->data + offsets[idx], 2);
        return (const uint16_t*)(index->data + offsets[idx] + 2);
    }else
        DIE("Corrupted index (item %"PRIu64")", item);
}

/*
Perform a bitwise-AND operation between two bitmaps.
*/
static void intersect(char *dst, const char *src, uint64_t len)
{
    uint64_t i;
    for (i = 0; i < len; i++)
        dst[i] &= src[i];
}

/*
Return a list of trail IDs that can contain matches for the given filter
(some may be false positives, due to the index being at the page level,
not at the exact item level).
*/
uint64_t *tdb_index_match_candidates(const struct tdb_index *index,
                                     const struct tdb_event_filter *filter,
                                     uint64_t *num_candidates)
{
    const uint64_t BUFFER_SIZE = 1 + INDEX_NUM_PAGES / 8;
    char *conjunction, *disjunction;
    uint64_t *candidates;
    uint64_t i, j, k;

    if (!(conjunction = malloc(BUFFER_SIZE)))
        DIE("Could not allocate conjunction buffer");
    memset(conjunction, 0xff, BUFFER_SIZE);

    if (!(disjunction = calloc(1, BUFFER_SIZE)))
        DIE("Could not allocate disjunction buffer");

    /*
    We can pre-evaluate CNF queries at the page level:
    Each clause (disjunction) is evaluated by constructing a bitmap
    that represents the union of pages in the clause. Clauses are
    combined together by conjunction, i.e. by producing the intersection
    between the clauses with bitwise-AND.

    Page-level negation is a special case: We need to evaluate each
    trail for negations, so the page-level index is useless for negations.
    */
    for (i = 0; i < tdb_event_filter_num_clauses(filter); i++){
        tdb_item item;
        int is_negative;
        memset(disjunction, 0, BUFFER_SIZE);
        j = 0;

        while (!tdb_event_filter_get_item(filter, i, j, &item, &is_negative)){
            if (is_negative){
                memset(disjunction, 0xff, BUFFER_SIZE);
                break;
            }else{
                /* union this item with the previous ones in this clause */
                uint16_t n;
                const uint16_t *pages = get_index_pages(index, item, &n);
                for (k = 0; k < n; k++)
                    write_bits(disjunction, pages[k] - 1, 1);
            }
            ++j;
        }
        /* intersect this clause with the previous clauses */
        intersect(conjunction, disjunction, BUFFER_SIZE);
    }

    *num_candidates = 0;
    for (i = 0; i < INDEX_NUM_PAGES; i++)
        if (read_bits(conjunction, i, 1))
            *num_candidates += index->head->trails_per_page;

    if (!(candidates = malloc(*num_candidates * 8)))
        DIE("Could not allocate a buffer for %"PRIu64" match candidates",
            *num_candidates);

    for (k = 0, i = 0; i < INDEX_NUM_PAGES; i++){
        if (read_bits(conjunction, i, 1)){
            for (j = 0; j < index->head->trails_per_page; j++){
                uint64_t trail_id = i * index->head->trails_per_page + j;
                if (trail_id < index->num_trails)
                    candidates[k++] = trail_id;
            }
        }
    }
    free(conjunction);
    free(disjunction);

    *num_candidates = k;
    return candidates;
}

/*
Utility function that tests if any of the canonical index
paths are found for the given TrailDB path.
*/
char *tdb_index_find(const char *root)
{
    char path[TDB_MAX_PATH_SIZE];
    int fd = 0;
    int __attribute__((unused)) ret; /* for TDB_PATH */

    TDB_PATH(path, "%s/index", root);
    if ((fd = open(path, O_RDONLY)) != -1)
        goto found;

    TDB_PATH(path, "%s.index", root);
    if ((fd = open(path, O_RDONLY)) != -1)
        goto found;

    TDB_PATH(path, "%s.tdb.index", root);
    if ((fd = open(path, O_RDONLY)) != -1)
        goto found;

    return NULL;
found:
    if (fd)
        close(fd);
    return strdup(path);
done:
    DIE("Path %s too long", root);
}

/*
Open an index.
*/
struct tdb_index *tdb_index_open(const char *tdb_path, const char *index_path)
{
    int fd;
    struct stat stats;
    struct tdb_index *index;
    tdb* db = tdb_init();
    tdb_error err;

    if ((err = tdb_open(db, tdb_path))){
        DIE("Opening TrailDB at %s failed: %s\n",
            tdb_path,
            tdb_error_str(err));
    }

    if (!(index = malloc(sizeof(struct tdb_index))))
        DIE("Could not allocate a new index handle");

    if ((fd = open(index_path, O_RDONLY)) == -1)
        DIE("Opening index at %s failed", index_path);

    if (fstat(fd, &stats))
        DIE("Reading index at %s failed", index_path);

    index->size = (uint64_t)stats.st_size;

    if (index->size > 0)
        index->data = mmap(NULL, index->size, PROT_READ, MAP_SHARED, fd, 0);

    if (index->data == MAP_FAILED)
        DIE("Memory mapping index at %s failed", index_path);

    index->head = (const struct header*)index->data;
    index->num_trails = tdb_num_trails(db);

    if (db_checksum(db) != index->head->checksum)
        DIE("TrailDB at %s and index at %s mismatch", tdb_path, index_path);

    close(fd);
    tdb_close(db);
    return index;
}

/*
Close an index.
*/
void tdb_index_close(struct tdb_index *index)
{
    munmap((char*)index->data, index->size);
    free(index);
}

/*
Create an index.
*/
tdb_error tdb_index_create(const char *db_path,
                           const char *index_path,
                           uint32_t num_shards)
{
    char tmp_path[TDB_MAX_PATH_SIZE];
    struct job_arg *args;
    struct thread_job *jobs;
    tdb* db = tdb_init();
    tdb_error err;
    uint64_t i;
    int ret;

    if ((err = tdb_open(db, db_path))){
        DIE("Opening TrailDB at %s failed: %s\n",
            db_path,
            tdb_error_str(err));
    }

    const uint64_t num_trails = tdb_num_trails(db);
    const uint64_t trails_per_page = 1 + num_trails / INDEX_NUM_PAGES;
    const uint64_t pages_per_shard = 1 + (INDEX_NUM_PAGES / num_shards);

    TDB_PATH(tmp_path, "%s.tmp.XXXXXX", index_path);
    FILE *out = init_file(db, tmp_path, trails_per_page);

    if (!(args = calloc(num_shards, sizeof(struct job_arg))))
        DIE("Couldn't allocate %u features args\n", num_shards);

    if (!(jobs = calloc(num_shards, sizeof(struct thread_job))))
        DIE("Couldn't allocate %u features jobs\n", num_shards);

    for (i = 0; i < num_shards; i++){
        args[i].tdb_path = db_path;
        args[i].trails_per_page = trails_per_page;
        args[i].start_trail = i * pages_per_shard * trails_per_page;
        args[i].end_trail = (i + 1) * pages_per_shard * trails_per_page;
        if (args[i].end_trail > num_trails)
            args[i].end_trail = num_trails;

        jobs[i].arg = &args[i];
    }

    /* construct the item -> pages mapping on K threads in parallel */
    execute_jobs(job_index_shard, jobs, num_shards, num_shards);
    /* write the mapping to disk */
    write_index(out, db, args, num_shards);

    if (fflush(out) || fclose(out))
        DIE("Closing index failed. Disk full?");

    if (rename(tmp_path, index_path))
        DIE("Renaming %s -> %s failed", tmp_path, index_path);

    /* FIXME free args.small_items and args.large_items */
    tdb_close(db);
    return 0;
done:
    DIE("Path name too long");
}

#if 0
int main(int argc, char **argv)
{
    create_index(argv[1], argv[2]);
    const struct tdb_index *index = open_index(argv[1], argv[2]);
    uint16_t i, num;
    const uint16_t *pages = get_index_pages(index, 22761987, &num);
    printf("pages:");
    for (i = 0; i < num; i++)
        printf(" %u", pages[i]);
    printf("\n");
}
#endif
