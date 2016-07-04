
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

#define UINT16_MAX_LLU 65535LLU
#define INDEX_NUM_PAGES (UINT16_MAX - 1)
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
};

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

static void *job_index_shard(void *arg0)
{
    struct job_arg *arg = (struct job_arg*)arg0;
    uint64_t i, j;
    tdb_error err;

    tdb* db = tdb_init();
    if ((err = tdb_open(db, arg->tdb_path))){
        printf("Opening TrailDB failed: %s\n", tdb_error_str(err));
        exit(1);
    }

    tdb_cursor *cursor = tdb_cursor_new(db);

    for (i = arg->start_trail; i < arg->end_trail; i++){
        const uint16_t page = 1 + i / arg->trails_per_page;
        const tdb_event *event;

        if (tdb_get_trail(cursor, i))
            DIE("get_trail %"PRIu64" failed\n", i);

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

    if (!(offsets = calloc(num_items + 1, 8)))
        DIE("Could not allocate offsets for %"PRIu64" items in field %u\n",
            num_items,
            field);

    if (!(shards = malloc(num_shards * sizeof(struct shard_data))))
        DIE("Could not allocate shard data");

    for (i = 0; i < num_items; i++){
        const tdb_item item = tdb_make_item(field, i);
        uint16_t key[] = {0, 0, 0, 0};
        int key_idx = 0;
        uint64_t prev = 0;

        memset(shards, 0, num_shards * sizeof(struct shard_data));
        for (j = 0; j < num_shards; j++){
            JLG(shards[j].small_ptr, args[j].small_items, item);
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

        offsets[i] = offset;
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
    }

    offsets[i] = offset;

    if (offset > UINT32_MAX){
        const uint32_t EIGHT = 8;
        TDB_WRITE(out, &EIGHT, 4);
        TDB_WRITE(out, offsets, (num_items + 1) * 8);
    }else{
        const uint32_t FOUR = 4;
        TDB_WRITE(out, &FOUR, 4);
        for (i = 0; i < num_items + 1; i++)
            TDB_WRITE(out, &offsets[i], 4);
    }

    free(offsets);
    free(shards);
    *dedup0 = dedup;
    return offset;
done:
    DIE("IO error when writing index (disk full?)");
}

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

static FILE *init_file(const tdb *db,
                       const char *path,
                       uint64_t trails_per_page)
{
    FILE *out;
    struct header head = {.version = INDEX_VERSION,
                          .checksum = db_checksum(db),
                          .trails_per_page = trails_per_page};
    int __attribute__((unused)) ret; /* TDB_WRITE */
    if (!(out = fopen(path, "w")))
        DIE("Could not create an index at %s", path);

    TDB_WRITE(out, &head, sizeof(struct header));
    return out;
done:
    DIE("Writing index header failed. Disk full?");
}

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
        *num_pages = (offsets[idx + 1] - offsets[idx]) / 2;
        return (const uint16_t*)(index->data + offsets[idx]);
    }else if (width == 8){
        const uint64_t *offsets = data + 4;
        *num_pages = (offsets[idx + 1] - offsets[idx]) / 2;
        return (const uint16_t*)(index->data + offsets[idx]);
    }else
        DIE("Corrupted index (item %"PRIu64")", item);
}

static void intersect(char *dst, const char *src, uint64_t len)
{
    uint64_t i;
    for (i = 0; i < len; i++)
        dst[i] &= src[i];
}

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
                uint16_t n;
                const uint16_t *pages = get_index_pages(index, item, &n);
                for (k = 0; k < n; k++)
                    write_bits(disjunction, pages[k] - 1, 1);
            }
            ++j;
        }
        intersect(conjunction, disjunction, BUFFER_SIZE);
    }

    *num_candidates = 0;
    for (i = 0; i < INDEX_NUM_PAGES; i++)
        if (read_bits(conjunction, i, 1))
            *num_candidates += index->head->trails_per_page;

    if (!(candidates = malloc(*num_candidates * 8)))
        DIE("Could not allocate a buffer for %"PRIu64" match candidates",
            *num_candidates);

    for (k = 0, i = 0; i < INDEX_NUM_PAGES; i++)
        if (read_bits(conjunction, i, 1))
            for (j = 0; j < index->head->trails_per_page; j++)
                candidates[k++] = i * index->head->trails_per_page + j;

    free(conjunction);
    free(disjunction);
    return candidates;
}

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

    if (db_checksum(db) != index->head->checksum)
        DIE("TrailDB at %s and index at %s mismatch", tdb_path, index_path);

    tdb_close(db);
    return index;
}

void tdb_index_close(struct tdb_index *index)
{
    munmap((char*)index->data, index->size);
    free(index);
}

tdb_error tdb_index_create(const char *tdb_path, const char *index_path)
{
    struct job_arg *args;
    struct thread_job *jobs;
    uint64_t i, num_shards = 8;
    tdb* db = tdb_init();
    tdb_error err;

    if ((err = tdb_open(db, tdb_path))){
        DIE("Opening TrailDB at %s failed: %s\n",
            tdb_path,
            tdb_error_str(err));
    }

    const uint64_t num_trails = tdb_num_trails(db);
    const uint64_t trails_per_page = 1 + num_trails / INDEX_NUM_PAGES;
    const uint64_t pages_per_shard = 1 + (INDEX_NUM_PAGES / num_shards);
    FILE *out = init_file(db, index_path, trails_per_page);

    if (!(args = calloc(num_shards, sizeof(struct job_arg))))
        DIE("Couldn't allocate %"PRIu64" features args\n", num_shards);

    if (!(jobs = calloc(num_shards, sizeof(struct thread_job))))
        DIE("Couldn't allocate %"PRIu64" features jobs\n", num_shards);

    for (i = 0; i < num_shards; i++){
        args[i].tdb_path = tdb_path;
        args[i].trails_per_page = trails_per_page;
        args[i].start_trail = i * pages_per_shard * trails_per_page;
        args[i].end_trail = (i + 1) * pages_per_shard * trails_per_page;
        if (args[i].end_trail > num_trails)
            args[i].end_trail = num_trails;

        jobs[i].arg = &args[i];
    }

    execute_jobs(job_index_shard, jobs, num_shards, num_shards);
    write_index(out, db, args, num_shards);

    if (fclose(out))
        DIE("Closing index failed. Disk full?");

    /* FIXME free args.small_items and args.large_items */
    tdb_close(db);
    return 0;
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
