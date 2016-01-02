#define _GNU_SOURCE /* for getline() */

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#ifdef ENABLE_UUID_INDEX
#include <cmph.h>
#endif

#include "tdb_internal.h"
#include "tdb_huffman.h"
#include "util.h"

/* TODO implement tdb_init() and fix error codes */
/* TODO remove DIEs */

void tdb_err(tdb *db, char *fmt, ...)
{
    if (db){
        va_list aptr;
        va_start(aptr, fmt);
        vsnprintf(db->error, TDB_MAX_ERROR_SIZE, fmt, aptr);
        va_end(aptr);
    }
}

void tdb_path(char path[TDB_MAX_PATH_SIZE], char *fmt, ...)
{
    va_list aptr;

    va_start(aptr, fmt);
    if (vsnprintf(path, TDB_MAX_PATH_SIZE, fmt, aptr) >= TDB_MAX_PATH_SIZE)
        DIE("Path too long (fmt %s)", fmt);
    va_end(aptr);
}

int tdb_mmap(const char *path, struct tdb_file *dst, tdb *db)
{
    int fd;
    struct stat stats;

    if ((fd = open(path, O_RDONLY)) == -1){
        tdb_err(db, "Could not open path: %s", path);
        return -1;
    }

    if (fstat(fd, &stats)){
        tdb_err(db, "Could not stat path: %s", path);
        close(fd);
        return -1;
    }

    if ((dst->size = (uint64_t)stats.st_size))
        dst->data = mmap(NULL, dst->size, PROT_READ, MAP_SHARED, fd, 0);
    else {
        tdb_err(db, "Could not mmap path: %s", path);
        close(fd);
        return -1;
    }

    if (dst->data == MAP_FAILED){
        tdb_err(db, "Could not mmap path: %s", path);
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

void tdb_lexicon_read(const tdb *db, tdb_field field, struct tdb_lexicon *lex)
{
    lex->version = db->version;
    lex->data = db->lexicons[field - 1].data;
    memcpy(&lex->size, lex->data, 4);
    lex->toc = (const uint32_t*)&lex->data[4];
}

const char *tdb_lexicon_get(const struct tdb_lexicon *lex,
                            tdb_val i,
                            uint64_t *length)
{
    if (lex->version == TDB_VERSION_V0){
        /* backwards compatibility with 0-terminated strings in v0 */
        *length = (uint64_t)strlen(&lex->data[lex->toc[i]]);
    }else
        *length = lex->toc[i + 1] - lex->toc[i];
    return &lex->data[lex->toc[i]];
}

static int tdb_fields_open(tdb *db, const char *root, char *path)
{
    FILE *f;
    char *line = NULL;
    size_t n = 0;
    tdb_field i, num_ofields = 0;

    tdb_path(path, "%s/fields", root);
    if (!(f = fopen(path, "r"))){
        tdb_err(db, "Could not open path: %s", path);
        return -1;
    }

    while (getline(&line, &n, f) != -1)
        ++num_ofields;
    db->num_fields = num_ofields + 1U;

    if (!feof(f)){
        /* we can get here if malloc fails inside getline() */
        tdb_err(db, "getline failed when opening fields");
        goto error;
    }

    if (!(db->field_names = calloc(db->num_fields, sizeof(char*)))){
        tdb_err(db, "Could not alloc %u field names", db->num_fields);
        goto error;
    }

    if (num_ofields){
        if (!(db->lexicons = calloc(num_ofields, sizeof(struct tdb_file)))){
            tdb_err(db, "Could not alloc %u files", num_ofields);
            goto error;
        }
    }else
        db->lexicons = NULL;

    if (!(db->previous_items = calloc(db->num_fields, sizeof(tdb_item)))){
        tdb_err(db, "Could not alloc %u values", db->num_fields);
        goto error;
    }

    rewind(f);

    db->field_names[0] = "time";

    for (i = 1; getline(&line, &n, f) != -1 && i < db->num_fields; i++){

        line[strlen(line) - 1] = 0;

        if (!(db->field_names[i] = strdup(line))){
            tdb_err(db, "Could not allocate field name %d", i);
            goto error;
        }

        tdb_path(path, "%s/lexicon.%s", root, line);
        /* TODO convert old-style lexicons to the new format on the fly */
        if (tdb_mmap(path, &db->lexicons[i - 1], db)){
            goto error;
        }
    }

    if (i != db->num_fields) {
        tdb_err(db, "Error reading fields file");
        goto error;
    }

    free(line);
    fclose(f);
    return 0;

error:
    free(line);
    fclose(f);
    return -1;
}

static int init_field_stats(tdb *db)
{
    tdb_field i;
    uint64_t *field_cardinalities;

    if (db->num_fields > 1) {
        if (!(field_cardinalities = calloc(db->num_fields - 1, 8)))
            return -1;
    } else {
        field_cardinalities = NULL;
    }

    for (i = 1; i < db->num_fields; i++){
        struct tdb_lexicon lex;
        tdb_lexicon_read(db, i, &lex);
        field_cardinalities[i - 1] = lex.size;
    }

    if (!(db->field_stats = huff_field_stats(field_cardinalities,
                                             db->num_fields,
                                             db->max_timestamp_delta))){
        free(field_cardinalities);
        return -1;
    }

    free(field_cardinalities);
    return 0;
}

static int read_version(tdb *db, const char *path)
{
    FILE *f;

    if (!(f = fopen(path, "r"))){
        tdb_err(db, "Could not open path: %s", path);
        return -1;
    }

    if (fscanf(f, "%"PRIu64, &db->version) != 1){
        tdb_err(db, "Invalid version file");
        return -1;
    }
    /* TODO check that db->version <= TDB_VERSION_LATEST */

    fclose(f);

    return 0;
}

static int read_info(tdb *db, const char *path)
{
    FILE *f;

    if (!(f = fopen(path, "r"))){
        tdb_err(db, "Could not open path: %s", path);
        return -1;
    }

    if (fscanf(f,
               "%"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64,
               &db->num_trails,
               &db->num_events,
               &db->min_timestamp,
               &db->max_timestamp,
               &db->max_timestamp_delta) != 5){
        tdb_err(db, "Invalid info file");
        return -1;
    }
    fclose(f);

    return 0;
}

tdb *tdb_open(const char *root)
{
    char path[TDB_MAX_PATH_SIZE];
    tdb *db;

    /* TODO create a version file and a version check that prevents older versions of
       traildb from reading newer files */

    if (!(db = calloc(1, sizeof(tdb))))
        return NULL;

    tdb_path(path, "%s/info", root);
    if (read_info(db, path))
        goto err;

    tdb_path(path, "%s/version", root);
    if (access(path, F_OK))
        db->version = TDB_VERSION_V0;
    else{
        if (read_version(db, path))
            goto err;
    }

    if (db->num_trails) {
        /* backwards compatibility: UUIDs used to be called cookies */
        tdb_path(path, "%s/cookies", root);
        if (access(path, F_OK))
            tdb_path(path, "%s/uuids", root);
        if (tdb_mmap(path, &db->uuids, db))
            goto err;

        /* TODO deprecate .index */
        tdb_path(path, "%s/cookies.index", root);
        if (access(path, F_OK))
            tdb_path(path, "%s/uuids.index", root);
        if (tdb_mmap(path, &db->uuid_index, db))
            db->uuid_index.data = NULL;

        tdb_path(path, "%s/trails.codebook", root);
        if (tdb_mmap(path, &db->codebook, db))
            goto err;

        tdb_path(path, "%s/trails.data", root);
        if (tdb_mmap(path, &db->trails, db))
            goto err;

        tdb_path(path, "%s/trails.toc", root);
        if (access(path, F_OK)) // backwards compat
            tdb_path(path, "%s/trails.data", root);
        if (tdb_mmap(path, &db->toc, db))
            goto err;
    }

    if (tdb_fields_open(db, root, path))
        goto err;

    if (init_field_stats(db)){
        tdb_err(db, "Could not init field stats");
        goto err;
    }
    return db;
err:
    db->error_code = 1;
    return NULL;
}

void tdb_willneed(const tdb *db)
{
    if (db){
        tdb_field i;
        for (i = 0; i < db->num_fields - 1; i++)
            madvise(db->lexicons[i].data,
                    db->lexicons[i].size,
                    MADV_WILLNEED);

        madvise(db->uuids.data, db->uuids.size, MADV_WILLNEED);
        madvise(db->codebook.data, db->codebook.size, MADV_WILLNEED);
        madvise(db->trails.data, db->trails.size, MADV_WILLNEED);
        madvise(db->toc.data, db->toc.size, MADV_WILLNEED);
    }
}

void tdb_dontneed(const tdb *db)
{
    if (db){
        tdb_field i;
        for (i = 0; i < db->num_fields - 1; i++)
            madvise(db->lexicons[i].data,
                    db->lexicons[i].size,
                    MADV_DONTNEED);

        madvise(db->uuids.data, db->uuids.size, MADV_DONTNEED);
        madvise(db->codebook.data, db->codebook.size, MADV_DONTNEED);
        madvise(db->trails.data, db->trails.size, MADV_DONTNEED);
        madvise(db->toc.data, db->toc.size, MADV_DONTNEED);
    }
}

void tdb_close(tdb *db)
{
    if (db){
        tdb_field i;
        for (i = 0; i < db->num_fields - 1; i++){
            free(db->field_names[i + 1]);
            munmap(db->lexicons[i].data, db->lexicons[i].size);
        }

        munmap(db->uuids.data, db->uuids.size);
        munmap(db->codebook.data, db->codebook.size);
        munmap(db->trails.data, db->trails.size);
        munmap(db->toc.data, db->toc.size);

        free(db->lexicons);
        free(db->previous_items);
        free(db->field_names);
        free(db->field_stats);
        free(db->filter);
        free(db);
    }
}

uint64_t tdb_lexicon_size(const tdb *db, tdb_field field)
{
    if (field == 0 || field >= db->num_fields)
        return 0;
    else{
        struct tdb_lexicon lex;
        tdb_lexicon_read(db, field, &lex);
        /* +1 refers to the implicit NULL value (empty string) */
        return lex.size + 1;
    }
}

int tdb_get_field(const tdb *db, const char *field_name, tdb_field *field)
{
    tdb_field i;
    for (i = 0; i < db->num_fields; i++)
        if (!strcmp(field_name, db->field_names[i])){
            *field = i;
            return 0;
        }
    return -1;
}

const char *tdb_get_field_name(const tdb *db, tdb_field field)
{
    if (field < db->num_fields)
        return db->field_names[field];
    return NULL;
}

tdb_item tdb_get_item(const tdb *db,
                      tdb_field field,
                      const char *value,
                      uint64_t value_length)
{
    if (!value_length)
        /* NULL value for this field */
        return field;
    else if (field == 0 || field >= db->num_fields)
        return 0;
    else{
        struct tdb_lexicon lex;
        tdb_val i;
        tdb_lexicon_read(db, field, &lex);

        for (i = 0; i < lex.size; i++){
            uint64_t length;
            const char *token = tdb_lexicon_get(&lex, i, &length);
            if (length == value_length && !memcmp(&token, value, length))
                return field | ((i + 1) << 8);
        }
        return 0;
    }
}

const char *tdb_get_value(const tdb *db,
                          tdb_field field,
                          tdb_val val,
                          uint64_t *value_length)
{
    if (field == 0 || field >= db->num_fields)
        return NULL;
    else if (!val){
        /* a valid NULL value for a valid field */
        *value_length = 0;
        return "";
    }else{
        struct tdb_lexicon lex;
        tdb_lexicon_read(db, field, &lex);
        if ((val - 1) < lex.size)
            return tdb_lexicon_get(&lex, val - 1, value_length);
        else
            return NULL;
    }
}

const char *tdb_get_item_value(const tdb *db,
                               tdb_item item,
                               uint64_t *value_length)
{
    return tdb_get_value(db,
                         tdb_item_field(item),
                         tdb_item_val(item),
                         value_length);
}

const uint8_t *tdb_get_uuid(const tdb *db, uint64_t trail_id)
{
    if (trail_id < db->num_trails)
        return (const uint8_t *)&db->uuids.data[trail_id * 16];
    return NULL;
}

int64_t tdb_get_trail_id(const tdb *db, const uint8_t *uuid)
{
    /* TODO implement binary search */
    /* TODO remove UUID index */
    uint64_t i;
#ifdef ENABLE_UUID_INDEX
    /* (void*) cast is horrible below. I don't know why cmph_search_packed
       can't have a const modifier. This will segfault loudly if cmph tries to
       modify the read-only mmap'ed uuid_index. */
    if (db->uuid_index.data){
        i = cmph_search_packed((void*)db->uuid_index.data,
                               (const char*)uuid,
                               16);

        if (i < db->num_trails){
            if (!memcmp(tdb_get_uuid(db, i), uuid, 16))
                return (int64_t)i;
        }
        return -1;
    }
#endif
    for (i = 0; i < db->num_trails; i++)
        if (!memcmp(tdb_get_uuid(db, i), uuid, 16))
            return (int64_t)i;
    return -1;
}

/* TODO obsolete with binary search */
int tdb_has_uuid_index(const tdb *db __attribute__((unused)))
{
#ifdef ENABLE_UUID_INDEX
    return db->uuid_index.data ? 1 : 0;
#else
    return 0;
#endif
}

const char *tdb_error(const tdb *db)
{
    return db->error;
}

uint64_t tdb_num_trails(const tdb *db)
{
    return db->num_trails;
}

uint64_t tdb_num_events(const tdb *db)
{
    return db->num_events;
}

uint64_t tdb_num_fields(const tdb *db)
{
    return db->num_fields;
}

uint64_t tdb_min_timestamp(const tdb *db)
{
    return db->min_timestamp;
}

uint64_t tdb_max_timestamp(const tdb *db)
{
    return db->max_timestamp;
}

uint64_t tdb_version(const tdb *db)
{
    return db->version;
}

int tdb_set_filter(tdb *db, const tdb_item *filter, uint64_t filter_len)
{
    /* TODO check that filter is valid: sum(clauses) < filter_len */
    free(db->filter);
    if (filter){
        if (!(db->filter = malloc(filter_len * sizeof(tdb_item)))){
            tdb_err(db, "Could not alloc new filter");
            return -1;
        }
        memcpy(db->filter, filter, filter_len * sizeof(tdb_item));
        db->filter_len = filter_len;
    }else{
        db->filter = NULL;
        db->filter_len = 0;
    }
    return 0;
}

const tdb_item *tdb_get_filter(const tdb *db, uint64_t *filter_len)
{
    *filter_len = db->filter_len;
    return db->filter;
}

