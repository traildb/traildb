#define _DEFAULT_SOURCE /* for getline() */
#define _BSD_SOURCE /* for madvise() */

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>

#include "tdb_internal.h"
#include "tdb_error.h"
#include "tdb_io.h"
#include "tdb_huffman.h"
#include "tdb_package.h"

#define DEFAULT_OPT_CURSOR_EVENT_BUFFER_SIZE 1000

struct io_ops{
    FILE* (*fopen)(const char *fname, const char *root, const tdb *db);

    int (*fclose)(FILE *f);

    int (*mmap)(const char *fname,
                const char *root,
                struct tdb_file *dst,
                const tdb *db);
};

int file_mmap(const char *fname,
              const char *root,
              struct tdb_file *dst,
              const tdb *db __attribute__((unused)))
{
    char path[TDB_MAX_PATH_SIZE];
    int fd = 0;
    int ret = 0;
    struct stat stats;

    if (root){
        TDB_PATH(path, "%s/%s", root, fname);
    }else{
        TDB_PATH(path, "%s", fname);
    }

    if ((fd = open(path, O_RDONLY)) == -1)
        return -1;

    if (fstat(fd, &stats)){
        ret = -1;
        goto done;
    }

    dst->size = dst->mmap_size = (uint64_t)stats.st_size;
    dst->data = dst->ptr = MAP_FAILED;

    if (dst->size > 0)
        dst->ptr = mmap(NULL, dst->size, PROT_READ, MAP_SHARED, fd, 0);

    if (dst->ptr == MAP_FAILED){
        ret = -1;
        goto done;
    }

    dst->data = dst->ptr;
done:
    if (fd)
        close(fd);
    return ret;
}

static FILE *file_fopen(const char *fname,
                        const char *root,
                        const tdb *db __attribute__((unused)))
{
    char path[TDB_MAX_PATH_SIZE];
    int ret = 0;
    FILE *f;

    TDB_PATH(path, "%s/%s", root, fname);
    TDB_OPEN(f, path, "r");

done:
    if (ret)
        return NULL;
    else
        return f;
}

static int file_fclose(FILE *f)
{
    return fclose(f);
}

void tdb_lexicon_read(const tdb *db, tdb_field field, struct tdb_lexicon *lex)
{
    lex->version = db->version;
    lex->data = db->lexicons[field - 1].data;
    lex->size = 0;
    if (db->lexicons[field - 1].size > UINT32_MAX){
        lex->width = 8;
        lex->toc.toc64 = (const uint64_t*)&lex->data[lex->width];
        memcpy(&lex->size, lex->data, 8);
    }else{
        lex->width = 4;
        lex->toc.toc32 = (const uint32_t*)&lex->data[lex->width];
        memcpy(&lex->size, lex->data, 4);
    }
}

static inline uint64_t tdb_lex_offset(const struct tdb_lexicon *lex, tdb_val i)
{
    if (lex->width == 4)
        return lex->toc.toc32[i];
    else
        return lex->toc.toc64[i];
}

const char *tdb_lexicon_get(const struct tdb_lexicon *lex,
                            tdb_val i,
                            uint64_t *length)
{
    if (lex->version == TDB_VERSION_V0){
        /* backwards compatibility with 0-terminated strings in v0 */
        *length = (uint64_t)strlen(&lex->data[tdb_lex_offset(lex, i)]);
    }else
        *length = tdb_lex_offset(lex, i + 1) - tdb_lex_offset(lex, i);
    return &lex->data[tdb_lex_offset(lex, i)];
}

static tdb_error fields_open(tdb *db, const char *root, struct io_ops *io)
{
    char path[TDB_MAX_PATH_SIZE];
    FILE *f = NULL;
    char *line = NULL;
    size_t n = 0;
    tdb_field i, num_ofields = 0;
    int ret = 0;
    int ok = 0;

    if (!(f = io->fopen("fields", root, db)))
        return TDB_ERR_INVALID_FIELDS_FILE;

    while (getline(&line, &n, f) != -1){
        if (line[0] == '\n'){
            /*
            V0 tdbs don't have the extra newline,
            they should read until EOF
            */
            ok = 1;
            break;
        }
        ++num_ofields;
    }
    if (!(ok || feof(f))){
        /* we can get here if malloc fails inside getline() */
        ret = TDB_ERR_NOMEM;
        goto done;
    }
    db->num_fields = num_ofields + 1U;

    if (!(db->field_names = calloc(db->num_fields, sizeof(char*)))){
        ret = TDB_ERR_NOMEM;
        goto done;
    }

    if (num_ofields){
        if (!(db->lexicons = calloc(num_ofields, sizeof(struct tdb_file)))){
            ret = TDB_ERR_NOMEM;
            goto done;
        }
    }else
        db->lexicons = NULL;

    /* io_ops doesn't support rewind(), so we have to close and reopen */
    io->fclose(f);
    if (!(f = io->fopen("fields", root, db))){
        ret = TDB_ERR_IO_OPEN;
        goto done;
    }

    db->field_names[0] = "time";

    for (i = 1; getline(&line, &n, f) != -1 && i < db->num_fields; i++){

        line[strlen(line) - 1] = 0;

        /* let's be paranoid and sanity check the fieldname again */
        if (is_fieldname_invalid(line)){
            ret = TDB_ERR_INVALID_FIELDS_FILE;
            goto done;
        }

        if (!(db->field_names[i] = strdup(line))){
            ret = TDB_ERR_NOMEM;
            goto done;
        }

        TDB_PATH(path, "lexicon.%s", line);
        if (io->mmap(path, root, &db->lexicons[i - 1], db)){
            ret = TDB_ERR_INVALID_LEXICON_FILE;
            goto done;
        }
    }

    if (i != db->num_fields){
        ret = TDB_ERR_INVALID_FIELDS_FILE;
        goto done;
    }

done:
    free(line);
    if (f)
        io->fclose(f);
    return ret;
}

static tdb_error init_field_stats(tdb *db)
{
    uint64_t *field_cardinalities = NULL;
    tdb_field i;
    int ret = 0;

    if (db->num_fields > 1){
        if (!(field_cardinalities = calloc(db->num_fields - 1, 8)))
            return TDB_ERR_NOMEM;
    }

    for (i = 1; i < db->num_fields; i++){
        struct tdb_lexicon lex;
        tdb_lexicon_read(db, i, &lex);
        field_cardinalities[i - 1] = lex.size;
    }

    if (!(db->field_stats = huff_field_stats(field_cardinalities,
                                             db->num_fields,
                                             db->max_timestamp_delta)))
        ret = TDB_ERR_NOMEM;

    free(field_cardinalities);
    return ret;
}

static tdb_error read_version(tdb *db, const char *root, struct io_ops *io)
{
    FILE *f;
    int ret = 0;

    if (!(f = io->fopen("version", root, db)))
        db->version = 0;
    else{
        if (fscanf(f, "%"PRIu64, &db->version) != 1)
            ret = TDB_ERR_INVALID_VERSION_FILE;
        else if (db->version > TDB_VERSION_LATEST)
            ret = TDB_ERR_INCOMPATIBLE_VERSION;
        io->fclose(f);
    }
    return ret;
}

static tdb_error read_info(tdb *db, const char *root, struct io_ops *io)
{
    FILE *f;
    int ret = 0;

    if (!(f = io->fopen("info", root, db)))
        return TDB_ERR_INVALID_INFO_FILE;

    if (fscanf(f,
               "%"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64,
               &db->num_trails,
               &db->num_events,
               &db->min_timestamp,
               &db->max_timestamp,
               &db->max_timestamp_delta) != 5)
        ret = TDB_ERR_INVALID_INFO_FILE;

    io->fclose(f);
    return ret;
}

tdb *tdb_init(void)
{
    return calloc(1, sizeof(tdb));
}

tdb_error tdb_open(tdb *db, const char *orig_root)
{
    char root[TDB_MAX_PATH_SIZE];
    struct stat stats;
    tdb_error ret = 0;
    struct io_ops io;

    /*
    by handling the "db == NULL" case here gracefully, we allow the return
    value of tdb_init() to be used unchecked like here:

    int err;
    tdb *db = tdb_init();
    if ((err = tdb_open(db, path)))
        printf("Opening tbd failed: %s", tdb_error(err));
    */
    if (!db)
        return TDB_ERR_HANDLE_IS_NULL;

    if (db->num_fields)
        return TDB_ERR_HANDLE_ALREADY_OPENED;

    /* set default options */
    db->opt_cursor_event_buffer_size = DEFAULT_OPT_CURSOR_EVENT_BUFFER_SIZE;

    TDB_PATH(root, "%s", orig_root);
    if (stat(root, &stats) == -1){
        TDB_PATH(root, "%s.tdb", orig_root);
        if (stat(root, &stats) == -1){
            ret = TDB_ERR_IO_OPEN;
            goto done;
        }
    }
    if (S_ISDIR(stats.st_mode)){
        /* open tdb in a directory */
        io.fopen = file_fopen;
        io.fclose = file_fclose;
        io.mmap = file_mmap;
    }else{
        /* open tdb in a tarball */
        io.fopen = package_fopen;
        io.fclose = package_fclose;
        io.mmap = package_mmap;
        if ((ret = open_package(db, root)))
            goto done;
    }

    if ((ret = read_info(db, root, &io)))
        goto done;

    if ((ret = read_version(db, root, &io)))
        goto done;

    if ((ret = fields_open(db, root, &io)))
        goto done;

    if ((ret = init_field_stats(db)))
        goto done;

    if (db->num_trails) {
        /* backwards compatibility: UUIDs used to be called cookies */
        if (db->version == TDB_VERSION_V0){
            if (io.mmap("cookies", root, &db->uuids, db)){
                ret = TDB_ERR_INVALID_UUIDS_FILE;
                goto done;
            }
        }else{
            if (io.mmap("uuids", root, &db->uuids, db)){
                ret = TDB_ERR_INVALID_UUIDS_FILE;
                goto done;
            }
        }

        if (io.mmap("trails.codebook", root, &db->codebook, db)){
            ret = TDB_ERR_INVALID_CODEBOOK_FILE;
            goto done;
        }

        if (db->version == TDB_VERSION_V0)
            if ((ret = huff_convert_v0_codebook(&db->codebook)))
                goto done;

        if (io.mmap("trails.toc", root, &db->toc, db)){
            ret = TDB_ERR_INVALID_TRAILS_FILE;
            goto done;
        }

        if (io.mmap("trails.data", root, &db->trails, db)){
            ret = TDB_ERR_INVALID_TRAILS_FILE;
            goto done;
        }
    }
done:
    free_package(db);
    return ret;
}

void tdb_willneed(const tdb *db)
{
    if (db && db->num_fields > 0){
        tdb_field i;
        for (i = 0; i < db->num_fields - 1; i++)
            madvise(db->lexicons[i].ptr,
                    db->lexicons[i].mmap_size,
                    MADV_WILLNEED);

        madvise(db->uuids.ptr, db->uuids.mmap_size, MADV_WILLNEED);
        madvise(db->codebook.ptr, db->codebook.mmap_size, MADV_WILLNEED);
        madvise(db->toc.ptr, db->toc.mmap_size, MADV_WILLNEED);
        madvise(db->trails.ptr, db->trails.mmap_size, MADV_WILLNEED);
    }
}

void tdb_dontneed(const tdb *db)
{
    if (db && db->num_fields > 0){
        tdb_field i;
        for (i = 0; i < db->num_fields - 1; i++)
            madvise(db->lexicons[i].ptr,
                    db->lexicons[i].mmap_size,
                    MADV_DONTNEED);

        madvise(db->uuids.ptr, db->uuids.mmap_size, MADV_DONTNEED);
        madvise(db->codebook.ptr, db->codebook.mmap_size, MADV_DONTNEED);
        madvise(db->toc.ptr, db->toc.mmap_size, MADV_DONTNEED);
        madvise(db->trails.ptr, db->trails.mmap_size, MADV_DONTNEED);
    }
}

void tdb_close(tdb *db)
{
    if (db){
        tdb_field i;

        if (db->num_fields > 0){
            for (i = 0; i < db->num_fields - 1; i++){
                free(db->field_names[i + 1]);
                if (db->lexicons[i].ptr)
                    munmap(db->lexicons[i].ptr, db->lexicons[i].mmap_size);
            }
        }

        if (db->uuids.ptr)
            munmap(db->uuids.ptr, db->uuids.mmap_size);
        if (db->codebook.ptr)
            munmap(db->codebook.ptr, db->codebook.mmap_size);
        if (db->toc.ptr)
            munmap(db->toc.ptr, db->toc.mmap_size);
        if (db->trails.ptr)
            munmap(db->trails.ptr, db->trails.mmap_size);

        free(db->lexicons);
        free(db->field_names);
        free(db->field_stats);
        free(db->opt_event_filter);
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

tdb_error tdb_get_field(const tdb *db,
                        const char *field_name,
                        tdb_field *field)
{
    tdb_field i;
    for (i = 0; i < db->num_fields; i++)
        if (!strcmp(field_name, db->field_names[i])){
            *field = i;
            return 0;
        }
    return TDB_ERR_UNKNOWN_FIELD;
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
        return tdb_make_item(field, 0);
    else if (field == 0 || field >= db->num_fields)
        return 0;
    else{
        struct tdb_lexicon lex;
        tdb_val i;
        tdb_lexicon_read(db, field, &lex);

        for (i = 0; i < lex.size; i++){
            uint64_t length;
            const char *token = tdb_lexicon_get(&lex, i, &length);
            if (length == value_length && !memcmp(token, value, length))
                return tdb_make_item(field, i + 1);
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

tdb_error tdb_get_trail_id(const tdb *db,
                           const uint8_t *uuid,
                           uint64_t *trail_id)
{
    __uint128_t cmp, key;
    memcpy(&key, uuid, 16);

    if (db->version == TDB_VERSION_V0){
        /* V0 doesn't guarantee that UUIDs would be ordered */
        uint64_t idx;
        for (idx = 0; idx < db->num_trails; idx++){
            memcpy(&cmp, &db->uuids.data[idx * 16], 16);
            if (key == cmp){
                *trail_id = idx;
                return 0;
            }
        }
    }else{
        /* note: TDB_MAX_NUM_TRAILS < 2^63, so we can safely use int64_t */
        int64_t idx;
        int64_t left = 0;
        int64_t right = ((int64_t)db->num_trails) - 1LL;

        while (left <= right){
            /* compute midpoint in an overflow-safe manner (see Wikipedia) */
            idx = left + ((right - left) / 2);
            memcpy(&cmp, &db->uuids.data[idx * 16], 16);
            if (cmp == key){
                *trail_id = (uint64_t)idx;
                return 0;
            }else if (cmp > key)
                right = idx - 1;
            else
                left = idx + 1;
        }
    }
    return TDB_ERR_UNKNOWN_UUID;
}

const char *tdb_error_str(tdb_error errcode)
{
    /* TODO implement this */
    switch (errcode){
        default:
            return "Unknown error";
    }
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

tdb_error tdb_set_opt(tdb *db, tdb_opt_key key, tdb_opt_value value)
{
    switch (key){
        case TDB_OPT_ONLY_DIFF_ITEMS:
            db->opt_edge_encoded = value.value ? 1: 0;
            return 0;
        case TDB_OPT_CURSOR_EVENT_BUFFER_SIZE:
            if (value.value > 0)
                db->opt_cursor_event_buffer_size = value.value;
            else
                return TDB_ERR_INVALID_OPTION_VALUE;
        default:
            return TDB_ERR_UNKNOWN_OPTION;
    }
}

tdb_error tdb_get_opt(tdb *db, tdb_opt_key key, tdb_opt_value *value)
{
    switch (key){
        case TDB_OPT_ONLY_DIFF_ITEMS:
            *value = db->opt_edge_encoded ? TDB_TRUE: TDB_FALSE;
            return 0;
        case TDB_OPT_CURSOR_EVENT_BUFFER_SIZE:
            value->value = db->opt_cursor_event_buffer_size;
            return 0;
        default:
            return TDB_ERR_UNKNOWN_OPTION;
    }
}

#if 0
tdb_error tdb_set_filter(tdb *db, const tdb_item *filter, uint64_t filter_len)
{
    /* TODO check that filter is valid: sum(clauses) < filter_len */
    free(db->filter);
    if (filter){
        if (!(db->filter = malloc(filter_len * sizeof(tdb_item))))
            return TDB_ERR_NOMEM;
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
#endif
