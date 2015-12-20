
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <unistd.h>

#ifdef ENABLE_COOKIE_INDEX
#include <cmph.h>
#endif

#include "tdb_internal.h"
#include "huffman.h"
#include "util.h"

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

int tdb_mmap(const char *path, tdb_file *dst, tdb *db)
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

    if ((dst->size = stats.st_size))
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

static int tdb_fields_open(tdb *db, const char *root, char *path)
{
    FILE *f;
    char *line = NULL;
    size_t n = 0;
    int i = 0;
    tdb_field num_ofields = 0;

    tdb_path(path, "%s/fields", root);
    if (!(f = fopen(path, "r"))){
        tdb_err(db, "Could not open path: %s", path);
        return -1;
    }

    while (getline(&line, &n, f) != -1)
        ++num_ofields;
    db->num_fields = num_ofields + 1;

    if (!feof(f)){
        /* we can get here if malloc fails inside getline() */
        tdb_err(db, "getline failed when opening fields");
        goto error;
    }

    if (!(db->field_names = calloc(db->num_fields, sizeof(char*)))){
        tdb_err(db, "Could not alloc %u field names", db->num_fields);
        goto error;
    }

    if (num_ofields) {
        if (!(db->lexicons = calloc(num_ofields, sizeof(tdb_file)))){
            tdb_err(db, "Could not alloc %u files", num_ofields);
            goto error;
        }
    } else {
        db->lexicons = NULL;
    }

    if (!(db->previous_items = calloc(db->num_fields, 4))){
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
        const tdb_lexicon *lex;

        if (tdb_lexicon_read(db, i, &lex)){
            free(field_cardinalities);
            return -1;
        }

        field_cardinalities[i - 1] = lex->size;
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

static int read_info(tdb *db, const char *path)
{
    FILE *f;

    if (!(f = fopen(path, "r"))){
        tdb_err(db, "Could not open path: %s", path);
        return -1;
    }

    if (fscanf(f,
               "%"PRIu64" %"PRIu64" %u %u %u",
               &db->num_cookies,
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

    if (!(db = calloc(1, sizeof(tdb))))
        return NULL;

    tdb_path(path, "%s/info", root);
    if (read_info(db, path))
        goto err;

    if (db->num_cookies) {
        tdb_path(path, "%s/cookies", root);
        if (tdb_mmap(path, &db->cookies, db))
            goto err;

        tdb_path(path, "%s/cookies.index", root);
        if (tdb_mmap(path, &db->cookie_index, db))
            db->cookie_index.data = NULL;

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

void tdb_willneed(tdb *db)
{
    if (db){
        tdb_field i;
        for (i = 0; i < db->num_fields - 1; i++)
            madvise((void*)db->lexicons[i].data,
                    db->lexicons[i].size,
                    MADV_WILLNEED);

        madvise((void*)db->cookies.data, db->cookies.size, MADV_WILLNEED);
        madvise((void*)db->codebook.data, db->codebook.size, MADV_WILLNEED);
        madvise((void*)db->trails.data, db->trails.size, MADV_WILLNEED);
        madvise((void*)db->toc.data, db->toc.size, MADV_WILLNEED);
    }
}

void tdb_dontneed(tdb *db)
{
    if (db){
        tdb_field i;
        for (i = 0; i < db->num_fields - 1; i++)
            madvise((void*)db->lexicons[i].data,
                    db->lexicons[i].size,
                    MADV_DONTNEED);

        madvise((void*)db->cookies.data, db->cookies.size, MADV_DONTNEED);
        madvise((void*)db->codebook.data, db->codebook.size, MADV_DONTNEED);
        madvise((void*)db->trails.data, db->trails.size, MADV_DONTNEED);
        madvise((void*)db->toc.data, db->toc.size, MADV_DONTNEED);
    }
}

void tdb_close(tdb *db)
{
    if (db){
        tdb_field i;
        for (i = 0; i < db->num_fields - 1; i++){
            free((char*)db->field_names[i + 1]);
            munmap((void*)db->lexicons[i].data, db->lexicons[i].size);
        }

        munmap((void*)db->cookies.data, db->cookies.size);
        munmap((void*)db->codebook.data, db->codebook.size);
        munmap((void*)db->trails.data, db->trails.size);
        munmap((void*)db->toc.data, db->toc.size);

        free(db->lexicons);
        free(db->previous_items);
        free(db->field_names);
        free(db->field_stats);
        free(db->filter);
        free(db);
    }
}

int tdb_lexicon_read(tdb *db, tdb_field field, const tdb_lexicon **lex)
{
    if (field == 0){
        tdb_err(db, "No lexicon for timestamp");
        return -1;
    }
    if (field >= db->num_fields){
        tdb_err(db, "Invalid field: %"PRIu8, field);
        return -1;
    }
    *lex = (tdb_lexicon*)db->lexicons[field - 1].data;
    return 0;
}

uint32_t tdb_lexicon_size(tdb *db, tdb_field field)
{
    const tdb_lexicon *lex;
    if (tdb_lexicon_read(db, field, &lex))
        return 0;
    return lex->size + 1;
}

int tdb_get_field(tdb *db, const char *field_name)
{
    tdb_field i;
    for (i = 0; i < db->num_fields; i++)
        if (!strcmp(field_name, db->field_names[i]))
            return i;
    tdb_err(db, "Field not found: %s", field_name);
    return -1;
}

const char *tdb_get_field_name(tdb *db, tdb_field field)
{
    if (field < db->num_fields)
        return db->field_names[field];
    return NULL;
}

int tdb_field_has_overflow_vals(tdb *db, tdb_field field)
{
    return tdb_lexicon_size(db, field) > TDB_MAX_NUM_VALUES + 1;
}

tdb_item tdb_get_item(tdb *db, tdb_field field, const char *value)
{
    const tdb_lexicon *lex;
    if (!strlen(value))
        return field;
    if (tdb_lexicon_read(db, field, &lex))
        return 0;
    if (*value){
        tdb_val i;
        for (i = 0; i < lex->size; i++)
            if (!strcmp((char*)lex + (&lex->toc)[i], value))
                return field | ((i + 1) << 8);
    }else{
        return field; /* valid empty value */
    }
    return 0;
}

const char *tdb_get_value(tdb *db, tdb_field field, tdb_val val)
{
    const tdb_lexicon *lex;
    if (!val && field && field < db->num_fields)
        return "";
    if (tdb_lexicon_read(db, field, &lex))
        return NULL;
    if ((val - 1) < lex->size)
        return (char*)lex + (&lex->toc)[val - 1];
    tdb_err(db, "Field %"PRIu8" has no val %"PRIu32, field, val);
    return NULL;
}

const char *tdb_get_item_value(tdb *db, tdb_item item)
{
    return tdb_get_value(db, tdb_item_field(item), tdb_item_val(item));
}

const uint8_t *tdb_get_cookie(const tdb *db, uint64_t cookie_id)
{
    if (cookie_id < db->num_cookies)
        return (const uint8_t *)&db->cookies.data[cookie_id * 16];
    return NULL;
}

inline uint64_t tdb_get_cookie_offs(const tdb *db, uint64_t cookie_id)
{
    if (db->trails.size < UINT32_MAX)
        return ((uint32_t*)db->toc.data)[cookie_id];
    return ((uint64_t*)db->toc.data)[cookie_id];
}

uint64_t tdb_get_cookie_id(const tdb *db, const uint8_t *cookie)
{
    uint64_t i;
#ifdef ENABLE_COOKIE_INDEX
    /* (void*) cast is horrible below. I don't know why cmph_search_packed
       can't have a const modifier. This will segfault loudly if cmph tries to
       modify the read-only mmap'ed cookie_index. */
    if (db->cookie_index.data){
        i = cmph_search_packed((void*)db->cookie_index.data,
                               (const char*)cookie,
                               16);

        if (i < db->num_cookies){
            if (!memcmp(tdb_get_cookie(db, i), cookie, 16))
                return i;
        }
        return -1;
    }
#endif
    for (i = 0; i < db->num_cookies; i++)
        if (!memcmp(tdb_get_cookie(db, i), cookie, 16))
            return i;
    return -1;
}

int tdb_has_cookie_index(const tdb *db)
{
#ifdef ENABLE_COOKIE_INDEX
    return db->cookie_index.data ? 1 : 0;
#else
    return 0;
#endif
}

const char *tdb_error(const tdb *db)
{
    return db->error;
}

uint64_t tdb_num_cookies(const tdb *db)
{
    return db->num_cookies;
}

uint64_t tdb_num_events(const tdb *db)
{
    return db->num_events;
}

uint32_t tdb_num_fields(const tdb *db)
{
    return db->num_fields;
}

uint32_t tdb_min_timestamp(const tdb *db)
{
    return db->min_timestamp;
}

uint32_t tdb_max_timestamp(const tdb *db)
{
    return db->max_timestamp;
}

int tdb_set_filter(tdb *db, const uint32_t *filter, uint32_t filter_len)
{
    free(db->filter);
    if (filter){
        if (!(db->filter = malloc(filter_len * 4))){
            tdb_err(db, "Could not alloc new filter");
            return -1;
        }
        memcpy(db->filter, filter, filter_len * 4);
        db->filter_len = filter_len;
    }else{
        db->filter = NULL;
        db->filter_len = 0;
    }
    return 0;
}

const uint32_t *tdb_get_filter(const tdb *db, uint32_t *filter_len)
{
    *filter_len = db->filter_len;
    return db->filter;
}

static void *split_fn(const tdb *db,
                      uint64_t cookie_id,
                      const tdb_item *items,
                      void *acc)
{
    struct _tdb_split *s = (struct _tdb_split*)acc;
    const uint8_t *cookie = tdb_get_cookie(db, cookie_id);
    int p = tdb_djb2(cookie) % s->num_parts;

    char *values = s->values, *value;
    tdb_lexicon *lex;
    tdb_field i;
    tdb_val val;
    for (i = 0; i < db->num_fields - 1; i++){
        lex = (tdb_lexicon*)db->lexicons[i].data;
        val = tdb_item_val(items[i + 1]);
        value = val ? (char*)lex + (&lex->toc)[val - 1] : "";
        values = stpcpy(values, value) + 1;
    }

    if (tdb_cons_add(s->cons[p], cookie, items[0], s->values))
        return NULL;
    return acc;
}

int tdb_split(const tdb *db,
              unsigned int num_parts,
              const char *fmt,
              uint64_t flags)
{
    return tdb_split_with(db, num_parts, fmt, flags, split_fn);
}


int tdb_split_with(const tdb *db,
                   unsigned int num_parts,
                   const char *fmt,
                   uint64_t flags,
                   tdb_fold_fn split_fn)
{
    if (num_parts < 1)
        return 0;

    tdb_field i, num_ofields = db->num_fields - 1;
    size_t size = 0;
    for (i = 1; i < db->num_fields; i++)
        size += strlen(db->field_names[i]) + 1;
    char *ofield_names = calloc(size, sizeof(char)), *field_name = ofield_names;
    if (ofield_names == NULL)
        return -1;
    for (i = 1; i < db->num_fields; i++)
        field_name = stpcpy(field_name, db->field_names[i]) + 1;

    int ret = 0;
    struct _tdb_split s = {
        .cons = calloc(num_parts, sizeof(tdb_cons*)),
        .split_fn = split_fn,
        .num_parts = num_parts,
        .values = calloc(num_ofields * (TDB_MAX_VALUE_SIZE + 1), sizeof(char))
    };
    if (s.cons == NULL || s.values == NULL){
        ret = -1;
        goto done;
    }

    unsigned int p;
    char path[TDB_MAX_PATH_SIZE];
    for (p = 0; p < num_parts; p++) {
        if (snprintf(path, TDB_MAX_PATH_SIZE, fmt, p) < 0){
            ret = -1;
            goto done;
        }

        if ((s.cons[p] = tdb_cons_new(path, ofield_names, num_ofields)) == NULL){
            ret = -1;
            goto done;
        }
    }

    if ((tdb_fold(db, split_fn, &s) == NULL)){
        ret = -1;
        goto done;
    }

    for (i = 0; i < num_parts; i++)
        if ((ret = tdb_cons_finalize(s.cons[i], flags)))
            goto done;

 done:
    for (i = 0; i < num_parts; i++)
        if (s.cons[i])
            tdb_cons_free(s.cons[i]);
    free(s.values);
    free(s.cons);
    free(ofield_names);
    return ret;
}
