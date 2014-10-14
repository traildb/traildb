
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
        DIE("Path too long (fmt %s)\n", fmt);
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
    dst->size = stats.st_size;

    dst->data = mmap(NULL, stats.st_size, PROT_READ, MAP_SHARED, fd, 0);
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

    tdb_path(path, "%s/fields", root);

    if (!(f = fopen(path, "r"))){
        tdb_err(db, "Could not open path: %s", path);
        return -1;
    }

    while (getline(&line, &n, f) != -1)
        ++db->num_fields;

    if (!feof(f)){
        /* we can get here if malloc fails inside getline() */
        tdb_err(db, "getline failed when opening fields");
        fclose(f);
        return -1;
    }

    if (!(db->previous_values = malloc(db->num_fields * 4))){
        tdb_err(db, "Could not alloc %u values", db->num_fields);
        fclose(f);
        return -1;
    }

    if (!(db->field_names = calloc(db->num_fields, sizeof(char*)))){
        tdb_err(db, "Could not alloc %u values", db->num_fields);
        fclose(f);
        return -1;
    }

    if (!(db->lexicons = calloc(db->num_fields, sizeof(tdb_file)))){
        tdb_err(db, "Could not alloc %u files", db->num_fields);
        fclose(f);
        return -1;
    }

    rewind(f);
    while (getline(&line, &n, f) != -1){

        line[strlen(line) - 1] = 0;

        if (!(db->field_names[i] = strdup(line))){
            tdb_err(db, "Could not allocate field name");
            fclose(f);
            return -1;
        }

        tdb_path(path, "%s/lexicon.%s", root, line);
        if (tdb_mmap(path, &db->lexicons[i], db)){
            fclose(f);
            return -1;
        }

        ++i;
    }

    free(line);
    fclose(f);
    return 0;
}

static int init_field_stats(tdb *db)
{
    uint32_t i;
    uint64_t *field_cardinalities;

    if (!(field_cardinalities = malloc((db->num_fields + 1) * 8)))
        return -1;

    for (i = 0; i < db->num_fields; i++){
        tdb_lexicon lex;

        if (tdb_lexicon_read(db, &lex, i)){
            free(field_cardinalities);
            return -1;
        }

        field_cardinalities[i] = lex.size;
    }

    if (!(db->field_stats = huff_field_stats(field_cardinalities,
                                             db->num_fields + 2,
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
               "%llu %llu %u %u %u",
               (long long unsigned int*)&db->num_cookies,
               (long long unsigned int*)&db->num_events,
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

    if (tdb_fields_open(db, root, path))
        goto err;

    if (init_field_stats(db)){
        tdb_err(db, "Could not init field stats");
        goto err;
    }

    return db;
err:
    db->error_code = 1;
    return db;
}

void tdb_close(tdb *db)
{
    if (db){
        int i = db->num_fields;

        while (i--){
            free((char*)db->field_names[i]);
            munmap((void*)db->lexicons[i].data, db->lexicons[i].size);
        }

        munmap((void*)db->cookies.data, db->cookies.size);
        munmap((void*)db->codebook.data, db->codebook.size);
        munmap((void*)db->trails.data, db->trails.size);

        free(db->lexicons);
        free(db->previous_values);
        free(db->field_names);
        free(db->field_stats);
        free(db);
    }
}

int tdb_lexicon_read(tdb *db, tdb_lexicon *lex, tdb_field field)
{
    if (field < db->num_fields){
        lex->size = *(uint32_t*)db->lexicons[field].data;
        lex->toc = (const uint32_t*)&db->lexicons[field].data[4];
        lex->data = (const char*)db->lexicons[field].data;
        return 0;
    }
    tdb_err(db, "Invalid field: %d", field);
    return -1;
}

int tdb_lexicon_size(tdb *db, tdb_field field, uint32_t *size)
{
    tdb_lexicon lex;

    if (!tdb_lexicon_read(db, &lex, field)){
        *size = lex.size;
        return 0;
    }
    return -1;
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

tdb_item tdb_get_item(tdb *db, tdb_field field, const char *value)
{
    tdb_lexicon lex;
    if (!tdb_lexicon_read(db, &lex, field)){
        uint32_t i;
        if (*value){
            for (i = 0; i < lex.size; i++)
                if (!strcmp(&lex.data[lex.toc[i]], value))
                    return (field + 1) | ((i + 1) << 8);
        }else{
            /* valid empty value */
            return field + 1;
        }
    }
    return 0;
}

const char *tdb_get_value(tdb *db, tdb_field field, tdb_val val)
{
    tdb_lexicon lex;
    if (val && !tdb_lexicon_read(db, &lex, field) && (val - 1) < lex.size)
        return &lex.data[lex.toc[val - 1]];
    return NULL;
}

const char *tdb_get_item_value(tdb *db, tdb_item item)
{
    return tdb_get_value(db, tdb_item_field(item) - 1, tdb_item_val(item));
}

tdb_cookie tdb_get_cookie(tdb *db, uint64_t cookie_id)
{
    if (cookie_id < db->num_cookies)
        return (tdb_cookie)&db->cookies.data[cookie_id * 16];
    return NULL;
}

/* Returns -1 if cookie not found, or -2 if cookie_index is disabled */
int64_t tdb_get_cookie_id(tdb *db, const tdb_cookie cookie)
{
#ifdef ENABLE_COOKIE_INDEX
    /* (void*) cast is horrible below. I don't know why cmph_search_packed
       can't have a const modifier. This will segfault loudly if cmph tries to
       modify the read-only mmap'ed cookie_index. */
    if (db->cookie_index.data){
        uint64_t id = cmph_search_packed((void*)db->cookie_index.data,
                                         (const char*)cookie,
                                         16);

        if (id < db->num_cookies){
            if (!memcmp(tdb_get_cookie(db, id), cookie, 16))
                return id;
        }
        return -1;
    }else
        return -2;
#else
    return -2;
#endif
}

int tdb_has_cookie_index(tdb *db)
{
#ifdef ENABLE_COOKIE_INDEX
    return db->cookie_index.data ? 1: 0;
#else
    return 0;
#endif
}

const char *tdb_error(const tdb *db)
{
    return db->error;
}

uint32_t tdb_num_cookies(const tdb *db)
{
    return db->num_cookies;
}

uint32_t tdb_num_events(const tdb *db)
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
