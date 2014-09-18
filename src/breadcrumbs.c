
#include <sys/mman.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

#include "util.h"

#include "breadcrumbs.h"
#include "breadcrumbs_decoder.h"

#ifdef ENABLE_COOKIE_INDEX
#include <cmph.h>
#endif

static int open_fields(const char *root, char *path, struct breadcrumbs *bd)
{
    FILE *f;
    char *line = NULL;
    size_t n = 0;
    int i = 0;

    make_path(path, "%s/fields", root);

    if (!(f = fopen(path, "r"))){
        bderror(bd, "Could not open path: %s", path);
        return -1;
    }

    while (getline(&line, &n, f) != -1)
        ++bd->num_fields;

    if (!feof(f)){
        /* we can get here if malloc fails inside getline() */
        bderror(bd, "getline failed in open_fields");
        fclose(f);
        return -1;
    }

    if (!(bd->previous_values = malloc(bd->num_fields * 4))){
        bderror(bd,
                "Could not allocate %u values in open_fields",
                bd->num_fields);
        fclose(f);
        return -1;
    }

    if (!(bd->field_names = calloc(bd->num_fields, sizeof(char*)))){
        bderror(bd,
                "Could not allocate %u values in open_fields",
                bd->num_fields);
        fclose(f);
        return -1;
    }

    if (!(bd->lexicons = calloc(bd->num_fields, sizeof(struct bdfile)))){
        bderror(bd,
                "Could not allocate %u files in open_fields",
                bd->num_fields);
        fclose(f);
        return -1;
    }

    rewind(f);
    while (getline(&line, &n, f) != -1){

        line[strlen(line) - 1] = 0;

        if (!(bd->field_names[i] = strdup(line))){
            bderror(bd, "Could allocate field name in open_fields");
            fclose(f);
            return -1;
        }

        make_path(path, "%s/lexicon.%s", root, line);
        if (mmap_file(path, &bd->lexicons[i], bd)){
            fclose(f);
            return -1;
        }

        ++i;
    }

    free(line);
    fclose(f);
    return 0;
}

static int init_field_stats(struct breadcrumbs *bd)
{
    uint32_t i;
    uint64_t *field_cardinalities;

    if (!(field_cardinalities = malloc((bd->num_fields + 1) * 8)))
        return -1;

    for (i = 0; i < bd->num_fields; i++){
        struct lexicon lex;

        if (open_lexicon(bd, &lex, i)){
            free(field_cardinalities);
            return -1;
        }

        field_cardinalities[i] = lex.size;
    }

    if (!(bd->fstats = huff_field_stats(field_cardinalities,
                                        bd->num_fields + 2,
                                        bd->max_timestamp_delta))){
        free(field_cardinalities);
        return -1;
    }

    free(field_cardinalities);
    return 0;
}

static int read_info(struct breadcrumbs *bd, const char *path)
{
    FILE *f;

    if (!(f = fopen(path, "r"))){
        bderror(bd, "Could not open path: %s", path);
        return -1;
    }

    if (fscanf(f,
               "%llu %llu %u %u %u",
               (long long unsigned int*)&bd->num_cookies,
               (long long unsigned int*)&bd->num_loglines,
               &bd->min_timestamp,
               &bd->max_timestamp,
               &bd->max_timestamp_delta) != 5){
        bderror(bd, "Invalid info file");
        return -1;
    }
    fclose(f);

    return 0;
}

struct breadcrumbs *bd_open(const char *root)
{
    char path[MAX_PATH_SIZE];
    struct breadcrumbs *bd;

    if (!(bd = calloc(1, sizeof(struct breadcrumbs))))
        return NULL;

    make_path(path, "%s/info", root);
    if (read_info(bd, path))
        goto err;

    make_path(path, "%s/cookies", root);
    if (mmap_file(path, &bd->cookies, bd))
        goto err;

    make_path(path, "%s/cookies.index", root);
    if (mmap_file(path, &bd->cookie_index, bd))
        bd->cookie_index.data = NULL;

    make_path(path, "%s/trails.codebook", root);
    if (mmap_file(path, &bd->codebook, bd))
        goto err;

    make_path(path, "%s/trails.data", root);
    if (mmap_file(path, &bd->trails, bd))
        goto err;

    if (open_fields(root, path, bd))
        goto err;

    if (init_field_stats(bd)){
        bderror(bd, "Could not init field stats");
        goto err;
    }

    return bd;
err:
    bd->error_code = 1;
    return bd;
}

void bd_close(struct breadcrumbs *bd)
{
    if (bd){
        int i = bd->num_fields;

        while (i--){
            free((char*)bd->field_names[i]);
            munmap((void*)bd->lexicons[i].data, bd->lexicons[i].size);
        }

        munmap((void*)bd->cookies.data, bd->cookies.size);
        munmap((void*)bd->codebook.data, bd->codebook.size);
        munmap((void*)bd->trails.data, bd->trails.size);

        free(bd->field_names);
        free(bd->lexicons);
        free(bd->previous_values);
        free(bd->fstats);
        free(bd);
    }
}

int bd_lookup_field_index(const struct breadcrumbs *bd, const char *field_name)
{
    int i;
    for (i = 0; i < bd->num_fields; i++)
        if (!strcmp(field_name, bd->field_names[i]))
            return i;
    return -1;
}

const char *bd_error(const struct breadcrumbs *bd)
{
    return bd->error;
}

uint32_t bd_num_cookies(const struct breadcrumbs *bd)
{
    return bd->num_cookies;
}

uint32_t bd_num_loglines(const struct breadcrumbs *bd)
{
    return bd->num_loglines;
}

uint32_t bd_num_fields(const struct breadcrumbs *bd)
{
    return bd->num_fields;
}

uint32_t bd_min_timestamp(const struct breadcrumbs *bd)
{
    return bd->min_timestamp;
}

uint32_t bd_max_timestamp(const struct breadcrumbs *bd)
{
    return bd->max_timestamp;
}

int bd_has_cookie_index(const struct breadcrumbs *bd)
{
#ifdef ENABLE_COOKIE_INDEX
    return bd->cookie_index.data ? 1: 0;
#else
    return 0;
#endif
}

int64_t bd_rlookup_cookie(const struct breadcrumbs *bd, const uint8_t key[16])
{
#ifdef ENABLE_COOKIE_INDEX
    /* (void*) cast is horrible below. I don't know why cmph_search_packed
       can't have a const modifier. This will segfault loudly if cmph tries to
       modify the read-only mmap'ed cookie_index. */
    if (bd->cookie_index.data){
        uint64_t i = cmph_search_packed((void*)bd->cookie_index.data,
                                        (const char*)key,
                                        16);

        if (i < bd->num_cookies){
            const char *cookie = bd_lookup_cookie(bd, i);
            if (!memcmp(cookie, key, 16))
                return i;
        }
        return -1;
    }else
        return -2;
#else
    return -2;
#endif
}

