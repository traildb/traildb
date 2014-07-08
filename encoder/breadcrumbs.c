
#include <sys/mman.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

#include "util.h"

#include "breadcrumbs.h"
#include "breadcrumbs_decoder.h"

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

    if (!(bd->previous_values = malloc(bd->num_fields * 4))){
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
        make_path(path, "%s/lexicon.%s", root, line);
        if (mmap_file(path, &bd->lexicons[i++], bd)){
            fclose(f);
            return -1;
        }
    }

    fclose(f);
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
               "%u %u %u %u",
               &bd->num_cookies,
               &bd->num_loglines,
               &bd->min_timestamp,
               &bd->max_timestamp) != 4){
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

    make_path(path, "%s/trails.codebook", root);
    if (mmap_file(path, &bd->codebook, bd))
        goto err;

    make_path(path, "%s/trails.data", root);
    if (mmap_file(path, &bd->trails, bd))
        goto err;

    if (open_fields(root, path, bd))
        goto err;

    return bd;
err:
    bd->errno = 1;
    return bd;
}

void bd_close(struct breadcrumbs *bd)
{
    if (bd){
        int i = bd->num_fields;

        while (i--)
            munmap((void*)bd->lexicons[i].data, bd->lexicons[i].size);

        munmap((void*)bd->cookies.data, bd->cookies.size);
        munmap((void*)bd->codebook.data, bd->codebook.size);
        munmap((void*)bd->trails.data, bd->trails.size);

        free(bd->lexicons);
        free(bd->previous_values);
        free(bd);
    }
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


