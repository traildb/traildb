
#include <sys/types.h>
#include <sys/stat.h>
#include <sys/mman.h>
#include <unistd.h>
#include <fcntl.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>

#include "util.h"

#define ERROR_SIZE (MAX_PATH_SIZE + 512)

struct bdfile{
    const char *data;
    uint64_t size;
};

struct breadcrumbs{
    uint32_t min_timestamp;
    uint32_t max_timestamp;
    uint32_t num_cookies;
    uint32_t num_loglines;
    uint32_t num_fields;
    uint32_t *previous_values;

    struct bdfile cookies;
    struct bdfile codebook;
    struct bdfile trails;
    struct bdfile *lexicons;

    int errno;
    char error[ERROR_SIZE];
};

static void bderror(struct breadcrumbs *bd, char *fmt, ...)
{
    va_list aptr;

    va_start(aptr, fmt);
    vsnprintf(bd->error, ERROR_SIZE, fmt, aptr);
    va_end(aptr);
}

static int mmap_file(const char *path,
                     struct bdfile *dst,
                     struct breadcrumbs *bd)
{
    int fd;
    struct stat stats;

    if ((fd = open(path, O_RDONLY)) == -1){
        bderror(bd, "Could not open path: %s", path);
        return -1;
    }

    if (fstat(fd, &stats)){
        bderror(bd, "Could not stat path: %s", path);
        close(fd);
        return -1;
    }
    dst->size = stats.st_size;

    dst->data = mmap(NULL, stats.st_size, PROT_READ, MAP_SHARED, fd, 0);
    if (dst->data == MAP_FAILED){
        bderror(bd, "Could not mmap path: %s", path);
        close(fd);
        return -1;
    }

    close(fd);
    return 0;
}

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
