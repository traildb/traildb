
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

static int compare(const void *p1, const void *p2)
{
    const struct sortpair *x = (const struct sortpair*)p1;
    const struct sortpair *y = (const struct sortpair*)p2;

    if (x->value > y->value)
        return -1;
    else if (x->value < y->value)
        return 1;
    return 0;
}

struct sortpair *sort_judyl(const Pvoid_t judy, Word_t *num_items)
{
    uint32_t i;
    struct sortpair *pairs;
    Word_t key;
    Word_t *val;

    JLC(*num_items, judy, 0, -1);

    if (!(pairs = calloc(*num_items, sizeof(struct sortpair))))
        DIE("Couldn't allocate sortpairs (%llu pairs)\n",
            (unsigned long long)num_items);

    key = i = 0;
    JLF(val, judy, key);
    while (val){
        pairs[i].key = key;
        pairs[i++].value = *val;
        JLN(val, judy, key);
    }
    qsort(pairs, *num_items, sizeof(struct sortpair), compare);
    return pairs;
}

uint32_t bits_needed(uint32_t max)
{
    uint32_t x = max;
    uint32_t bits = x ? 0: 1;
    while (x){
        x >>= 1;
        ++bits;
    }
    return bits;
}

void make_path(char path[MAX_PATH_SIZE], char *fmt, ...)
{
    va_list aptr;

    va_start(aptr, fmt);
    if (vsnprintf(path, MAX_PATH_SIZE, fmt, aptr) >= MAX_PATH_SIZE)
        DIE("Path too long (fmt %s)\n", fmt);
    va_end(aptr);
}

void bderror(struct breadcrumbs *bd, char *fmt, ...)
{
    if (bd){
        va_list aptr;

        va_start(aptr, fmt);
        vsnprintf(bd->error, BD_ERROR_SIZE, fmt, aptr);
        va_end(aptr);
    }
}


int mmap_file(const char *path, struct bdfile *dst, struct breadcrumbs *bd)
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

