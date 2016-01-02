
#include <sys/types.h>
#include <stdarg.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <errno.h>
#include <float.h>

#include <unistd.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>

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

static void *sort_j128m_fun(__uint128_t key, Word_t *value, void *state)
{
    struct sortpair *pair = (struct sortpair*)state;

    pair->key = key;
    pair->value = *value;

    return ++pair;
}

struct sortpair *sort_j128m(const struct judy_128_map *j128m,
                            uint64_t *num_items)
{
    struct sortpair *pairs;

    *num_items = j128m_num_keys(j128m);

    if (*num_items == 0)
        return NULL;

    if (!(pairs = calloc(*num_items, sizeof(struct sortpair))))
        DIE("Couldn't allocate sortpairs (%llu pairs)",
            (unsigned long long)*num_items);

    j128m_fold(j128m, sort_j128m_fun, pairs);

    qsort(pairs, *num_items, sizeof(struct sortpair), compare);
    return pairs;
}

struct sortpair *sort_judyl(const Pvoid_t judy, Word_t *num_items)
{

    uint32_t i;
    struct sortpair *pairs;
    Word_t key;
    Word_t *val;
    JLC(*num_items, judy, 0, (Word_t)-1);

    if (*num_items == 0)
        return NULL;

    if (!(pairs = calloc(*num_items, sizeof(struct sortpair))))
        DIE("Couldn't allocate sortpairs (%llu pairs)",
            (unsigned long long)*num_items);

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

uint8_t bits_needed(uint64_t max)
{
    uint64_t x = max;
    uint8_t bits = x ? 0: 1;
    while (x){
        x >>= 1;
        ++bits;
    }
    return bits;
}

uint64_t parse_uint64(const char *str, const char *ctx)
{
    char *p;
    uint64_t n;

    errno = 0;
    n = strtoull(str, &p, 10);

    if (*p || errno)
        DIE("Invalid unsigned integer '%s' (%s)", str, ctx);

    return n;
}

void dsfmt_shuffle(uint64_t *arr, uint64_t len, uint32_t seed)
{
    uint64_t i;
    dsfmt_t state;

    if (len >= (uint64_t)DBL_MAX)
        DIE("Too many items to shuffle!");

    if (len > 1){
        dsfmt_init_gen_rand(&state, seed);

        for (i = 0; i < len - 1; i++){
            uint64_t j = i + (uint64_t)((double)(len - i) * dsfmt_genrand_close_open(&state));
            uint64_t t = arr[j];
            arr[j] = arr[i];
            arr[i] = t;
        }
    }
}

const char *mmap_file(const char *path, uint64_t *size)
{
    int fd;
    struct stat stats;
    const char *data;

    if ((fd = open(path, O_RDONLY)) == -1)
        DIE("Could not open path: %s", path);

    if (fstat(fd, &stats))
        DIE("Could not stat path: %s", path);

    data = mmap(NULL, (size_t)stats.st_size, PROT_READ, MAP_SHARED, fd, 0);
    *size = (uint64_t)stats.st_size;

    if (data == MAP_FAILED)
        DIE("Could not mmap path: %s", path);

    close(fd);
    return data;
}

void make_path(char path[MAX_PATH_SIZE], char *fmt, ...)
{
    va_list aptr;

    va_start(aptr, fmt);
    if (vsnprintf(path, MAX_PATH_SIZE, fmt, aptr) >= MAX_PATH_SIZE)
        DIE("Path too long (fmt %s)", fmt);
    va_end(aptr);
}
