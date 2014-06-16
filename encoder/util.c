
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

void make_path(char path[MAX_PATH_SIZE], char *fmt, ...)
{
    va_list aptr;

    va_start(aptr, fmt);
    if (vsnprintf(path, MAX_PATH_SIZE, fmt, aptr) >= MAX_PATH_SIZE)
        DIE("Path too long (fmt %s)\n", fmt);
    va_end(aptr);
}
