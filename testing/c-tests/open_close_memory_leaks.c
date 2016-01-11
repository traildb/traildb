#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <sys/time.h>
#include <sys/resource.h>

#include <traildb.h>

#define NUM_CONS_ITER 10
#define NUM_OPEN_ITER 100
#define NUM_FIELDS 1000

/* we may need to adjust this limit as buffer sizes change etc. */
#define MEM_LIMIT (300 * 1024 * 1024)

static void do_cons(const char *root, int do_finalize)
{
    static uint8_t uuid[16];
    char* fields = malloc(NUM_FIELDS * 11);
    assert(fields != NULL);
    const char **fields_ptr = malloc(NUM_FIELDS * sizeof(char*));
    assert(fields_ptr != NULL);
    uint64_t *lengths = malloc(NUM_FIELDS * 8);
    assert(lengths != NULL);

    uint32_t i;
    for (i = 0; i < NUM_FIELDS; i++ ){
        fields_ptr[i] = &fields[i * 11];
        sprintf(&fields[i * 11], "%u", i);
        lengths[i] = 1;
    }

    tdb_cons* c = tdb_cons_init();
    assert(tdb_cons_open(c, root, fields_ptr, NUM_FIELDS) == 0);
    assert(tdb_cons_add(c, uuid, 0, fields_ptr, lengths) == 0);

    if (do_finalize)
        assert(tdb_cons_finalize(c) == 0);
    tdb_cons_close(c);

    free(lengths);
    free(fields_ptr);
    free(fields);
}

static void do_open(const char *path)
{
    tdb_item *items;
    uint64_t n, items_len = 0;

    tdb* t = tdb_init();
    assert(tdb_open(t, path) == 0);
    assert(tdb_get_trail(t, 0, &items, &items_len, &n, 0) == 0);
    assert(n == NUM_FIELDS + 2);
    tdb_close(t);

    free(items);
}

int main(int argc, char** argv)
{
    uint32_t i = 0;
    struct rlimit limit = {.rlim_cur = MEM_LIMIT,
                           .rlim_max = MEM_LIMIT};

    assert(setrlimit(RLIMIT_AS, &limit) == 0);

    for (i = 0; i < NUM_CONS_ITER; i++)
        do_cons(argv[1], 0);

    for (i = 0; i < NUM_CONS_ITER; i++)
        do_cons(argv[1], 1);

    for (i = 0; i < NUM_OPEN_ITER; i++)
        do_open(argv[1]);

    return 0;
}
