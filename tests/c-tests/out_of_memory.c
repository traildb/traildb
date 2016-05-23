#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

#include <sys/time.h>
#include <sys/resource.h>

#include <traildb.h>
#include "tdb_test.h"

/* this is a hack that allows us to check arena_increment below */
#define SIZEOF_OFF_T 8
#define SIZEOF_SIZE_T 8
#include <tdb_internal.h>

/* we may need to adjust this limit as buffer sizes change etc. */
#define MEM_LIMIT (50 * 1024 * 1024)
#define MAX_MEM_MULTIPLIER 10
/* VALUE_SIZE < 8 so we end up using the small mode of judy_str_map */
#define VALUE_SIZE 7
#define NUM_ITER 100000000

int main(int argc, char** argv)
{
    static uint8_t uuid[16];
    static char value[VALUE_SIZE];
    const char *values[] = {value};
    uint64_t lengths[] = {VALUE_SIZE};
    const char *fields[] = {"a"};
    uint32_t m, i;

    /* setrlimit() accepts only decreasing limits */
    for (m = MAX_MEM_MULTIPLIER; m > 1; m--){
        struct rlimit limit = {.rlim_cur = MEM_LIMIT * m,
                               .rlim_max = MEM_LIMIT * m};

        assert(setrlimit(RLIMIT_AS, &limit) == 0);

        tdb_cons* c = tdb_cons_init();
        test_cons_settings(c);
        assert(tdb_cons_open(c, argv[1], fields, 1) == 0);

        /* the reason for this check is to ensure that we are using a
           debug version of libtraildb, compiled with
           -DEVENTS_ARENA_INCREMENT=100, which makes it easier to surface
           Judy malloc bugs (otherwise it would be arena realloc always
           failing)
        */
        assert(c->events.arena_increment == 100);

        for (i = 0; i < NUM_ITER; i++){
            memcpy(value, &i, 4);
            int ret = tdb_cons_add(c, uuid, 0, values, lengths);
            assert(ret == 0 || ret == TDB_ERR_NOMEM);
            if (ret == TDB_ERR_NOMEM){
                break;
            }
        }

        tdb_cons_close(c);
    }

    return 0;
}
