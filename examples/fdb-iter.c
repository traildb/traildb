#include <fcntl.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#include <traildb.h>
#include "tdb_funnel.h"

int main(int argc, char **argv) {
    char *path = argc > 1 ? argv[1] : "a.tdb/funnel.db";
    int row = argc > 2 ? atoi(argv[2]) : 0;
    int terms = argc > 3 ? atoi(argv[3]) : -1;
    int fd = open(path, O_RDONLY);
    fdb *fdb;
    if (fd < 0 || !(fdb = fdb_load(fd))) {
        printf("Bad funneldb: %s\n", path);
        return 1;
    }

    fdb_cnf cnf = {
        .num_clauses = 1,
        .clauses = &(fdb_clause){.terms = terms}
    };

    fdb_set set = {
        .flags = FDB_SIMPLE,
        .simple = {
            .db = fdb,
            .funnel_id = row,
            .cnf = &cnf
        }
    };

    fdb_iter *iter = fdb_iter_new(&set);
    fdb_elem *elem;
    while ((elem = fdb_iter_next(iter)))
        printf("%u %u\n", elem->id, elem->mask);

    fdb_iter_free(iter);
    fdb_free(fdb);
    return 0;
}