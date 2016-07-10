
#define _DEFAULT_SOURCE /* strdup() */

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <strings.h>
#include <assert.h>
#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include <traildb.h>

#include "tdbcli.h"

/*

SYNTAX:

- Disjunctions are separated by space
- Conjunctions are separated by &
- Terms are one of the following:
   - field_name=value (equality)
   - field_name!=value (inequality)
   - field_name=@filename (read value from a file)
   - field= (empty value)

Example:

"author=Asimov & name=Foundation name=@name_file & price!="

*/

static tdb_item file_to_item(const tdb *db, tdb_field field, const char *path)
{
    tdb_item item = 0;
    struct stat stats;
    int fd;

    if ((fd = open(path, O_RDONLY)) == -1)
        DIE("Could not open file '%s' in the filter.", path);

    if (fstat(fd, &stats))
        DIE("Could not read file '%s' in the filter.", path);

    if (stats.st_size){
        char *p = mmap(NULL, stats.st_size, PROT_READ, MAP_SHARED, fd, 0);
        if (p == MAP_FAILED)
            DIE("Could not read file '%s' in the filter.", path);
        item = tdb_get_item(db, field, p, stats.st_size);
        munmap(p, stats.st_size);
    }else
        item = tdb_get_item(db, field, NULL, 0);

    close(fd);
    return item;
}

static tdb_item parse_term(const tdb *db,
                           const char *token,
                           int *is_negative,
                           const char *expr,
                           int verbose)
{
    tdb_field field;
    tdb_item item;

    char *val = index(token, '=');
    if (!val)
        DIE("Token '%s' is missing = in the filter '%s'", token, expr);

    if (val != token && *(val - 1) == '!'){
        *is_negative = 1;
        *(val - 1) = 0;
    }else
        *val = 0;

    if (verbose)
        fprintf(stderr, "  field='%s'", token);

    ++val;

    if (tdb_get_field(db, token, &field)){
        /* unknown field */
        if (verbose)
            fprintf(stderr, " (unknown field)");
        item = 0;
    }else{
        if (*val == '@'){
            ++val;
            if (verbose)
                fprintf(stderr, " value=FILE[%s]", val);
            item = file_to_item(db, field, val);
        }else{
            if (verbose)
                fprintf(stderr, " value='%s'", val);
            item = tdb_get_item(db, field, val, strlen(val));
        }
    }

    if (verbose)
        fprintf(stderr, " item=%"PRIu64" is_negative=%d\n", item, *is_negative);

    return item;
}

struct tdb_event_filter *parse_filter(const tdb *db,
                                      const char *filter_expression,
                                      int verbose)
{
    char *dup = strdup(filter_expression);
    char *expr = dup;
    char *ctx = NULL;
    char *token = NULL;

    struct tdb_event_filter *filter = tdb_event_filter_new();

    if (!dup || !filter)
        DIE("Out of memory.");

    while ((token = strtok_r(expr, " \n\t", &ctx))){
        int is_negative = 0;

        if (*token == '&'){
            if (verbose)
                fprintf(stderr, "AND\n");
            tdb_event_filter_new_clause(filter);
        }else{
            tdb_item item = parse_term(db, token, &is_negative, dup, verbose);
            tdb_event_filter_add_term(filter, item, is_negative);
        }

        expr = NULL;
    }

    free(dup);
    return filter;
}


#ifdef TEST_FILTER
int main(int argc, char **argv)
{
    tdb* t = tdb_init();
    assert(tdb_open(t, argv[1]) == 0);
    parse_filter(t, argv[2], 1);
    return 0;
}
#endif
