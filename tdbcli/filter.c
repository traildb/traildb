
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

static struct tdb_event_filter *parse_filter(const tdb *db,
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

static void apply_uuid(const char *uuidstr,
                       tdb *db,
                       struct tdb_event_filter *filter,
                       uint32_t *num_invalid,
                       uint32_t *num_missing)
{
    static uint8_t uuid[16];
    uint64_t trail_id;
    tdb_opt_value value = {.ptr = filter};

    if (strlen(uuidstr) != 32 || tdb_uuid_raw(uuidstr, uuid)){
        ++*num_invalid;
        return;
    }
    if (tdb_get_trail_id(db, uuid, &trail_id)){
        ++*num_missing;
        return;
    }
    if (tdb_set_trail_opt(db, trail_id, TDB_OPT_EVENT_FILTER, value))
        DIE("Could not set event filter");
}

static void apply_uuids_from_file(const char *fname,
                                  tdb *db,
                                  struct tdb_event_filter *filter,
                                  uint32_t *num_uuids,
                                  uint32_t *num_invalid,
                                  uint32_t *num_missing)
{
    FILE *f;
    char *line = NULL;
    size_t n = 0;

    if (!(f = fopen(fname, "r")))
        DIE("Could not read UUIDs from %s\n", fname);

    while (getline(&line, &n, f) != -1){
        ++*num_uuids;
        line[strlen(line) - 1] = 0;
        apply_uuid(line, db, filter, num_invalid, num_missing);
    }

    fclose(f);
    free(line);
}

static void apply_uuids(tdb *db,
                        struct tdb_event_filter *filter,
                        struct tdbcli_options *opt)
{
    char *dup = strdup(opt->uuids);
    uint32_t num_uuids = 0;
    uint32_t num_invalid = 0;
    uint32_t num_missing = 0;

    if (dup[0] == '@')
        apply_uuids_from_file(&dup[1],
                              db,
                              filter,
                              &num_uuids,
                              &num_invalid,
                              &num_missing);
    else{
        char *uuidstr;
        while ((uuidstr = strsep(&dup, ","))){
            ++num_uuids;
            apply_uuid(uuidstr, db, filter, &num_invalid, &num_missing);
        }
    }
    if (opt->verbose)
        fprintf(stderr,
                "Found %u UUIDs: %u selected, %u missing, %u invalid.\n",
                num_uuids,
                num_uuids - (num_invalid + num_missing),
                num_missing,
                num_invalid);
    free(dup);
}

struct tdb_event_filter *apply_filter(tdb *db, struct tdbcli_options *opt)
{
    struct tdb_event_filter *f = NULL;
    tdb_opt_value value;

    if (opt->filter_arg)
        f = parse_filter(db, opt->filter_arg, opt->verbose);

    if (opt->uuids){
        /* all and none are never freed, not a biggie */
        if (f)
            apply_uuids(db, f, opt);
        else
            apply_uuids(db, tdb_event_filter_new_match_all(), opt);
        value.ptr = tdb_event_filter_new_match_none();
    }else
        value.ptr = f;

    if (tdb_set_opt(db, TDB_OPT_EVENT_FILTER, value))
        DIE("Could not set event filter");

    return f;
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
