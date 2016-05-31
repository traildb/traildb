#ifndef __TDB_CLI_H__
#define __TDB_CLI_H__

#include <stdint.h>
#include <stdarg.h>

#include <Judy.h>

#include <traildb.h>

#define ERR_OR_DIE(die, msg, ...)\
    do { fprintf(stderr, msg"\n", ##__VA_ARGS__); \
         if (die) { exit(EXIT_FAILURE); } \
       } while (0)

#define DIE(msg, ...)\
    do { fprintf(stderr, msg"\n", ##__VA_ARGS__);   \
         exit(EXIT_FAILURE); } while (0)

struct tdbcli_options{
    int format;
    const char *input;
    const char *output;

    char *fields_arg;

    /* fields */
    Pvoid_t csv_input_fields;
    uint64_t output_fields[TDB_MAX_NUM_FIELDS + 2];
    const char *field_names[TDB_MAX_NUM_FIELDS];
    uint32_t num_fields;

    /* filter */
    const char *filter_arg;

    /* csv */
    int csv_has_header;

    /* json */
    int json_no_empty;

    const char *delimiter;
    uint64_t output_format;
    int output_format_is_set;
    int skip_bad_input;
    int verbose;
};

#define FORMAT_CSV 0
#define FORMAT_JSON 1

long int safely_to_int(const char *str, const char *field);

struct tdb_event_filter *parse_filter(const tdb *db,
                                      const char *filter_expression,
                                      int verbose);

int op_dump(struct tdbcli_options *opt);
int op_make(struct tdbcli_options *opt);

#endif /* __TDB_CLI_H__ */
