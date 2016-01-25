#ifndef __TDB_CLI_H__
#define __TDB_CLI_H__

#include <stdint.h>
#include <stdarg.h>

#include <Judy.h>

#define DIE(msg, ...)\
    do { fprintf(stderr, msg"\n", ##__VA_ARGS__);   \
         exit(EXIT_FAILURE); } while (0)

struct tdbcli_options{
    int format;
    const char *input;
    const char *output;

    /* fields */
    const uint32_t *field_map;
    uint32_t field_map_size;
    Pvoid_t json_input_fields;
    const char **fieldnames;
    uint32_t num_fields;

    /* csv */
    int csv_has_header;

    const char *delimiter;
    uint64_t output_format;
    int output_format_is_set;
};

#define FORMAT_CSV 0
#define FORMAT_JSON 1

int op_dump(const struct tdbcli_options *opt);
int op_make(const struct tdbcli_options *opt);

#endif /* __TDB_CLI_H__ */
