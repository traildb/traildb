#define _GNU_SOURCE /* asprintf */
#define _DEFAULT_SOURCE /* strsep */
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include <Judy.h>

#include <traildb.h>

#include "tdbcli.h"

static const uint8_t *parse_uuid(const char *token, uint64_t lineno)
{
    static uint8_t hexuuid[32];
    static uint8_t uuid[16];
    uint64_t len = strlen(token);

    if (len <= 32){
        memset(hexuuid, '0', 32);
        memcpy(hexuuid, token, len);
    }
    if (len > 32 || tdb_uuid_raw(hexuuid, uuid))
        DIE("Line %"PRIu64": Invalid UUID '%s'\n", lineno, token);

    return uuid;
}

static void populate_fields(const char *fieldstr,
                            Word_t idx,
                            struct tdbcli_options *opt,
                            int *fields)
{
    Word_t *ptr;

    if (!strcmp(fieldstr, "uuid")){
        JLI(ptr, opt->csv_input_fields, idx);
        *ptr = 0;
        *fields |= 1;
    }else if (!strcmp(fieldstr, "time")){
        JLI(ptr, opt->csv_input_fields, idx);
        *ptr = 1;
        *fields |= 2;
    }else{
        JLI(ptr, opt->csv_input_fields, idx);
        *ptr = opt->num_fields + 2;
        opt->field_names[opt->num_fields] = fieldstr;
        if (++opt->num_fields == TDB_MAX_NUM_FIELDS)
            DIE("Too many fields");
    }
}

static void init_fields_from_header(FILE *input, struct tdbcli_options *opt)
{
    const char *fieldstr;
    char *line = NULL;
    size_t n = 0;
    uint64_t idx = 0;
    int fields = 0;

    ssize_t line_len = getline(&line, &n, input);
    if (line_len < 2)
        DIE("Could not read header line");
    line[line_len - 1] = 0;

    while ((fieldstr = strsep(&line, opt->delimiter)))
        populate_fields(fieldstr, ++idx, opt, &fields);

    if (!(fields & 1))
        DIE("Field 'uuid' is missing in input");
    if (!(fields & 2))
        DIE("Field 'time' is missing in input");
}

static void init_fields_from_arg(struct tdbcli_options *opt)
{
    char *fieldstr;
    uint64_t idx = 0;
    int fields = 0;

    if (opt->fields_arg){
        if (index(opt->fields_arg, ':')){
            while ((fieldstr = strsep(&opt->fields_arg, ","))){
                const char *field_idx = strsep(&fieldstr, ":");
                if (!field_idx)
                    DIE("Specify field index for all fields in --field");
                idx = safely_to_int(field_idx, "field index");
                populate_fields(fieldstr, idx, opt, &fields);
            }
        }else
            while ((fieldstr = strsep(&opt->fields_arg, ",")))
                populate_fields(fieldstr, ++idx, opt, &fields);
    }else{
        populate_fields("uuid", ++idx, opt, &fields);
        populate_fields("time", ++idx, opt, &fields);
    }
    if (!(fields & 1))
        DIE("Field 'uuid' is missing in input");
    if (!(fields & 2))
        DIE("Field 'time' is missing in input");
}

static void parse_csv(tdb_cons *cons,
                      FILE *input,
                      const struct tdbcli_options *opt)
{
    char *line = NULL;
    char *fieldstr = NULL;
    size_t n = 0;
    ssize_t line_len;
    uint64_t lineno = 0;
    Word_t last_idx = -1;
    Word_t *ptr;

    const char **values = NULL;
    uint64_t *lengths = NULL;

    if (opt->num_fields){
        if (!(values = malloc(opt->num_fields * sizeof(char*))))
            DIE("Out of memory.");
        if (!(lengths = malloc(opt->num_fields * 8)))
            DIE("Out of memory.");
    }

    JLL(ptr, opt->csv_input_fields, last_idx);

    while ((line_len = getline(&line, &n, input)) != -1){
        const uint8_t *uuid;
        uint32_t val_idx;
        uint64_t tstamp = 0;
        int tstamp_set = 0;
        int uuid_set = 0;
        int err;
        Word_t idx = 0;

        ++lineno;

        /* remove newline */
        line[line_len - 1] = 0;
        memset(lengths, 0, opt->num_fields * 8);

        while ((fieldstr = strsep(&line, opt->delimiter))){

            if (++idx > last_idx)
                break;

            JLG(ptr, opt->csv_input_fields, idx);
            if (ptr){
                switch (*ptr){
                    case 0:
                        uuid = parse_uuid(fieldstr, lineno);
                        uuid_set = 1;
                        break;
                    case 1:
                        tstamp = safely_to_int(fieldstr, "timestamp");
                        tstamp_set = 1;
                        break;
                    default:
                        val_idx = *ptr - 2;
                        values[val_idx] = fieldstr;
                        lengths[val_idx] = strlen(fieldstr);
                }
            }
        }

        if (!uuid_set)
            DIE("Line %"PRIu64": UUID missing\n", lineno);
        if (!tstamp_set)
            DIE("Line %"PRIu64": Timestamp missing\n", lineno);
        if ((err = tdb_cons_add(cons, uuid, tstamp, values, lengths)))
            DIE("Line %"PRIu64": Adding event failed: %s\n",
                lineno,
                tdb_error_str(err));

    }

    free(lengths);
    free(values);
    free(line);

    if (feof(input))
        return;
    else
        DIE("Premature end of input or out of memory.");
}

static void parse_json(tdb_cons *cons,
                       FILE *input,
                       const struct tdbcli_options *opt)
{

}

int op_make(struct tdbcli_options *opt)
{
    FILE *input = stdin;
    int err;
    tdb_cons *cons = tdb_cons_init();
    char *output_file = NULL;

    if (!cons)
        DIE("out of memory.");

    if (opt->output_format_is_set)
        if (tdb_cons_set_opt(cons,
                             TDB_OPT_CONS_OUTPUT_FORMAT,
                             opt_val(opt->output_format)))
            DIE("Invalid --tdb-format. "
                "Maybe TrailDB was compiled without libarchive");

    if (strcmp(opt->input, "-")){
        if (!(input = fopen(opt->input, "r")))
            DIE("Could not open input file %s", opt->input);
    }

    if (!strcmp(opt->output, "-"))
        DIE("Cannot output a tdb to stdout.");
    if (!access(opt->output, W_OK))
        DIE("Output file %s already exists.", opt->output);
    if (asprintf(&output_file, "%s.tdb", opt->output) < 1)
        DIE("Out of memory");
    if (!access(output_file, W_OK))
        DIE("Output file %s already exists.", output_file);
    free(output_file);

    if (opt->csv_has_header)
        init_fields_from_header(input, opt);
    else
        init_fields_from_arg(opt);

    if ((err = tdb_cons_open(cons,
                             opt->output,
                             opt->field_names,
                             opt->num_fields)))
        DIE("Opening a new tdb at %s failed: %s",
            opt->output,
            tdb_error_str(err));

    if (opt->format == FORMAT_CSV)
        parse_csv(cons, input, opt);
    else
        parse_json(cons, input, opt);

    if ((err = tdb_cons_finalize(cons)))
        DIE("Finalizing a new tdb at %s failed: %s",
            opt->output,
            tdb_error_str(err));

    tdb_cons_close(cons);
    if (input != stdin)
        fclose(input);
    return 0;
}
