#define _GNU_SOURCE /* asprintf */
#define _DEFAULT_SOURCE /* strsep */
#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <errno.h>

#include <Judy.h>
#include "jsmn/jsmn.h"

#include <traildb.h>

#include "tdbcli.h"

static const uint8_t *parse_uuid(const char *token,
                                 uint64_t len,
                                 uint64_t lineno)
{
    static uint8_t hexuuid[32];
    static uint8_t uuid[16];

    if (len <= 32){
        memset(hexuuid, '0', 32);
        memcpy(hexuuid, token, len);
    }
    if (len > 32 || tdb_uuid_raw(hexuuid, uuid))
        DIE("Line %"PRIu64": Invalid UUID '%.*s'", lineno, (int)len, token);

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
        DIE("Field 'uuid' is missing in --fields");
    if (!(fields & 2))
        DIE("Field 'time' is missing in --fields");
}

static void populate_values(Word_t field_idx,
                            char *value,
                            uint64_t value_len,
                            const uint8_t **uuid,
                            uint64_t *tstamp,
                            const char **values,
                            uint64_t *lengths,
                            uint64_t lineno)
{
    switch (field_idx){
        case 0:
            *uuid = parse_uuid(value, value_len, lineno);
            break;
        case 1:
            value[value_len] = 0;
            *tstamp = safely_to_int(value, "timestamp");
            break;
        default:
            values[field_idx - 2] = value;
            lengths[field_idx - 2] = value_len;
    }
}

static void insert_to_tdb(tdb_cons *cons,
                          const uint8_t *uuid,
                          uint64_t tstamp,
                          const char **values,
                          uint64_t *lengths,
                          uint64_t lineno)
{
    int err;
    if (!uuid)
        DIE("Line %"PRIu64": UUID missing", lineno);
    if (tstamp == UINT64_MAX)
        DIE("Line %"PRIu64": Timestamp missing", lineno);
    if ((err = tdb_cons_add(cons, uuid, tstamp, values, lengths)))
        DIE("Line %"PRIu64": Adding event failed: %s",
            lineno,
            tdb_error_str(err));
}

static void parse_csv(tdb_cons *cons,
                      FILE *input,
                      const struct tdbcli_options *opt)
{
    char *line = NULL;
    char *parse_line = NULL;
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
        const uint8_t *uuid = NULL;
        uint64_t tstamp = UINT64_MAX;
        Word_t idx = 0;

        ++lineno;

        /* remove newline */
        line[line_len - 1] = 0;
        memset(lengths, 0, opt->num_fields * 8);
        parse_line = line;

        while ((fieldstr = strsep(&parse_line, opt->delimiter))){

            if (++idx > last_idx)
                break;

            JLG(ptr, opt->csv_input_fields, idx);
            if (ptr)
                populate_values(*ptr,
                                fieldstr,
                                strlen(fieldstr),
                                &uuid,
                                &tstamp,
                                values,
                                lengths,
                                lineno);
        }
        insert_to_tdb(cons, uuid, tstamp, values, lengths, lineno);
    }

    free(lengths);
    free(values);
    free(line);

    if (feof(input))
        return;
    else
        DIE("Premature end of input or out of memory.\n");
}

static Pvoid_t init_json_fields(const struct tdbcli_options *opt)
{
    uint8_t UUID[] = "uuid";
    uint8_t TIME[] = "time";
    uint64_t i;
    Pvoid_t json_fields = NULL;
    Word_t *ptr;

    JHSI(ptr, json_fields, UUID, 4);
    *ptr = 0;
    JHSI(ptr, json_fields, TIME, 4);
    *ptr = 1;

    for (i = 0; i < opt->num_fields; i++){
        Word_t len = strlen(opt->field_names[i]);
        JHSI(ptr, json_fields, (uint8_t*)opt->field_names[i], len);
        *ptr = i + 2;
    }
    return json_fields;
}

static void parse_json(tdb_cons *cons,
                       FILE *input,
                       const struct tdbcli_options *opt)
{
    char *line = NULL;
    size_t n = 0;
    ssize_t line_len;
    uint64_t lineno = 0;

    jsmn_parser parser;
    jsmntok_t *tokens = NULL;
    uint64_t num_tokens = 0;
    int i, ret;
    Word_t len;

    const char **values = NULL;
    uint64_t *lengths = NULL;
    Pvoid_t json_fields = init_json_fields(opt);

    if (opt->num_fields){
        if (!(values = malloc(opt->num_fields * sizeof(char*))))
            DIE("Out of memory.");
        if (!(lengths = malloc(opt->num_fields * 8)))
            DIE("Out of memory.");
    }

    while ((line_len = getline(&line, &n, input)) != -1){
        const uint8_t *uuid = NULL;
        uint64_t tstamp = UINT64_MAX;

        if (line_len > INT32_MAX)
            /* this is a jsmn library limitation (sizes are ints) */
            DIE("JSON supports at most 2GB objects");
        ++lineno;

        jsmn_init(&parser);
        while (num_tokens == 0 ||
               ((ret = jsmn_parse(&parser,
                                 line,
                                 line_len - 1,
                                 tokens,
                                 num_tokens)) == JSMN_ERROR_NOMEM)){
            num_tokens += 100000;
            free(tokens);
            if (!(tokens = malloc(num_tokens * sizeof(jsmntok_t))))
                DIE("Out of memory");
            jsmn_init(&parser);
        }

        if (ret == JSMN_ERROR_PART)
            DIE("Line %"PRIu64": Truncated JSON", lineno);
        else if (ret == JSMN_ERROR_INVAL)
            DIE("Line %"PRIu64": Corrupted JSON", lineno);

        if (ret < 1 || tokens[0].type != JSMN_OBJECT)
            DIE("Line %"PRIu64": Not a JSON object", lineno);

        memset(lengths, 0, opt->num_fields * 8);

        for (i = 1; i < ret; i += 2){
            Word_t *ptr;

            if (tokens[i].type == JSMN_STRING){
                uint8_t *key = (uint8_t*)&line[tokens[i].start];
                len = tokens[i].end - tokens[i].start;
                JHSG(ptr, json_fields, key, len);
            }else
                DIE("Line %"PRIu64": Invalid key in the JSON object", lineno);

            if (ptr){
                char *value = &line[tokens[i + 1].start];
                switch (tokens[i + 1].type){
                    case JSMN_PRIMITIVE:
                        if (value[0] == 'n')
                            /* ignore null values */
                            break;
                    case JSMN_STRING:
                        len = tokens[i + 1].end - tokens[i + 1].start;

                        populate_values(*ptr,
                                        value,
                                        len,
                                        &uuid,
                                        &tstamp,
                                        values,
                                        lengths,
                                        lineno);
                        break;
                    default:
                        DIE("Line %"PRIu64": Invalid value in the JSON object",
                            lineno);
                }
            }
        }
        insert_to_tdb(cons, uuid, tstamp, values, lengths, lineno);
    }

    JHSFA(len, json_fields);
    free(lengths);
    free(values);
    free(line);

    if (feof(input))
        return;
    else
        DIE("Premature end of input or out of memory.");
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
