#define _DEFAULT_SOURCE

#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <strings.h>

#include <traildb.h>

#include "tdbcli.h"
#include "tdb_index.h"

#define SAFE_FPRINTF(fmt, ...)\
    if (fprintf(output, fmt, ##__VA_ARGS__) < 1){\
        DIE("Output to %s failed (disk full?)", opt->output);\
    }

static void populate_fields(const tdb_event *event,
                            const char *hexuuid,
                            const tdb *db,
                            const struct tdbcli_options *opt,
                            const char **out_values,
                            uint64_t *out_lengths)
{
    static char tstamp_str[21];
    uint64_t idx, len, i;

    memset(out_lengths, 0, opt->num_fields * 8);

    if (opt->output_fields[0]){
        out_values[opt->output_fields[0] - 1] = hexuuid;
        out_lengths[opt->output_fields[0] - 1] = 32;
    }
    if (opt->output_fields[1]){
        len = sprintf(tstamp_str, "%"PRIu64, event->timestamp);
        out_values[opt->output_fields[1] - 1] = tstamp_str;
        out_lengths[opt->output_fields[1] - 1] = len;
    }
    for (i = 0; i < event->num_items; i++){
        idx = opt->output_fields[tdb_item_field(event->items[i]) + 1];
        if (idx){
            out_values[idx - 1] = tdb_get_item_value(db, event->items[i], &len);
            if (len > INT32_MAX)
                DIE("Value too large (over 2GB!)");
            out_lengths[idx - 1] = len;
        }
    }
}

static void dump_csv_event(FILE *output,
                           const struct tdbcli_options *opt,
                           const char **out_values,
                           const uint64_t *out_lengths)
{
    uint64_t i;

    if (out_lengths[0])
        SAFE_FPRINTF("%.*s", (int)out_lengths[0], out_values[0]);
    for (i = 1; i < opt->num_fields; i++){
        SAFE_FPRINTF("%s%.*s",
                     opt->delimiter,
                     (int)out_lengths[i],
                     out_values[i]);
    }
    SAFE_FPRINTF("\n");
}

/* Add a backslash before the given character. If no occurences of the
   character was found, returns the original string. If escaping is
   necessary, a new string is allocated which must later be freed. The
   length of the new string is set in the 'outlen' argument. */

uint8_t * escape(const uint8_t *s, uint64_t len, uint64_t *outlen, uint8_t c) {
    /* How many chars do we need to escape? */
    int num = 0;
    for (int i = 0; i < len; i++)
        if (s[i] == c)
            num++;

    if (!num)
        return (uint8_t *) s;

    /* Allocate a buffer with enough space */
    uint8_t *buf = malloc(len+num);
    if(!buf)
        DIE("Out of memory.");

    /* Iterate through the source string, copying characters and
       adding escape characters as necessary. */
    int j = 0;
    for (int i = 0; i < len; i++) {
        if (s[i] == c)
            buf[j++] = '\\';

        buf[j++] = s[i];
    }
    *outlen = j;
    return buf;
}

static void dump_json_event(FILE *output,
                            const struct tdbcli_options *opt,
                            const char **out_values,
                            uint64_t *out_lengths)
{
    const char PREFIX1[] = "";
    const char PREFIX2[] = ", ";
    const char *prefix = PREFIX1;
    uint64_t i;

    SAFE_FPRINTF("{");
    for (i = 0; i < opt->num_fields; i++)
        if (out_lengths[i] || !opt->json_no_empty){

            uint64_t len = out_lengths[i];
            const uint8_t *str = out_values[i];
            uint8_t *maybe_escaped = escape(str, len, &len, '"');

            SAFE_FPRINTF("%s\"%s\": \"%.*s\"",
                         prefix,
                         opt->field_names[i],
                         (int)len,
                         maybe_escaped);

            if (str != maybe_escaped)
                free(maybe_escaped);

            prefix = PREFIX2;
        }
    SAFE_FPRINTF("}\n");
}

static void dump_header(FILE *output, const struct tdbcli_options *opt)
{
    uint64_t i;
    SAFE_FPRINTF("%s", opt->field_names[0]);
    for (i = 1; i < opt->num_fields; i++){
        SAFE_FPRINTF("%s%s", opt->delimiter, opt->field_names[i]);
    }
    SAFE_FPRINTF("\n");
}

static void dump_trails(const tdb *db,
                        FILE *output,
                        const struct tdbcli_options *opt,
                        const uint64_t *trail_filter,
                        uint64_t num_trails)
{
    const char **out_values = NULL;
    uint64_t *out_lengths = NULL;
    uint64_t i;
    uint8_t hexuuid[32];
    int err;

    tdb_cursor *cursor = tdb_cursor_new(db);
    if (!cursor)
        DIE("Out of memory.");

    if (!(out_values = malloc(opt->num_fields * sizeof(char*))))
        DIE("Out of memory.");
    if (!(out_lengths = malloc(opt->num_fields * 8)))
        DIE("Out of memory.");

    if (opt->format == FORMAT_CSV && opt->csv_has_header)
        dump_header(output, opt);

    if (!trail_filter)
        num_trails = tdb_num_trails(db);

    for (i = 0; i < num_trails; i++){
        const tdb_event *event;
        uint64_t trail_id = trail_filter ? trail_filter[i]: i;

        if ((err = tdb_get_trail(cursor, trail_id)))
            DIE("Could not get %"PRIu64"th trail: %s\n",
                trail_id,
                tdb_error_str(err));

        if (tdb_cursor_peek(cursor)){
            tdb_uuid_hex(tdb_get_uuid(db, trail_id), hexuuid);

            while ((event = tdb_cursor_next(cursor))){
                populate_fields(event,
                                (const char*)hexuuid,
                                db,
                                opt,
                                out_values,
                                out_lengths);
                if (opt->format == FORMAT_CSV)
                    dump_csv_event(output, opt, out_values, out_lengths);
                else
                    dump_json_event(output, opt, out_values, out_lengths);
            }
        }
    }

    free(out_values);
    free(out_lengths);
    tdb_cursor_free(cursor);
}

static void init_fields_from_arg(struct tdbcli_options *opt, const tdb *db)
{
    char *fieldstr;
    tdb_field field;
    uint64_t idx = 1;

    if (opt->fields_arg){

        if (index(opt->fields_arg, ':'))
            DIE("Field indices in --field are not supported with dump");

        while ((fieldstr = strsep(&opt->fields_arg, ","))){
            if (!strcmp(fieldstr, "uuid"))
                opt->output_fields[0] = idx;
            else if (!strcmp(fieldstr, "time"))
                opt->output_fields[1] = idx;
            else if (tdb_get_field(db, fieldstr, &field))
                DIE("Field not found: '%s'", fieldstr);
            else
                opt->output_fields[field + 1] = idx;
            opt->field_names[idx - 1] = fieldstr;
            if (++idx == TDB_MAX_NUM_FIELDS + 2)
                DIE("Too many fields");
        }
        opt->num_fields = idx - 1;
    }else{
        opt->field_names[0] = "uuid";
        opt->output_fields[0] = 1;

        for (field = 0; field < tdb_num_fields(db); field++){
            opt->field_names[field + 1] = tdb_get_field_name(db, field);
            opt->output_fields[field + 1] = field + 2;
        }
        opt->num_fields = tdb_num_fields(db) + 1;
    }
}

int op_dump(struct tdbcli_options *opt)
{
    FILE *output = stdout;
    int err;
    tdb *db = tdb_init();

    struct tdb_event_filter *filter = NULL;
    uint64_t *trail_filter = NULL;
    uint64_t num_trails;

    if (!db)
        DIE("Out of memory.");

    if (!strcmp(opt->input, "-"))
        DIE("Cannot read a tdb from stdin.");

    if (!access(opt->output, W_OK))
        DIE("Output file %s already exists.", opt->output);
    if (strcmp(opt->output, "-")){
        if (!(output = fopen(opt->output, "w")))
            DIE("Could not open output file %s", opt->output);
    }

    if ((err = tdb_open(db, opt->input)))
        DIE("Opening a tdb at %s failed: %s",
            opt->input,
            tdb_error_str(err));

    filter = apply_filter(db, opt);
    if (filter){
        const char *index_path = NULL;
        char *free_path = NULL;

        if (!opt->no_index &&
            ((index_path = opt->index_path) ||
             (index_path = free_path = tdb_index_find(opt->input)))){

            struct tdb_index *index = tdb_index_open(opt->input, index_path);
            trail_filter = tdb_index_match_candidates(index,
                                                      filter,
                                                      &num_trails);
            if (opt->verbose)
                fprintf(stderr,
                        "Using index at %s. "
                        "Evaluating %"PRIu64"/%"PRIu64" (%2.2f%%) trails.\n",
                        index_path,
                        num_trails,
                        tdb_num_trails(db),
                        (100. * num_trails) / tdb_num_trails(db));
            tdb_index_close(index);
        }else if (opt->verbose)
            fprintf(stderr, "Not using an index.\n");

        free(free_path);
    }

    init_fields_from_arg(opt, db);
    if (opt->num_fields)
        dump_trails(db, output, opt, trail_filter, num_trails);

    if (output != stdout)
        if (fclose(output))
            DIE("Closing %s failed. Disk full?", opt->output);

    if (filter)
        tdb_event_filter_free(filter);

    free(trail_filter);
    tdb_close(db);
    return 0;
}

