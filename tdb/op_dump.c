#include <unistd.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <traildb.h>
#include "tdbcli.h"

#define SAFE_FPRINTF(fmt, ...)\
    if (fprintf(output, fmt, ##__VA_ARGS__) < 1){\
        DIE("Output to %s failed (disk full?)", opt->output);\
    }

static void populate_fields(const tdb_event *event,
                            const char *hexuuid,
                            const tdb *db,
                            const struct tdbcli_options *opt,
                            const char **out_fields,
                            uint64_t *out_len,
                            uint64_t max_out_field)
{
    static char tstamp_str[21];
    uint64_t idx, len, i;

    len = sprintf(tstamp_str, "%"PRIu64, event->timestamp);
    memset(out_len, 0, max_out_field * 8);

    out_fields[opt->field_map[0] - 1] = hexuuid;
    out_len[opt->field_map[0] - 1] = 32;
    out_fields[opt->field_map[1] - 1] = tstamp_str;
    out_len[opt->field_map[1] - 1] = len;

    for (i = 0; i < event->num_items; i++){
        idx = opt->field_map[tdb_item_field(event->items[i]) + 1] - 1;
        out_fields[idx] = tdb_get_item_value(db, event->items[i], &len);
        out_len[idx] = len;
    }
}

static void dump_csv_event(FILE *output,
                           const struct tdbcli_options *opt,
                           const char **out_fields,
                           uint64_t *out_len,
                           uint64_t max_out_field)
{
    uint64_t i;

    if (out_len[0] > INT32_MAX)
        DIE("Field too large");

    SAFE_FPRINTF("%.*s", (int)out_len[0], out_fields[0]);
    for (i = 1; i < max_out_field; i++){
        if (out_len[i] > INT32_MAX)
            DIE("Field too large");
        SAFE_FPRINTF("%s%.*s", opt->delimiter, (int)out_len[i], out_fields[i]);
    }
    SAFE_FPRINTF("\n");
}

static void dump_json_event(FILE *output,
                            const struct tdbcli_options *opt,
                            const char **out_fields,
                            uint64_t *out_len,
                            uint64_t max_out_field)
{

}

static void dump_header(FILE *output, const struct tdbcli_options *opt)
{
    uint64_t i;
    SAFE_FPRINTF("%s", opt->fieldnames[0]);
    for (i = 1; i < opt->num_fields; i++){
        SAFE_FPRINTF("%s%s", opt->delimiter, opt->fieldnames[i]);
    }
    SAFE_FPRINTF("\n");
}

static void dump_trails(const tdb *db,
                        FILE *output,
                        const struct tdbcli_options *opt)
{
    const char **out_fields = NULL;
    uint64_t *out_len = NULL;
    uint64_t max_out_field = 0;
    uint64_t i, trail_id;
    int err;
    uint8_t hexuuid[32];

    tdb_cursor *cursor = tdb_cursor_new(db);
    if (!cursor)
        DIE("Out of memory.");

    for (i = 0; i < opt->field_map_size; i++)
        if (opt->field_map[i] > max_out_field)
            max_out_field = opt->field_map[i];

    if (!(out_fields = malloc(max_out_field * sizeof(char*))))
        DIE("Out of memory.");
    if (!(out_len = malloc(max_out_field * 8)))
        DIE("Out of memory.");

    if (opt->format == FORMAT_CSV && opt->csv_has_header)
        dump_header(output, opt);

    for (trail_id = 0; trail_id < tdb_num_trails(db); trail_id++){
        const tdb_event *event;

        if ((err = tdb_get_trail(cursor, trail_id)))
            DIE("Could not get %"PRIu64"th trail: %s\n",
                trail_id,
                tdb_error_str(err));

        tdb_uuid_hex(tdb_get_uuid(db, trail_id), hexuuid);

        while ((event = tdb_cursor_next(cursor))){
            populate_fields(event,
                            (const char*)hexuuid,
                            db,
                            opt,
                            out_fields,
                            out_len,
                            max_out_field);
            if (opt->format == FORMAT_CSV)
                dump_csv_event(output, opt, out_fields, out_len, max_out_field);
            else
                dump_json_event(output, opt, out_fields, out_len, max_out_field);
        }
    }

    free(out_fields);
    free(out_len);
    tdb_cursor_free(cursor);
}

int op_dump(const struct tdbcli_options *opt)
{
    FILE *output = stdout;
    int err;
    tdb *db = tdb_init();

    if (!db)
        DIE("Out of memory.");

    if (!strcmp(opt->input, "-"))
        DIE("Cannot read a tdb from stdin.");

    if (!access(opt->output, W_OK))
        DIE("Output file %s already exists.", opt->output);
    if (strcmp(opt->output, "-")){
        if (!(output = fopen(opt->input, "w")))
            DIE("Could not open output file %s", opt->output);
    }

    if ((err = tdb_open(db, opt->input)))
        DIE("Opening a tdb at %s failed: %s",
            opt->input,
            tdb_error_str(err));

    dump_trails(db, output, opt);

    if (output != stdout)
        if (fclose(output))
            DIE("Closing %s failed. Disk full?", opt->output);

    tdb_close(db);
    return 0;
}
