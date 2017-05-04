
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <Judy.h>

#include <traildb.h>

#include "tdbcli.h"

static tdb *open_tdb(const char *path)
{
    tdb_error err;
    tdb *db = tdb_init();
    if ((err = tdb_open(db, path)))
        DIE("Opening a tdb at %s failed: %s", path, tdb_error_str(err));
    return db;
}

static void map_fields_and_append(tdb_cons *cons,
                                  const tdb *db,
                                  const char **dst_fields,
                                  uint64_t dst_num_fields)
{
    const char **values = NULL;
    uint64_t *lengths = NULL;
    tdb_field *field_map;
    uint64_t i, j, trail_id, tst;
    uint64_t src_num_fields = tdb_num_fields(db);

    tdb_cursor *cursor = tdb_cursor_new(db);
    if (!cursor)
        DIE("Out of memory");

    if (!(field_map = calloc(src_num_fields, sizeof(tdb_field))))
        DIE("Out of memory");

    for (i = 1; i < src_num_fields; i++){
        const char *key = tdb_get_field_name(db, i);
        for (j = 0, tst = 0; j < dst_num_fields; j++){
            if (!strcmp(key, dst_fields[j])){
                field_map[i] = j;
                tst = 1;
                break;
            }
        }
        if (!tst)
            DIE("Assert failed: Field map mismatch (%s)!\n", key);
    }

    if (!(values = malloc(dst_num_fields * sizeof(char*))))
        DIE("Out of memory\n");

    if (!(lengths = malloc(dst_num_fields * sizeof(uint64_t))))
        DIE("Out of memory\n");

    for (trail_id = 0; trail_id < tdb_num_trails(db); trail_id++){
        const tdb_event *event;
        const uint8_t *uuid = tdb_get_uuid(db, trail_id);

        if (tdb_get_trail(cursor, trail_id))
            DIE("Get_trail failed\n");

        while ((event = tdb_cursor_next(cursor))){
            memset(lengths, 0, dst_num_fields * sizeof(uint64_t));
            for (i = 0; i < event->num_items; i++){
                tdb_field src_field = tdb_item_field(event->items[i]);
                tdb_val src_val = tdb_item_val(event->items[i]);
                tdb_field dst_field = field_map[src_field];
                values[dst_field] = tdb_get_value(db,
                                                  src_field,
                                                  src_val,
                                                  &lengths[dst_field]);
            }

            if (tdb_cons_add(cons,
                             uuid,
                             event->timestamp,
                             values,
                             lengths))
                DIE("tdb_cons_add failed. Out of memory?\n");
        }
    }

    free(field_map);
    free(values);
    free(lengths);
    tdb_cursor_free(cursor);
}

static tdb **open_tdbs(const char **inputs,
                       uint32_t num_inputs,
                       const char ***out_fields,
                       uint64_t *num_fields,
                       int *equal_fields)
{
    tdb **dbs;
    uint64_t i, j, n;
    const char **fields;
    Pvoid_t dedup_fields = NULL;
    char fieldname[TDB_MAX_FIELDNAME_LENGTH + 1];
    Word_t *ptr;
    Word_t tmp;

    /*
    TODO this could support --fields a,b,c so one one could
    create a new tdb based on a subset of existing fields
    */
    if (!(dbs = malloc(num_inputs * sizeof(tdb*))))
        DIE("Out of memory");

    *num_fields = 0;
    *equal_fields = 1;
    for (i = 0; i < num_inputs; i++){
        dbs[i] = open_tdb(inputs[i]);
        n = tdb_num_fields(dbs[i]);
        if (i > 0 && n != *num_fields)
            *equal_fields = 0;

        for (j = 1; j < n; j++){
            const char *key = tdb_get_field_name(dbs[i], j);
            JSLI(ptr, dedup_fields, key);
            if (!*ptr){
                if (i > 0)
                    *equal_fields = 0;
                *ptr = (Word_t)key;
                ++*num_fields;
            }
        }
    }

    if (!(fields = malloc(*num_fields * sizeof(char*))))
        DIE("Out of memory");

    fieldname[0] = i = 0;
    JSLF(ptr, dedup_fields, fieldname);
    while (ptr){
        fields[i++] = (const char*)*ptr;
        JSLN(ptr, dedup_fields, fieldname);
    }

    JSLFA(tmp, dedup_fields);
    *out_fields = fields;
    return dbs;
}

int op_merge(struct tdbcli_options *opt,
             const char **inputs,
             uint32_t num_inputs)
{
    uint32_t i;
    tdb **dbs;
    tdb_cons *cons = tdb_cons_init();
    tdb_error err;
    const char **fields;
    uint64_t num_fields;
    int equal_fields = 0;

    if (!num_inputs)
        DIE("Specify at least one input tdb");

    if (!cons)
        DIE("Out of memory");

    dbs = open_tdbs(inputs, num_inputs, &fields, &num_fields, &equal_fields);

    if ((err = tdb_cons_open(cons,
                             opt->output,
                             fields,
                             num_fields)))
        DIE("Opening a new tdb at %s failed: %s",
            opt->output,
            tdb_error_str(err));

    if (opt->output_format_is_set)
        if (tdb_cons_set_opt(cons,
                             TDB_OPT_CONS_OUTPUT_FORMAT,
                             opt_val(opt->output_format)))
            DIE("Invalid --tdb-format. "
                "Maybe TrailDB was compiled without libarchive");

    if (opt->no_bigrams)
        if (tdb_cons_set_opt(cons,
                             TDB_OPT_CONS_NO_BIGRAMS,
                             opt_val(1)))
            DIE("Invalid --no-bigrams. "
                "TrailDB library doesn't understand TDB_OPT_CONS_NO_BIGRAMS; "
                "library not up-to-date?");

    /* set the filter if set */
    if (opt->filter_arg){
        for (i = 0; i < num_inputs; i++){
            tdb_opt_value value;
            value.ptr = parse_filter(dbs[i], opt->filter_arg, opt->verbose);
            if (tdb_set_opt(dbs[i], TDB_OPT_EVENT_FILTER, value))
                DIE("Could not set event filter");
        }
    }

    for (i = 0; i < num_inputs; i++){
        if (equal_fields){
            if ((err = tdb_cons_append(cons, dbs[i])))
                DIE("Merging %s failed: %s", inputs[i], tdb_error_str(err));
        }else
            map_fields_and_append(cons, dbs[i], fields, num_fields);

        /* field names may point to this db, so we can't close it yet */
        tdb_dontneed(dbs[i]);
    }

    if ((err = tdb_cons_finalize(cons)))
        DIE("Merging failed: %s", tdb_error_str(err));

    for (i = 0; i < num_inputs; i++)
        tdb_close(dbs[i]);

    free(fields);
    free(dbs);
    return 0;
}
