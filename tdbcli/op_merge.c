
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

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

static tdb **open_and_validate(const char **inputs,
                               uint32_t num_inputs,
                               const char ***out_fields,
                               uint64_t *num_fields)
{
    tdb **dbs;
    uint64_t i, j;
    const char **fields;

    /*
    TODO this could support --fields a,b,c so one one could
    create a new tdb based on a subset of existing fields
    */

    /* validate fields */
    if (!(dbs = malloc(num_inputs * sizeof(tdb*))))
        DIE("Out of memory");

    dbs[0] = open_tdb(inputs[0]);
    *num_fields = tdb_num_fields(dbs[0]) - 1;
    if (!(fields = malloc(*num_fields * sizeof(char*))))
        DIE("Out of memory");
    /* skip time field */
    for (i = 0; i < *num_fields; i++)
        fields[i] = tdb_get_field_name(dbs[0], i + 1);

    for (i = 1; i < num_inputs; i++){
        dbs[i] = open_tdb(inputs[i]);

        if (tdb_num_fields(dbs[i]) - 1 != *num_fields)
            DIE("The number of fields differ in %s (%u fields) and %s (%u fields)",
                inputs[0], *num_fields, inputs[i], tdb_num_fields(dbs[i]) - 1);

        for (j = 0; j < *num_fields; j++){
            if (strcmp(fields[j], tdb_get_field_name(dbs[i], j + 1)))
                DIE("The %uth field differ in %s and %s (%s != %s)",
                    j,
                    inputs[0],
                    inputs[i],
                    tdb_get_field_name(dbs[i], j + 1),
                    fields[j]);
        }
    }

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

    if (!num_inputs)
        DIE("Specify at least one input tdb");

    dbs = open_and_validate(inputs, num_inputs, &fields, &num_fields);

    if (!cons)
        DIE("Out of memory");

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
        if ((err = tdb_cons_append(cons, dbs[i])))
            DIE("Merging %s failed: %s", inputs[i], tdb_error_str(err));
        tdb_close(dbs[i]);
    }

    if ((err = tdb_cons_finalize(cons)))
        DIE("Merging failed: %s", tdb_error_str(err));

    free(fields);
    free(dbs);
    return 0;
}
