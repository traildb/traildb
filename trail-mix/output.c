
#include <stdio.h>
#include <stdlib.h>

#include <Judy.h>

#include <util.h>
#include <breadcrumbs_decoder.h>

#include "trail-mix.h"
#include "hex_encode.h"

#define TRAIL_BUF_INCREMENT 1000000

#if 0
static void convert_cardinalities(struct trail_ctx *ctx)
{

}
#endif

static void finalize_attributes(struct trail_ctx *ctx)
{
    /* merge input and runtime attributes */
    if (ctx->runtime_attributes){

    }
    /* convert sets to sets cardinalities if requested */
    if (ctx->opt_cardinalities && ctx->attr_type == TRAIL_ATTR_SET){

    }
}

static void output_binary(FILE *out,
                          const char *id,
                          Word_t *attr,
                          const char *path)
{

}

static void output_text(FILE *out,
                        const char *id,
                        Word_t *attr,
                        const char *path)
{
    static char hexencoded[32];
    hex_encode((const uint8_t*)id, hexencoded);

    if (attr){
        SAFE_FPRINTF(out,
                     path,
                     "%s %llu\n",
                     hexencoded,
                     (long long unsigned int)*attr);
    }else
        SAFE_FPRINTF(out, path, "%s\n", hexencoded);
}

static FILE *open_output(const struct trail_ctx *ctx, const char **path)
{
    FILE *out;

    if (ctx->output_file){
        *path = ctx->output_file;
        if (!(out = fopen(*path, "w")))
            DIE("Could not open output file at %s\n", *path);
    }else{
        *path = "stdout";
        out = stdout;
    }
    return out;
}

void output_matches(struct trail_ctx *ctx)
{
    Word_t row_id;
    int cont;
    const char *path;
    FILE *out = open_output(ctx, &path);

    if (ctx->attr_type == TRAIL_ATTR_SET && !ctx->opt_binary)
        DIE("Set attributes require --binary\n");

    finalize_attributes(ctx);

    if (ctx->output_file || ctx->opt_binary)
        setvbuf(out, NULL, _IOFBF, 10485760);

    row_id = 0;
    J1F(cont, ctx->matched_rows, row_id);
    while (cont){
        const char *id;
        Word_t *attr;

        if (ctx->db)
            id = bd_lookup_cookie(ctx->db, row_id);
        else
            id = &ctx->input_ids[row_id * 16];

        JLG(attr, ctx->attributes, row_id);

        if (ctx->opt_binary)
            output_binary(out, id, attr, path);
        else
            output_text(out, id, attr, path);

        J1N(cont, ctx->matched_rows, row_id);
    }

    SAFE_CLOSE(out, path);
}

static Pvoid_t serialize_trails(FILE *out, struct trail_ctx *ctx)
{
    uint64_t row_id = 0;
    int cont, tmp;
    uint32_t *trail = NULL;
    uint32_t trail_size = 0;
    uint32_t i, n, len = 0;
    Pvoid_t fieldset = NULL;

    J1F(cont, ctx->matched_rows, row_id);
    while (cont){
        /*
          A serialized trail is structured as follows:
          ID: 16-byte ID
          NUMBER-OF-EVENTS: 4-byte uint
          TRAILS:
            TIMESTAMP: 4-byte uint
            N*FIELD-VALUE: 4-byte uint, maps to lexicon, where N = len(fields)
        */
        const char *id = bd_lookup_cookie(ctx->db, row_id);

        while ((len = bd_trail_decode(ctx->db,
                                      row_id,
                                      trail,
                                      trail_size,
                                      0)) == trail_size){
            free(trail);
            trail_size += TRAIL_BUF_INCREMENT;
            if (!(trail = malloc(trail_size * 4)))
                DIE("Could not allocate trail buffer of %u items\n",
                     trail_size);
        }

        /*
           We dare to use fwrite instead of SAFE_WRITE here with the assumption
           that if the writes fail, e.g. due to running out of disk space,
           one of the subsequent writes will fail too and capture the error.
        */
        fwrite(id, 16, 1, out);

        /* count the number of events by checking the number of end markers */
        n = 0;
        for (i = 0; i < len; i++)
            if (!trail[i])
                ++n;

        fwrite(&n, 4, 1, out);

        if (len){
            fwrite(&trail[0], 4, 1, out); /* write initial timestamp */
            for (i = 1; i < len; i++){
                if (trail[i]){ /* skip event delimitiers */
                    if (trail[i] >> 8){
                        fwrite(&trail[i], 4, 1, out);
                        J1S(tmp, fieldset, trail[i]);
                    }else{
                        /* FIXME this is a bit strange format */
                        /* we should include field id in the null values */
                        tmp = 0;
                        fwrite(&tmp, 4, 1, out);
                    }
                }else{
                    /* write timestamp that follows event delimiter,
                       don't add it to fieldset */
                    if (++i < len)
                        fwrite(&trail[i], 4, 1, out);
                }
            }
        }

        J1N(cont, ctx->matched_rows, row_id);
    }

    free(trail);
    return fieldset;
}

static void serialize_fields(FILE *out, struct trail_ctx *ctx)
{
    static const char *TIMESTAMP = "timestamp";
    uint32_t i, len = strlen(TIMESTAMP);

    fwrite(&len, 4, 1, out);
    fwrite(TIMESTAMP, len, 1, out);

    for (i = 0; i < ctx->db->num_fields; i++){
        len = strlen(ctx->db->field_names[i]);

        /* see a comment above about fwrite */
        fwrite(&len, 4, 1, out);
        fwrite(ctx->db->field_names[i], len, 1, out);
    }
}

static void serialize_lexicon(FILE *out,
                              const Pvoid_t fieldset,
                              struct trail_ctx *ctx)
{
    Word_t field = 0;
    int cont;

    J1F(cont, fieldset, field);
    while (cont){
        const char *str = bd_lookup_value(ctx->db, field);
        uint32_t len = strlen(str);

        /* see a comment above about fwrite */
        fwrite(&field, 4, 1, out);
        fwrite(&len, 4, 1, out);
        fwrite(str, len, 1, out);

        J1N(cont, fieldset, field);
    }
}

void output_trails(struct trail_ctx *ctx)
{
    const char *path;
    FILE *out = open_output(ctx, &path);
    char *buf = NULL;
    size_t buf_size = 0;
    uint32_t offsets[3] = {12, 0, 0};
    Pvoid_t fieldset = NULL;
    Word_t tmp;

    /*
    Blob is structured in four distinct sections:

    HEADER  [ body offset | fields offset | lexicon offset ]
    BODY    [ trails data ]
    FIELDS  [ list of field names ]
    LEXICON [ field - field value pairs ]
    */

    if (out == stdout)
        if (!(out = open_memstream(&buf, &buf_size)))
            DIE("Could not initialize memstream in output_trails\n");

    SAFE_SEEK(out, 12, path);

    fieldset = serialize_trails(out, ctx);
    SAFE_TELL(out, offsets[1], path);

    serialize_fields(out, ctx);
    SAFE_TELL(out, offsets[2], path);

    serialize_lexicon(out, fieldset, ctx);

    SAFE_TELL(out, tmp, path);
    if (tmp >= UINT32_MAX)
        DIE("Trying to output over 4GB of data which is not supported yet\n");

    SAFE_FLUSH(out, path);
    if (buf_size){
        memcpy(buf, offsets, 12);
        SAFE_WRITE(buf, buf_size, path, stdout);
        fclose(stdout);
    }else{
        /* rewinding doesn't work with memstream! */
        SAFE_SEEK(out, 0, path);
        SAFE_WRITE(offsets, 12, path, out);
    }

    SAFE_CLOSE(out, path);
    free(buf);
    J1FA(tmp, fieldset);
}
