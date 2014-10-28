
#include <stdio.h>
#include <stdlib.h>

#include <Judy.h>

#include "tdb_internal.h"
#include "util.h"
#include "mix.h"

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
                          const uint8_t *cookie,
                          Word_t *attr,
                          const char *path)
{

}

static void output_text(FILE *out,
                        const uint8_t *cookie,
                        Word_t *attr,
                        const char *path)
{
    static uint8_t hexcookie[32];
    tdb_cookie_hex(cookie, hexcookie);

    if (attr){
        SAFE_FPRINTF(out,
                     path,
                     "%s %llu\n",
                     hexcookie,
                     (long long unsigned int)*attr);
    }else{
        SAFE_FPRINTF(out, path, "%s\n", hexcookie);
    }
}

static FILE *open_output(const struct trail_ctx *ctx, const char **path)
{
    FILE *out;

    if (ctx->output_file){
        *path = ctx->output_file;
        if (!(out = fopen(*path, "w")))
            DIE("Could not open output file at %s", *path);
    }else{
        *path = "stdout";
        out = stdout;
    }
    return out;
}

void output_matches(struct trail_ctx *ctx)
{
    Word_t cookie_id = 0;
    int cont;
    const char *path;
    FILE *out = open_output(ctx, &path);

    if (ctx->attr_type == TRAIL_ATTR_SET && !ctx->opt_binary)
        DIE("Set attributes require --binary");

    finalize_attributes(ctx);

    if (ctx->output_file || ctx->opt_binary)
        setvbuf(out, NULL, _IOFBF, 10485760);

    J1F(cont, ctx->matched_rows, cookie_id);
    while (cont){
        const uint8_t *cookie;
        Word_t *attr;

        if (ctx->db)
            cookie = tdb_get_cookie(ctx->db, cookie_id);
        else
            cookie = (uint8_t*)&ctx->input_ids[cookie_id * 16];

        JLG(attr, ctx->attributes, cookie_id);

        if (ctx->opt_binary)
            output_binary(out, cookie, attr, path);
        else
            output_text(out, cookie, attr, path);

        J1N(cont, ctx->matched_rows, cookie_id);
    }

    SAFE_CLOSE(out, path);
}

static Pvoid_t serialize_trails(FILE *out, struct trail_ctx *ctx)
{
    Word_t cookie_id = 0;
    int cont, tmp;
    uint32_t *trail = NULL;
    uint32_t trail_size = 0;
    uint32_t i, n, len = 0;
    Pvoid_t items = NULL;

    J1F(cont, ctx->matched_rows, cookie_id);
    while (cont){
        /*
          A serialized trail is structured as follows:
          ID: 16-byte ID
          NUMBER-OF-EVENTS: 4-byte uint
          TRAILS:
            TIMESTAMP: 4-byte uint
            N*ITEM-VALUE: 4-byte uint, maps to lexicon, where N = len(items)
        */
        const uint8_t *cookie = tdb_get_cookie(ctx->db, cookie_id);

        while ((len = tdb_decode_trail(ctx->db,
                                       cookie_id,
                                       trail,
                                       trail_size,
                                       0)) == trail_size){
            free(trail);
            trail_size += TRAIL_BUF_INCREMENT;
            if (!(trail = malloc(trail_size * 4)))
                DIE("Could not allocate trail buffer of %u items",
                     trail_size);
        }

        /*
           We dare to use fwrite instead of SAFE_WRITE here with the assumption
           that if the writes fail, e.g. due to running out of disk space,
           one of the subsequent writes will fail too and capture the error.
        */
        fwrite(cookie, 16, 1, out);

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
                        J1S(tmp, items, trail[i]);
                    }else{
                        /* FIXME this is a bit strange format */
                        /* we should include field id in the null values */
                        tmp = 0;
                        fwrite(&tmp, 4, 1, out);
                    }
                }else{
                    /* write timestamp that follows event delimiter,
                       don't add it to items */
                    if (++i < len)
                        fwrite(&trail[i], 4, 1, out);
                }
            }
        }

        J1N(cont, ctx->matched_rows, cookie_id);
    }

    free(trail);
    return items;
}

static void serialize_fields(FILE *out, struct trail_ctx *ctx)
{
    static const char *TIMESTAMP = "timestamp";
    uint32_t i, len = strlen(TIMESTAMP);

    fwrite(&len, 4, 1, out);
    fwrite(TIMESTAMP, len, 1, out);

    for (i = 1; i < ctx->db->num_fields; i++){
        len = strlen(ctx->db->field_names[i]);

        /* see a comment above about fwrite */
        fwrite(&len, 4, 1, out);
        fwrite(ctx->db->field_names[i], len, 1, out);
    }
}

static void serialize_lexicon(FILE *out,
                              const Pvoid_t items,
                              struct trail_ctx *ctx)
{
    Word_t item = 0;
    int cont;

    J1F(cont, items, item);
    while (cont){
        const char *str = tdb_get_item_value(ctx->db, item);
        uint32_t len = strlen(str);

        /* see a comment above about fwrite */
        fwrite(&item, 4, 1, out);
        fwrite(&len, 4, 1, out);
        fwrite(str, len, 1, out);

        J1N(cont, items, item);
    }
}

void output_trails(struct trail_ctx *ctx)
{
    const char *path;
    FILE *out = open_output(ctx, &path);
    char *buf = NULL;
    size_t buf_size = 0;
    Pvoid_t items = NULL;
    uint64_t tmp;
    uint64_t offset = 24;
    uint64_t sizes[3];

    /*
    Blob is structured in four distinct sections:

    HEADER  [ body size (64bit) | fields size (64bit) | lexicon size (64bit) ]
    BODY    [ trails data ]
    FIELDS  [ list of field names ]
    LEXICON [ field - field value pairs ]
    */

    if (out == stdout)
        if (!(out = open_memstream(&buf, &buf_size)))
            DIE("Could not initialize memstream in output_trails");

    SAFE_SEEK(out, 24, path);

    items = serialize_trails(out, ctx);
    SAFE_TELL(out, tmp, path);
    sizes[0] = tmp - offset;
    offset = tmp;

    serialize_fields(out, ctx);
    SAFE_TELL(out, tmp, path);
    sizes[1] = tmp - offset;
    offset = tmp;

    serialize_lexicon(out, items, ctx);
    SAFE_TELL(out, tmp, path);
    sizes[2] = tmp - offset;

    SAFE_FLUSH(out, path);
    if (buf_size){
        memcpy(buf, sizes, 24);
        SAFE_WRITE(buf, buf_size, path, stdout);
        fclose(stdout);
    }else{
        /* rewinding doesn't work with memstream! */
        SAFE_SEEK(out, 0, path);
        SAFE_WRITE(sizes, 24, path, out);
    }

    SAFE_CLOSE(out, path);
    free(buf);
    J1FA(tmp, items);
}
