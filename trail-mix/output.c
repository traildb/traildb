
#include <stdio.h>
#include <stdlib.h>

#include <Judy.h>

#include <util.h>
#include <breadcrumbs_decoder.h>

#include "trail-mix.h"
#include "hex_encode.h"

static void convert_cardinalities(struct trail_ctx *ctx)
{

}

static void finalize_attributes(struct trail_ctx *ctx)
{
    /* merge input and runtime attributes */
    if (ctx->runtime_attributes){

    }
    /* convert sets to sets cardinalities if requested */
    if (ctx->opt_cardinalities && ctx->attr_type == TRAIL_ATTR_SET){

    }
}

void output_trails(struct trail_ctx *ctx)
{
    if (!ctx->db)
        DIE("Cannot output trails without a DB\n");
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

void output_matches(struct trail_ctx *ctx)
{
    Word_t row_id;
    int cont;
    FILE *out;
    const char *path;

    if (ctx->attr_type == TRAIL_ATTR_SET && !ctx->opt_binary)
        DIE("Set attributes require --binary\n");

    finalize_attributes(ctx);

    if (ctx->output_file){
        path = ctx->output_file;
        if (!(out = fopen(path, "w")))
            DIE("Could not open output file at %s\n", path);
    }else{
        path = "stdout";
        out = stdout;
    }

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
