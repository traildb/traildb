#include <stdlib.h>
#include <stdio.h>

#include <Judy.h>

#include <breadcrumbs_decoder.h>
#include <hex_decode.h>

#include "util.h"
#include "arena.h"

#include "trail-mix.h"

static struct arena input_ids = {.arena_increment = 1000000,
                                 .item_size = 16};

static Pvoid_t index_db_ids(const struct trail_ctx *ctx)
{
    uint8_t key[16];
    Pvoid_t index = NULL;
    uint32_t i;

    for (i = 0; i < bd_num_cookies(ctx->db); i++){
        const char *id = bd_lookup_cookie(ctx->db, i);
        Word_t *ptr;
        memcpy(key, id, 16);
        JHSI(ptr, index, key, 16);
        *ptr = i + 1;
    }
    return index;
}

static int parse_binary(const uint8_t keybuf[33],
                        Pvoid_t *id_index,
                        struct trail_ctx *ctx)
{
    return 0;
}

static int parse_text(const uint8_t keybuf[33],
                      Pvoid_t *id_index,
                      struct trail_ctx *ctx)
{
    uint8_t id[16];
    Word_t row_id = 0;
    int tmp;
    Word_t *ptr;

    if (hex_decode((const char*)keybuf, id))
        DIE("Invalid ID: %s\n", keybuf);

    if (ctx->db){
        JHSG(ptr, *id_index, id, 16);
        if (ptr)
            row_id = *ptr;
    }else{
        JHSI(ptr, *id_index, id, 16);
        if (*ptr)
            row_id = *ptr;
        else{
            void *dst = arena_add_item(&input_ids);
            memcpy(dst, id, 16);
            row_id = *ptr = input_ids.next;
            if (input_ids.next == UINT32_MAX)
                DIE("Too many input IDs (over 2^32!)\n");
        }
    }

    if (row_id){
        --row_id;

        J1S(tmp, ctx->matched_rows, row_id);

        if (keybuf[32] == ' '){
            unsigned long long attr_value;

            if (ctx->attr_type == 0)
                ctx->attr_type = TRAIL_ATTR_SCALAR;
            else if (ctx->attr_type != TRAIL_ATTR_SCALAR)
                DIE("Cannot mix set and scalar attributes "
                    "(offending ID: %*s)\n", 32, keybuf);

            if (fscanf(stdin, "%llu", &attr_value) == 1){
                Word_t *attr;
                JLI(attr, ctx->attributes, row_id);
                *attr += attr_value;
            }else
                DIE("Invalid attribute value "
                    "(offending ID: %*s)\n", 32, keybuf);

        }else if (keybuf[32] != '\n')
            DIE("Invalid input (offending ID: %*s)\n", 32, keybuf);

        return 1;
    }else
        return 0;
}

void input_parse_stdin(struct trail_ctx *ctx)
{
    Pvoid_t id_index = NULL;
    uint8_t keybuf[33];
    unsigned long long num_lines = 0;
    unsigned long long num_matches = 0;
    Word_t tmp;

    if (ctx->db)
        id_index = index_db_ids(ctx);

    while (1){
        int n = fread(keybuf, 1, 17, stdin);
        if (!n)
            break;
        else if (n != 17)
            DIE("Truncated input after %llu lines\n", num_lines);

        ++num_lines;

        if (keybuf[0] < 128){
            SAFE_FREAD(stdin, "stdin", &keybuf[17], 16);
            if (parse_text(keybuf, &id_index, ctx))
                ++num_matches;
        }else
            parse_binary(keybuf, &id_index, ctx);
    }

    MSG(ctx, "%llu lines read, %llu lines match\n", num_lines, num_matches);

    ctx->input_ids = input_ids.data;
    JHSFA(tmp, id_index);
}

void input_choose_all_rows(struct trail_ctx *ctx)
{
    uint32_t tmp, i;
    for (i = 0; i < bd_num_cookies(ctx->db); i++)
        J1S(tmp, ctx->matched_rows, i);
}
