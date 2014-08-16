
#include <stdlib.h>
#include <stdio.h>

#include <cmph.h>

#include <util.h>
#include <breadcrumbs_decoder.h>

struct cookie_ctx{
    uint32_t index;
    struct breadcrumbs *db;
    char key[16];
};

void ci_dispose(void *data, char *key, cmph_uint32 l)
{ /* no-op */
}

void ci_rewind(void *data)
{
    struct cookie_ctx *ctx = (struct cookie_ctx*)data;
    ctx->index = 0;
}

int ci_read(void *data, char **p, cmph_uint32 *len)
{
    struct cookie_ctx *ctx = (struct cookie_ctx*)data;
    const char *id = bd_lookup_cookie(ctx->db, ctx->index);

    memcpy(ctx->key, id, 16);
    *p = ctx->key;
    *len = 16;

    ++ctx->index;
    return *len;
}

void create_cookie_index(const char *path, struct breadcrumbs *bd)
{
    void *data;
    cmph_config_t *config;
    cmph_t *cmph;
    FILE *out;
    uint32_t size;

    struct cookie_ctx ctx = {
        .index = 0,
        .db = bd
    };

    cmph_io_adapter_t r = {
        .data = &ctx,
        .nkeys = bd_num_cookies(bd),
        .read = ci_read,
        .dispose = ci_dispose,
        .rewind = ci_rewind
    };

    if (!(out = fopen(path, "w")))
        DIE("Could not open output file at %s\n", path);

    if (!(config = cmph_config_new(&r)))
        DIE("cmph_config failed\n");

    cmph_config_set_algo(config, CMPH_CHM);

    if (getenv("DEBUG_CMPH"))
        cmph_config_set_verbosity(config, 5);

    if (!(cmph = cmph_new(config)))
        DIE("cmph_new failed\n");

    size = cmph_packed_size(cmph);
    if (!(data = malloc(size)))
        DIE("Could not malloc a hash of %u bytes\n", size);

    cmph_pack(cmph, data);
    SAFE_WRITE(data, size, path, out);
    SAFE_CLOSE(out, path);

    cmph_destroy(cmph);
    cmph_config_destroy(config);
    free(data);
}
