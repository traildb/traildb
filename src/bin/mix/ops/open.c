
#include "mix.h"

void op_help_open()
{
    printf("help open\n");
}

void *op_init_open(struct trail_ctx *ctx,
                   const char *arg,
                   int op_index,
                   int num_ops,
                   uint64_t *flags)
{
    if (op_index > 0)
        DIE("Open must be the first operation\n");
    if (ctx->db)
        DIE("Can't open more than one traildb\n");
    if (!(ctx->db = tdb_open(arg)))
        DIE("Malloc failed in op_init_open\n");
    if (ctx->db->error_code)
        DIE("%s\n", tdb_error(ctx->db));

    ctx->db_path = arg;
    *flags = 0;
    MSG(ctx, "DB %s opened\n", arg);

    return NULL;
}

int op_exec_open(struct trail_ctx *ctx,
                 int mode,
                 uint64_t cookie_id,
                 const tdb_item *trail,
                 uint32_t trail_size,
                 const void *arg)
{
    return 1;
}
