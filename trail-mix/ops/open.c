
//#include <trail-util.h>

#include <util.h>
#include <breadcrumbs_decoder.h>

#include <trail-mix.h>

void op_help_open()
{
    printf("help open\n");
}

void op_init_open(struct trail_ctx *ctx,
                  const char *arg,
                  int op_index,
                  int num_ops)
{
    if (op_index > 0)
        DIE("Open must be the first operation\n");
    if (ctx->db)
        DIE("Can't open more than one traildb\n");
    if (!(ctx->db = bd_open(arg)))
        DIE("Malloc failed in op_init_open\n");
    if (ctx->db->errno)
        DIE("%s\n", bd_error(ctx->db));
    MSG(ctx, "DB %s opened\n", arg);
}

void op_exec_open(struct trail_ctx *ctx, const void *arg)
{
    /* no-op */
}
