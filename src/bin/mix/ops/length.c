
#include "mix.h"

void op_help_length()
{
    INFO("help length");
}

void *op_init_length(struct trail_ctx *ctx,
                     const char *arg,
                     int op_index,
                     int num_ops,
                     uint64_t *flags)
{
    if (arg)
        DIE("length does not accept arguments");

    if (!ctx->db)
        DIE("length requires a DB");

    *flags = TRAIL_OP_POST_TRAIL | TRAIL_OP_MOD_ATTR;
    return NULL;
}

int op_exec_length(struct trail_ctx *ctx,
                   int mode,
                   uint64_t cookie_id,
                   const tdb_item *trail,
                   uint32_t trail_size,
                   const void *arg)
{
    Word_t *ptr;

    if (ctx->attr_type){
        if (ctx->attr_type != TRAIL_ATTR_SCALAR)
            DIE("Can not mix attribute types (length is scalar)");
    }else
        ctx->attr_type = TRAIL_ATTR_SCALAR;

    JLI(ptr, ctx->attributes, cookie_id);
    *ptr += trail_size;

    return 0;
}
