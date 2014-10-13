
#include "mix.h"

void op_help_count()
{
}

void *op_init_count(struct trail_ctx *ctx,
                    const char *arg,
                    int op_index,
                    int num_ops,
                    uint64_t *flags)
{
    if (arg)
        DIE("length does not accept arguments\n");

    if (!ctx->db)
        DIE("length requires a DB\n");

    *flags = TRAIL_OP_MOD_ATTR;
    if (ctx->opt_match_events)
        /* in the --match-events mode, count counts individual events */
        *flags |= TRAIL_OP_EVENT;
    else
        /* in the normal mode, count counts matching trails */
        *flags |= TRAIL_OP_POST_TRAIL;

    return NULL;
}

int op_exec_count(struct trail_ctx *ctx,
                  int mode,
                  uint64_t cookie_id,
                  const tdb_item *trail,
                  uint32_t trail_size,
                  const void *arg)
{
    Word_t *ptr;

    if (ctx->attr_type){
        if (ctx->attr_type != TRAIL_ATTR_SCALAR)
            DIE("Can not mix attribute types (count is scalar)\n");
    }else
        ctx->attr_type = TRAIL_ATTR_SCALAR;

    JLI(ptr, ctx->attributes, cookie_id);
    ++*ptr;

    return 0;
}
