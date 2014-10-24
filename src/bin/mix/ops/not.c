
#include "mix.h"

void op_help_not()
{
    INFO("help not");
}

void *op_init_not(struct trail_ctx *ctx,
                  const char *arg,
                  int op_index,
                  int num_ops,
                  uint64_t *flags)
{
    uint64_t *data;

    if (!(data = malloc(8)))
        DIE("Malloc failed in op_init_not");

    if (arg)
        *data = parse_uint64(arg, "not");
    else
        DIE("not requires an integer argument (e.g. not=10)");

    *flags = TRAIL_OP_CHECK_ATTR | TRAIL_OP_PRE_TRAIL | TRAIL_OP_POST_TRAIL;
    return data;
}

int op_exec_not(struct trail_ctx *ctx,
                int mode,
                uint64_t cookie_id,
                const tdb_item *trail,
                uint32_t trail_size,
                const void *arg)
{
    Word_t *ptr;
    uint64_t val = *(uint64_t*)arg;

    JLG(ptr, ctx->attributes, cookie_id);
    if (!ptr)
        return 1;

    if (ctx->attr_type == TRAIL_ATTR_SCALAR)
        return *ptr == val ? 1: 0;
    else
        /* FIXME set cardinality */
        return 1;
}
