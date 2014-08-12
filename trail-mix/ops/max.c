
#include <util.h>
#include <trail-mix.h>

void op_help_max()
{
    printf("help max\n");
}

void *op_init_max(struct trail_ctx *ctx,
                  const char *arg,
                  int op_index,
                  int num_ops,
                  uint64_t *flags)
{
    uint64_t *data;

    if (!(data = malloc(8)))
        DIE("Malloc failed in op_init_max\n");

    if (arg)
        *data = parse_uint64(arg, "max");
    else
        DIE("max requires an integer argument (e.g. max=10)\n");

    *flags = TRAIL_OP_CHECK_ATTR | TRAIL_OP_PRE_TRAIL | TRAIL_OP_POST_TRAIL;
    return data;
}

int op_exec_max(struct trail_ctx *ctx,
                int mode,
                uint64_t row_id,
                const uint32_t *fields,
                uint32_t num_fields,
                const void *arg)
{
    Word_t *ptr;
    uint64_t val = *(uint64_t*)arg;

    JLG(ptr, ctx->attributes, row_id);
    if (!ptr)
        return 1;

    if (ctx->attr_type == TRAIL_ATTR_SCALAR)
        return *ptr > val ? 1: 0;
    else
        /* FIXME set cardinality */
        return 1;
}
