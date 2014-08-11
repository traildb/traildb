
#include <trail-mix.h>

void op_help_q()
{

}

void *op_init_q(struct trail_ctx *ctx,
                const char *arg,
                int op_index,
                int num_ops,
                uint64_t *flags)
{
    #if 0
    if index_enabled
        *flags = TRAIL_OP_DB
        if (!ctx.opt_index_only){
            if (!(only-one-term && !opt_match_events))
                *flags |= TRAIL_OP_EVENT;
        }
    else
        *flags = TRAIL_OP_EVENT;
    #endif
    return NULL;
}

int op_exec_q(struct trail_ctx *ctx,
              int mode,
              uint64_t row_id,
              const uint32_t fields,
              uint32_t num_fields,
              const void *arg)
{
    return 1;
}
