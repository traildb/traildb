
#include "mix.h"

void op_help_limit()
{
    printf("help limit\n");
}

void *op_init_limit(struct trail_ctx *ctx,
                    const char *arg,
                    int op_index,
                    int num_ops,
                    uint64_t *flags)
{
    uint64_t *data;

    if (!(data = malloc(8)))
        DIE("Malloc failed in op_init_limit\n");

    if ((op_index == 0 ||
        (ctx->read_stdin && ctx->db && op_index == 1)))
        *flags = TRAIL_OP_CHECK_ATTR | TRAIL_OP_DB;
    else if (op_index == num_ops - 1)
        *flags = TRAIL_OP_CHECK_ATTR | TRAIL_OP_FINALIZE;
    else
        DIE("limit must be either the first or last operation\n");

    if (arg)
        *data = parse_uint64(arg, "limit");
    else
        DIE("limit requires an integer argument "
            "(maximum sum of attribute values)\n");

    return data;
}

static uint64_t sum_and_shuffle(const struct trail_ctx *ctx, uint64_t *rows)
{
    int cont;
    Word_t cookie_id = 0;
    uint64_t total = 0;
    uint32_t i = 0;
    Word_t *ptr;

    J1F(cont, ctx->matched_rows, cookie_id);
    while (cont){
        JLG(ptr, ctx->attributes, cookie_id);
        if (ptr)
            total += *ptr;
        rows[i++] = cookie_id;
        J1N(cont, ctx->matched_rows, cookie_id);
    }

    dsfmt_shuffle(rows, i, ctx->random_seed);

    return total;
}

int op_exec_limit(struct trail_ctx *ctx,
                  int mode,
                  uint64_t cookie_id,
                  const tdb_item *trail,
                  uint32_t trail_size,
                  const void *arg)
{
    if (ctx->attr_type == TRAIL_ATTR_SCALAR){
        uint64_t *rows;
        uint64_t total;
        uint64_t limit = *(uint64_t*)arg;
        Word_t num_rows;
        uint32_t i = 0;

        J1C(num_rows, ctx->matched_rows, 0, -1);
        if (!(rows = malloc(num_rows * 8)))
            DIE("Malloc failed in op_exec_limit (tried to allocate %llu rows)",
                (unsigned long long)num_rows);

        total = sum_and_shuffle(ctx, rows);

        write_counter("limit.total-events", total);

        for (i = 0; i < num_rows && total > limit; i++){
            Word_t *ptr;
            int tmp;

            JLG(ptr, ctx->attributes, rows[i]);
            if (ptr)
                total -= *ptr;
            J1U(tmp, ctx->matched_rows, rows[i]);
        }

        write_counter("limit.sampled-events", total);

        free(rows);
    }else
        /* TODO make limit work with TRAIL_ATTR_SET */
        DIE("limit requires scalar attributes\n");

    return 1;
}
