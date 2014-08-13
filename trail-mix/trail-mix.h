
#ifndef __TRAIL_MIX_H__
#define __TRAIL_MIX_H__

#include <Judy.h>

#ifdef ENABLE_DISCODB
#include <discodb.h>
#endif

#include <breadcrumbs.h>

#define TRAIL_ATTR_SCALAR 1
#define TRAIL_ATTR_SET 2

#define TRAIL_OP_DB 1
#define TRAIL_OP_PRE_TRAIL 2
#define TRAIL_OP_POST_TRAIL 4
#define TRAIL_OP_EVENT 8
#define TRAIL_OP_FINALIZE 16

#define TRAIL_OP_MOD_ATTR 32
#define TRAIL_OP_CHECK_ATTR 64

struct trail_ctx;

typedef void (*op_help_t)(void);

typedef void* (*op_init_t)(struct trail_ctx *ctx,
                           const char *arg,
                           int op_index,
                           int num_ops,
                           uint64_t *flags);

typedef int (*op_exec_t)(struct trail_ctx *ctx,
                         int mode,
                         uint64_t row_id,
                         const uint32_t *fields,
                         const uint32_t num_fields,
                         const void *arg);

struct trail_available_op{
    const char *name;
    op_help_t help;
    op_init_t init;
    op_exec_t exec;
};

struct trail_op{
    const struct trail_available_op *op;
    const void *arg;
    uint64_t flags;
};

struct trail_ctx{
    /* settings */
    int opt_match_events;
    int opt_cardinalities;
    int opt_output_trails;
    int opt_output_count;
    int opt_binary;
    int opt_verbose;
    int opt_no_index;

    uint32_t random_seed;

    /* input and output */
    int read_stdin;
    const char *output_file;
    const char *input_file;

    /* operations */
    int num_ops;
    struct trail_op *ops;

    /* matched rows */
    Pvoid_t matched_rows;
    const char *input_ids;

    /* attributes */
    Pvoid_t runtime_attributes;
    Pvoid_t attributes;
    int attr_type;

    /* trail db */
    struct breadcrumbs *db;
    const char *db_path;
#ifdef ENABLE_DISCODB
    struct ddb *db_index;
#endif
};

/* input.c */
void input_parse_stdin(struct trail_ctx *ctx);
void input_choose_all_rows(struct trail_ctx *ctx);

/* output.c */
void output_matches(struct trail_ctx *ctx);
void output_trails(struct trail_ctx *ctx);

#define MSG(ctx, msg, ...)\
     { if ((ctx)->opt_verbose) fprintf(stderr, msg, ##__VA_ARGS__); }

#endif /* __TRAIL_MIX_H__ */

