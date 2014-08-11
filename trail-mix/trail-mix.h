
#ifndef __TRAIL_MIX_H__
#define __TRAIL_MIX_H__

#include <Judy.h>

#include <breadcrumbs.h>

#define TRAIL_ATTR_SCALAR 1
#define TRAIL_ATTR_SET 2

struct trail_ctx;

typedef void (*op_help_t)(void);

typedef void* (*op_init_t)(struct trail_ctx *ctx,
                           const char *arg,
                           int op_index,
                           int num_ops);

typedef void (*op_exec_t)(struct trail_ctx *ctx, const void *arg);

struct trail_available_op{
    const char *name;
    op_help_t help;
    op_init_t init;
    op_exec_t exec;
};

struct trail_op{
    const struct trail_available_op *op;
    const void *arg;
};

struct trail_ctx{
    /* settings */
    int opt_match_events;
    int opt_cardinalities;
    int opt_output_trails;
    int opt_binary;
    int opt_verbose;

    const char *output_file;

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

