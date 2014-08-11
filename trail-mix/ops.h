
void op_help_q();
void *op_init_q(struct trail_ctx*, const char*, int, int);
void op_exec_q(struct trail_ctx*, const void*);

void op_help_open();
void *op_init_open(struct trail_ctx*, const char*, int, int);
void op_exec_open(struct trail_ctx*, const void*);

static struct trail_available_op available_ops[] = {
    {"q", op_help_q, op_init_q, op_exec_q},
    {"open", op_help_open, op_init_open, op_exec_open},
    {0, 0, 0, 0}
};

