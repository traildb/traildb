
void op_help_length();
void *op_init_length(struct trail_ctx*, const char*, int, int, uint64_t*);
int op_exec_length(struct trail_ctx*, int, uint64_t, const uint32_t*, uint32_t, const void*);

void op_help_open();
void *op_init_open(struct trail_ctx*, const char*, int, int, uint64_t*);
int op_exec_open(struct trail_ctx*, int, uint64_t, const uint32_t*, uint32_t, const void*);

void op_help_q();
void *op_init_q(struct trail_ctx*, const char*, int, int, uint64_t*);
int op_exec_q(struct trail_ctx*, int, uint64_t, const uint32_t*, uint32_t, const void*);

static struct trail_available_op available_ops[] = {
    {"length", op_help_length, op_init_length, op_exec_length},
    {"open", op_help_open, op_init_open, op_exec_open},
    {"q", op_help_q, op_init_q, op_exec_q},
    {0, 0, 0, 0}
};
