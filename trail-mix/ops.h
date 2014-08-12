
void op_help_equals();
void *op_init_equals(struct trail_ctx*, const char*, int, int, uint64_t*);
int op_exec_equals(struct trail_ctx*, int, uint64_t, const uint32_t*, uint32_t, const void*);

void op_help_length();
void *op_init_length(struct trail_ctx*, const char*, int, int, uint64_t*);
int op_exec_length(struct trail_ctx*, int, uint64_t, const uint32_t*, uint32_t, const void*);

void op_help_max();
void *op_init_max(struct trail_ctx*, const char*, int, int, uint64_t*);
int op_exec_max(struct trail_ctx*, int, uint64_t, const uint32_t*, uint32_t, const void*);

void op_help_min();
void *op_init_min(struct trail_ctx*, const char*, int, int, uint64_t*);
int op_exec_min(struct trail_ctx*, int, uint64_t, const uint32_t*, uint32_t, const void*);

void op_help_not();
void *op_init_not(struct trail_ctx*, const char*, int, int, uint64_t*);
int op_exec_not(struct trail_ctx*, int, uint64_t, const uint32_t*, uint32_t, const void*);

void op_help_open();
void *op_init_open(struct trail_ctx*, const char*, int, int, uint64_t*);
int op_exec_open(struct trail_ctx*, int, uint64_t, const uint32_t*, uint32_t, const void*);

void op_help_q();
void *op_init_q(struct trail_ctx*, const char*, int, int, uint64_t*);
int op_exec_q(struct trail_ctx*, int, uint64_t, const uint32_t*, uint32_t, const void*);

static struct trail_available_op available_ops[] = {
    {"equals", op_help_equals, op_init_equals, op_exec_equals},
    {"length", op_help_length, op_init_length, op_exec_length},
    {"max", op_help_max, op_init_max, op_exec_max},
    {"min", op_help_min, op_init_min, op_exec_min},
    {"not", op_help_not, op_init_not, op_exec_not},
    {"open", op_help_open, op_init_open, op_exec_open},
    {"q", op_help_q, op_init_q, op_exec_q},
    {0, 0, 0, 0}
};
