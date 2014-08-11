#!/bin/bash

for fname in ops/*.c
do
    OP=`basename $fname .c`
    echo "
void op_help_$OP();
void *op_init_$OP(struct trail_ctx*, const char*, int, int, uint64_t*);
int op_exec_$OP(struct trail_ctx*, int, uint64_t, const uint32_t*, uint32_t, const void*);"
done

echo
echo "static struct trail_available_op available_ops[] = {"

for fname in ops/*.c
do
    OP=`basename $fname .c`
    echo "    {\"$OP\", op_help_$OP, op_init_$OP, op_exec_$OP},"
done

echo "    {0, 0, 0, 0}"
echo "};"

