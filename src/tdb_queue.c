#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>

#include "tdb_queue.h"

struct tdb_queue{
    void **q;
    uint32_t max;
    uint32_t head;
    uint32_t tail;
    uint32_t count;
};

struct tdb_queue *tdb_queue_new(uint32_t max_length)
{
    struct tdb_queue *q = NULL;
    if (!max_length)
        return NULL;
    if (!(q = malloc(sizeof(struct tdb_queue))))
        return NULL;
    if (!(q->q = malloc(max_length * sizeof(void*))))
        return NULL;
    q->max = max_length;
    q->head = q->tail = q->count = 0;
    return q;
}

void tdb_queue_free(struct tdb_queue *q)
{
    free(q->q);
    free(q);
}

void tdb_queue_push(struct tdb_queue *q, void *e)
{
    if (q->max == q->count++){
        fprintf(stderr, "tdb_queue_push: max=%d, count=%d "
            "(this should never happen!)", q->max, q->count);
        abort();
    }
    q->q[q->head++ % q->max] = e;
}

void *tdb_queue_pop(struct tdb_queue *q)
{
    if (!q->count)
        return NULL;
    --q->count;
    return q->q[q->tail++ % q->max];
}

int tdb_queue_length(const struct tdb_queue *q)
{
    return q->count;
}

void *tdb_queue_peek(const struct tdb_queue *q)
{
    if (!q->count)
        return NULL;
    return q->q[q->tail % q->max];
}

