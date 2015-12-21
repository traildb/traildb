#ifndef __TDB_QUEUE__
#define __TDB_QUEUE__

#include <stdint.h>

struct tdb_queue;

struct tdb_queue *tdb_queue_new(uint32_t max_length);
void tdb_queue_free(struct tdb_queue *q);
void tdb_queue_push(struct tdb_queue *q, void *e);
void *tdb_queue_pop(struct tdb_queue *q);
void *tdb_queue_peek(const struct tdb_queue *q);
int tdb_queue_length(const struct tdb_queue *q);

#endif /* __TDB_QUEUE__ */

