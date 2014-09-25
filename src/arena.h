
#ifndef __ARENA_H__
#define __ARENA_H__

#include <stdint.h>
#include <stdio.h>

#define ARENA_INCREMENT 100000000
#define ARENA_DISK_BUFFER (1 << 23) /* must be a power of two */

struct arena{
    void *data;
    uint64_t size;
    uint64_t next;
    uint32_t item_size;
    uint32_t arena_increment;
    FILE *fd;
};

void arena_flush(const struct arena *a);

void *arena_add_item(struct arena *a);

#endif /* __ARENA_H__ */
