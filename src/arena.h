
#ifndef __ARENA_H__
#define __ARENA_H__

#include <stdint.h>
#include <stdio.h>

#ifndef ARENA_INCREMENT
#define ARENA_INCREMENT 1000000
#endif

#define ARENA_DISK_BUFFER (1 << 23) /* must be a power of two */

struct arena{
    char *data;
    uint64_t size;
    uint64_t next;
    uint64_t item_size;
    uint64_t arena_increment;
    int failed;
    FILE *fd;
};

int arena_flush(const struct arena *a);

void *arena_add_item(struct arena *a);

#endif /* __ARENA_H__ */
