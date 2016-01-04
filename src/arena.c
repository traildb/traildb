
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#include "arena.h"
#include "tdb_io.h"

int arena_flush(const struct arena *a)
{
    int ret = 0;
    if (a->fd && a->next){
        uint64_t size = (((a->next - 1) & (ARENA_DISK_BUFFER - 1)) + 1) *
                        (uint64_t)a->item_size;
        TDB_WRITE(a->fd, a->data, size);
    }
done:
    return ret;
}

void *arena_add_item(struct arena *a)
{
    if (a->fd){
        if (a->size == 0){
            a->size = ARENA_DISK_BUFFER;
            if (!(a->data = malloc(a->item_size * (uint64_t)a->size))){
                a->size = 0;
                return NULL;
            }
        }else if ((a->next & (ARENA_DISK_BUFFER - 1)) == 0){
            if (arena_flush(a))
                return NULL;
        }
        return a->data + a->item_size * (a->next++ & (ARENA_DISK_BUFFER - 1));
    }else{
        if (a->next >= a->size){
            a->size += a->arena_increment ? a->arena_increment: ARENA_INCREMENT;
            if (!(a->data = realloc(a->data, a->item_size * (uint64_t)a->size))){
                a->size = 0;
                return NULL;
            }
        }
        return a->data + a->item_size * a->next++;
    }
}

