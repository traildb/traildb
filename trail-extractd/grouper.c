
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include <util.h>
#include <arena.h>

#include "trail-extractd.h"

#define GROUPS_INC 1000000

/*
A straightforward way to join trails by cookie would use a dictionary that
maps cookie IDs to a list of trails (hash join).

Maintaining a growing array for each cookie would be expensive time- and/or
space-wise. Instead, we maintain one global array of trails and have a mapping
from cookies to a linked list of slices (chunks) in that array.

The number of slices by cookie is bounded the number of mappers, so the
lists are kept short. Producing the final join is simply a matter of
traversing the linked list and concatenating the chunks.

In the groupby-mode, we maintain a separate cookie dictionary and an array of
trails for each group. All groups share a global arena of chunk objects.

 (Array)       Group
               |
 (JudyL)       Cookies
               |
 (Linked List) Nth Chunk -> N-1th Chunk -> N-2th Chunk ...
               |               |            |
 (Array)       [Events ....... Events ..... Events .... ]
*/

struct trail_chunk{
    /* index to the previous trail_chunk (linked list) */
    Word_t prev_chunk;
    /* offset in the events_buffer for this slice */
    Word_t events_index;
    /* number of values in this slice */
    uint32_t num_values;
} __attribute__((packed));

struct group{
    /* cookie index -> latest chunk mapping */
    Pvoid_t cookies;
    /* all events of this group */
    uint32_t *events_buffer;
    uint64_t events_buffer_len;
    uint64_t events_buffer_size;
};

/* 16-byte cookie -> a cookie index (uint64_t) mapping */
static Pvoid_t cookie_index;
static uint64_t num_cookies;

/* groups */
static struct group *groups;
static uint32_t num_groups;

/* chunks */
static struct arena trail_chunks = {
    .item_size = sizeof(struct trail_chunk)
};

/* lookup a 16 byte cookie by doing two 8-byte lookups */
static inline Word_t lookup_cookie(const char *cookie)
{
    Word_t cookie_lo = *(Word_t*)cookie;
    Word_t cookie_hi = *(Word_t*)&cookie[8];
    Pvoid_t cookies_hi;
    Word_t *ptr1, *ptr2;

    JLI(ptr1, cookie_index, cookie_lo);
    cookies_hi = (Pvoid_t)*ptr1;
    JLI(ptr2, cookies_hi, cookie_hi);

    if (!*ptr2)
        *ptr2 = ++num_cookies;
    return *ptr2;
}

static uint32_t find_groupby_field(const struct extractd *ext,
                                   const char *groupby_str)
{
    uint32_t i, num_fields = extractd_get_num_fields(ext);

    for (i = 0; i < num_fields; i++){
        const char *field_name = extractd_get_field_name(ext, i);
        if (!strcmp(field_name, groupby_str))
            return i;
    }
    DIE("Mappers are not sending the groupby field '%s'\n", groupby_str);
}

/*
There are two optimizations related to the groupby operation in add_chunk().

Imagine a trail like

A A A B B A A B

where A and B are different groupby keys. A straightforward solution would
loop through the trail and add each event separately to the corresponding
group. This would involve a scary number of lookups and it would create a
fat chunk entry for each event.

The first optimization is to add consecutive events with the same key in one
operation, i.e.

add_chunk A A A -> Chunk_A1
add_chunk B B   -> Chunk_B1
add_chunk A A   -> Chunk_A2
add_chunk B     -> Chunk_B2

This would create only four trail_chunk entries, one for each call, instead
of one for each event.

The second optimization gets rid of redundant chunks. If two chunks would
point to consecutive arrays of events, we can merge them into one:

add_chunk A A A -> Chunk_A1
add_chunk B B   -> Chunk_B1
add_chunk A A   -> Chunk_A1
add_chunk B     -> Chunk_B1
*/
static inline void add_chunk(uint32_t groupby_key,
                             uint32_t groupby_field,
                             Word_t cookie_idx,
                             const uint32_t *events,
                             uint32_t num_events,
                             uint32_t num_fields)
{
    struct trail_chunk *chunk = NULL;
    struct group *group;
    Word_t *ptr;
    uint64_t offset;
    uint32_t *ev;
    uint32_t i, j, k;

    /* we will drop the groupby field from each event, hence num_fields -= 1 */
    if (groupby_field)
        --num_fields;

    if (groupby_key >= num_groups){
        uint64_t offs = num_groups;
        num_groups = groupby_key + GROUPS_INC;
        if (!(groups = realloc(groups, sizeof(struct group) * num_groups)))
            DIE("Could not allocate %u groups\n", num_groups);
        memset(&groups[offs], 0, (num_groups - offs) * sizeof(struct group));
    }
    group = &groups[groupby_key];
    JLI(ptr, group->cookies, cookie_idx);

    /* Optimization 2: Merge consecutive chunks into one (mainly groupby) */
    if (*ptr){
        chunk = &((struct trail_chunk*)trail_chunks.data)[*ptr - 1];
        if (chunk->events_index + chunk->num_values != group->events_buffer_len)
            /* chunks do not point to consecutive sequences of events,
               create a new chunk */
            chunk = NULL;
    }

    if (chunk){
        /* extend existing chunk */
        chunk->num_values += num_events * num_fields;
    }else{
        /* create a new chunk */
        chunk = (struct trail_chunk*)arena_add_item(&trail_chunks);
        chunk->prev_chunk = *ptr;
        chunk->num_values = num_events * num_fields;
        chunk->events_index = group->events_buffer_len;
        *ptr = trail_chunks.next;
    }

    /* make sure our events_buffer is large enough */
    offset = group->events_buffer_len;
    group->events_buffer_len += num_events * num_fields;
    if (group->events_buffer_len >= group->events_buffer_size){
        if (!group->events_buffer_size)
            group->events_buffer_size = 2;
        while (group->events_buffer_size < group->events_buffer_len)
            group->events_buffer_size *= 2;
        if (!(group->events_buffer = realloc(group->events_buffer,
                                             group->events_buffer_size * 4)))
            DIE("Could not grow events_buffer to %llu bytes\n",
                (unsigned long long)group->events_buffer_size * 4);
    }

    ev = &group->events_buffer[offset];

    if (groupby_field){
        /* drop the redundant groupby field from results */
        for (k = 0, i = 0; i < num_events; i++){
            const uint32_t *event = &events[i * (num_fields + 1)];
            for (j = 0; j < groupby_field; j++)
                ev[k++] = event[j];
            for (j = groupby_field + 1; j < num_fields + 1; j++)
                ev[k++] = event[j];
        }
    }else
        memcpy(ev, events, num_events * num_fields * 4);
}

void grouper_process(struct extractd_ctx *ctx)
{
    const char *cookie;
    const uint32_t *events;
    uint32_t i, groupby_field, num_fields, num_events;
    int ret;

    ret = extractd_next_trail(ctx->extd,
                              &cookie,
                              &events,
                              &num_events,
                              &num_fields,
                              MAPPER_TIMEOUT);
    if (ret == -1)
        DIE("Timeout");
    else if (ret == 0)
        return;

    if (ctx->groupby_str){
        /* GROUPBY MODE */
        groupby_field = find_groupby_field(ctx->extd, ctx->groupby_str) + 1;

        while (ret){
            Word_t cookie_idx = lookup_cookie(cookie);

            for (i = 0; i < num_events;){
                const uint32_t *event = &events[i * (num_fields + 1)];
                uint32_t groupby_key = event[groupby_field];
                uint32_t first = i;
                uint32_t n = 1;

                /* Optimization 1 (see above):
                   Add all consecutive events with the same key in one operation. */
                while (++i < num_events){
                    event = &events[i * (num_fields + 1)];
                    if (event[groupby_field] == groupby_key)
                        ++n;
                    else
                        break;
                }

                add_chunk(groupby_key,
                          groupby_field,
                          cookie_idx,
                          &events[first],
                          n,
                          num_fields + 1);
            }

            ret = extractd_next_trail(ctx->extd,
                                      &cookie,
                                      &events,
                                      &num_events,
                                      &num_fields,
                                      MAPPER_TIMEOUT);
            if (ret == -1)
                DIE("Timeout");
        }
    }else{
        /* NORMAL MODE */
        while (ret){
            Word_t cookie_idx = lookup_cookie(cookie);
            add_chunk(0, 0, cookie_idx, events, num_events, num_fields + 1);
            ret = extractd_next_trail(ctx->extd,
                                      &cookie,
                                      &events,
                                      &num_events,
                                      &num_fields,
                                      MAPPER_TIMEOUT);
            if (ret == -1)
                DIE("Timeout");
        }
    }
}

void grouper_output(struct extractd_ctx *ctx)
{
    /*
        loop through groups
            loop through cookies
                traverse events
                sort
                output
    */
}

