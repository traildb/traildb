
#include <stdint.h>
#include <stdlib.h>
#include <string.h>

#include "tdb_internal.h"
#include "merge.h"
#include "arena.h"
#include "util.h"

#define GROUPS_INC 1000000
#define MERGE_BUF_INC 1000000

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
static inline Word_t lookup_cookie(tdb_cookie cookie)
{
    Word_t cookie_lo = *(Word_t*)cookie;
    Word_t cookie_hi = *(Word_t*)&cookie[8];
    Pvoid_t cookies_hi;
    Word_t *ptr1, *ptr2;

    JLI(ptr1, cookie_index, cookie_lo);
    cookies_hi = (Pvoid_t)*ptr1;
    JLI(ptr2, cookies_hi, cookie_hi);
    *ptr1 = (Word_t)cookies_hi;

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
    tdb_cookie cookie;
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
                const uint32_t *event;
                const uint32_t *event0 = event = &events[i * (num_fields + 1)];
                uint32_t groupby_key = event[groupby_field];
                uint32_t n = 1;

                /*
                if (event[0] < 1400000000)
                    DIE("Strange timestamp osid %u\n", event[0]);
                */

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
                          event0,
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

int event_cmp(const void *a, const void *b)
{
    uint32_t a_tstamp = *(uint32_t*)a;
    uint32_t b_tstamp = *(uint32_t*)b;

    if (a_tstamp < b_tstamp)
        return -1;
    else if (a_tstamp > b_tstamp)
        return 1;
    else
        return 0;
}

static void sort_events(uint32_t *merge_buf, uint32_t len, uint32_t num_fields)
{
    /* timsort would work great here, since each chunk is already sorted.
       This implementation https://github.com/swenson/sort would be almost
       perfect but it does not support dynamic item sizes. It also allocates
       auxiliary space with malloc(), which we would like to avoid here,
       since this function is called for all (group, cookie) pairs.

       Instead of a proper timsort, we implement a poor man's timsort here:
       We first check if the array is already sorted and only if it is not,
       we sort it using the standard qsort().
    */

    uint32_t i, prev_tstamp;

    /* check that timestamps are in the ascending order */
    for (prev_tstamp = 0, i = 0; i < len; i += num_fields){
        if (merge_buf[i] < prev_tstamp)
            break;
        prev_tstamp = merge_buf[i];
        /*
        if (prev_tstamp < 1400000000)
            DIE("Strange timestamp %u\n", prev_tstamp);
        */
    }
    if (i != len)
        /* timestamps are not increasing -> sort */
        qsort(merge_buf, len / num_fields, num_fields * 4, event_cmp);
}

static const uint32_t *merge_chunks(const struct group *group,
                                    uint64_t chunk_index,
                                    uint32_t num_fields,
                                    uint32_t *len)
{
    static uint32_t *merge_buf;
    static uint64_t merge_buf_size;

    const struct trail_chunk *chunk;
    const struct trail_chunk *chunks =
        ((const struct trail_chunk*)trail_chunks.data);

    *len = 0;

    /* concatenate all chunks into merge_buf */
    do{
        chunk = &chunks[chunk_index - 1];
        if (*len + chunk->num_values >= merge_buf_size){
            merge_buf_size = *len + chunk->num_values + MERGE_BUF_INC;
            if (!(merge_buf = realloc(merge_buf, merge_buf_size * 4)))
                DIE("Could not allocate merge buffer of %llu bytes\n",
                    (long long unsigned int)merge_buf_size);
        }

        memcpy(&merge_buf[*len],
               &group->events_buffer[chunk->events_index],
               chunk->num_values * 4);
        *len += chunk->num_values;

    }while ((chunk_index = chunk->prev_chunk));

    sort_events(merge_buf, *len, num_fields);

    return merge_buf;
}

static void output_group(struct extractd_ctx *ctx,
                         const char *fname,
                         const struct group *group)
{
    static char path[TDB_MAX_PATH_SIZE];
    FILE *out;
    Word_t *ptr;
    Word_t cookie_id = 0;
    uint32_t num_fields = extractd_get_num_fields(ctx->extd) + 1;
    uint32_t tmp;

    if (ctx->dir)
        tdb_path(path, "%s/%s", ctx->dir, fname);
    else
        tdb_path(path, "%s", fname);

    /* groupby mode excludes the groupby field from results */
    if (ctx->groupby_str)
        --num_fields;

    if (!(out = fopen(path, "w")))
        DIE("Could not open output file at %s\n", path);

    tmp = num_fields + 1;
    SAFE_WRITE(&tmp, 4, path, out);

    JLF(ptr, group->cookies, cookie_id);
    while (ptr){
        uint32_t i, len;
        const uint32_t *events = merge_chunks(group,
                                              *ptr,
                                              num_fields,
                                              &len);

        for (i = 0; i < len; i += num_fields){
            const uint32_t *event = &events[i];
            SAFE_WRITE(ptr, 4, path, out);
            SAFE_WRITE(event, num_fields * 4, path, out);
        }
        JLN(ptr, group->cookies, cookie_id);
    }
    fclose(out);
    printf("%s created\n", path);
}

void grouper_output(struct extractd_ctx *ctx)
{
    static char fname[TDB_MAX_PATH_SIZE];

    if (ctx->groupby_str){
        uint32_t i;
        uint32_t groupby_field = find_groupby_field(ctx->extd,
                                                    ctx->groupby_str);

        for (i = 0; i < num_groups; i++)
            if (groups[i].cookies){
                const char *field = extractd_get_token(ctx->extd,
                                                       groupby_field,
                                                       i);
                const char *safe = safe_filename(field);
                tdb_path(fname, "trail-extract.%u.%s", i, safe);
                output_group(ctx, fname, &groups[i]);
            }
    }else
        output_group(ctx, "trail-extract", &groups[0]);

}
