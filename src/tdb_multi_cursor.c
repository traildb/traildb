
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>

#include "traildb.h"
#include "tdb_internal.h"

#include "pqueue/pqueue.h"

/*
Multi-cursor merges events from K cursors (trails) in a single
stream of timestamp ordered events on the fly. Merging is done with
a simple priority queue (heap), implemented by the pqueue library.

A key feature of multi-cursor is that it performs merging in a
zero-copy fashion by relying on the event buffers of underlying
cursors. A downside of this performance optimization is that the
buffer/event lifetime needs to be managed carefully.

If the buffer of an underlying cursor is exhausted temporarily, the
cursor can't be refreshed right away since this would invalidate
the past events. Instead, the cursor is marked as dirty in
popped_node, so it can be reinstered when multi-cursor is called
the next time.
*/

struct mcursor_node{
    pqueue_pri_t timestamp;
    size_t pos;
    tdb_cursor *cursor;
    uint64_t index;
};

struct tdb_multi_cursor{
    /* priority queue */
    pqueue_t *queue;
    struct mcursor_node *nodes;
    uint64_t num_nodes;

    /* node that needs to be reinserted on the next call */
    struct mcursor_node *popped_node;

    /* returned event buffer */
    tdb_multi_event current_event;
};

/* pqueue callback functions */

static int cmp_pri(pqueue_pri_t next, pqueue_pri_t cur)
{
	return next > cur;
}

static pqueue_pri_t get_pri(void *a)
{
	return ((struct mcursor_node*)a)->timestamp;
}

static void set_pri(void *a, pqueue_pri_t timestamp)
{
	((struct mcursor_node*)a)->timestamp = timestamp;
}

static size_t get_pos(void *a)
{
	return ((struct mcursor_node*)a)->pos;
}

static void set_pos(void *a, size_t pos)
{
    ((struct mcursor_node*)a)->pos = pos;
}

static void print_entry(FILE *out, void *a)
{
    struct mcursor_node *node = (struct mcursor_node*)a;
    fprintf(out,
            "node[%"PRIu64"] timestamp %"PRIu64"\n",
            node->index,
            (uint64_t)node->timestamp);
}

TDB_EXPORT tdb_multi_cursor *tdb_multi_cursor_new(tdb_cursor **cursors,
                                                  uint64_t num_cursors)
{
    tdb_multi_cursor *mc = NULL;
    uint64_t i;

    if (num_cursors > SIZE_MAX - 1)
        return NULL;

    if (!(mc = calloc(1, sizeof(struct tdb_multi_cursor))))
        goto err;

    if (!(mc->nodes = calloc(num_cursors, sizeof(struct mcursor_node))))
        goto err;

    if (!(mc->queue = pqueue_init(num_cursors,
                                  cmp_pri,
                                  get_pri,
                                  set_pri,
                                  get_pos,
                                  set_pos)))
        goto err;

    mc->num_nodes = num_cursors;

    for (i = 0; i < num_cursors; i++){
        mc->nodes[i].cursor = cursors[i];
        mc->nodes[i].index = i;
    }
    tdb_multi_cursor_reset(mc);

    return mc;
err:
    tdb_multi_cursor_free(mc);
    return NULL;
}

/*
Reinitialize the priority queue after the state of the underlying cursors
has changed, e.g. after tdb_get_trail().
*/
TDB_EXPORT void tdb_multi_cursor_reset(tdb_multi_cursor *mc)
{
    uint64_t i;

    pqueue_reset(mc->queue);
    for (i = 0; i < mc->num_nodes; i++){
        const tdb_event *event = tdb_cursor_peek(mc->nodes[i].cursor);
        if (event){
            mc->nodes[i].timestamp = event->timestamp;
            /*
            we can ignore the return value of pqueue_insert since it
            won't need to call realloc()
            */
            pqueue_insert(mc->queue, &mc->nodes[i]);
        }
    }
    mc->popped_node = NULL;
}

/*
Reinsert exhausted cursor in the heap
(see the top of this file for an explanation)
*/
static inline void reinsert_popped(tdb_multi_cursor *mc)
{
    if (mc->popped_node){
        const tdb_event *event = tdb_cursor_peek(mc->popped_node->cursor);
        if (event){
            mc->popped_node->timestamp = event->timestamp;
            pqueue_insert(mc->queue, mc->popped_node);
        }
        mc->popped_node = NULL;
    }
}

/* Peek the next event to be returned */
TDB_EXPORT const tdb_multi_event *tdb_multi_cursor_peek(tdb_multi_cursor *mc)
{
    const tdb_event *next_event;
    struct mcursor_node *node;

    reinsert_popped(mc);
    node = (struct mcursor_node*)pqueue_peek(mc->queue);

    if (!node)
        return NULL;

    mc->current_event.event = tdb_cursor_peek(node->cursor);
    mc->current_event.db = node->cursor->state->db;
    mc->current_event.cursor_idx = node->index;

    return &mc->current_event;
}

/* Return the next event */
TDB_EXPORT const tdb_multi_event *tdb_multi_cursor_next(tdb_multi_cursor *mc)
{
    struct mcursor_node *node;

    reinsert_popped(mc);
    node = (struct mcursor_node*)pqueue_peek(mc->queue);

    if (!node)
        return NULL;

    mc->current_event.event = tdb_cursor_next(node->cursor);
    mc->current_event.db = node->cursor->state->db;
    mc->current_event.cursor_idx = node->index;

    if (node->cursor->num_events_left){
        /*
        the event buffer of the cursor has remaining events,
        so we can just update the heap with the next timestamp
        */
        const tdb_event *next_event = (const tdb_event*)node->cursor->next_event;
        pqueue_change_priority(mc->queue, next_event->timestamp, node);
    }else{
        /*
        the event buffer of the cursor is empty. We don't know
        the next timestamp, so mark this cursor as dirty in popped_node
        (calling tdb_cursor_peek() would invalidate mc->current_event.event)
        */
        pqueue_pop(mc->queue);
        mc->popped_node = node;
    }

    return &mc->current_event;
}

/*
Return a batch of events. This is an optimized version of
tdb_multi_cursor_next()
*/
TDB_EXPORT uint64_t tdb_multi_cursor_next_batch(tdb_multi_cursor *mc,
                                                tdb_multi_event *events,
                                                uint64_t max_events)
{
    uint64_t n = 0;

    reinsert_popped(mc);

    /*
    next batch relies on the following heuristic:
    Often an individual cursor contains a long run of events whose
    timestamp is smaller than those of any other cursor, e.g. if there is
    a cursor for each daily traildb.

    Updating the heap for every single event is expensive and unnecessary
    in this case. It suffices to update the heap only when we switch cursors.
    This logic is implemented by popping the current cursor and peeking the
    next one: We can consume the current cursor as long as its timestamps are
    smaller than that of the next cursor.
    */
    while (n < max_events){
        struct mcursor_node *current =
            (struct mcursor_node*)pqueue_pop(mc->queue);
        struct mcursor_node *next =
            (struct mcursor_node*)pqueue_peek(mc->queue);
        tdb_cursor *cur;
        uint64_t next_timestamp = 0;
        int is_last = 0;

        if (current)
            cur = current->cursor;
        else
            /* heap is empty - all cursors exhausted */
            break;

        if (next)
            /*
            consume the current cursor while timestamps are smaller than
            next_timestamp
            */
            next_timestamp = next->timestamp;
        else
            /*
            no next timestamp - current is the last one. We can consume
            current until its end
            */
            is_last = 1;

        while (1){
            if (cur->num_events_left){
                /* there are events left in the buffer */
                const tdb_event *event = (const tdb_event*)cur->next_event;

                if (n < max_events &&
                    (is_last || event->timestamp <= next_timestamp)){

                    events[n].event = event;
                    events[n].db = cur->state->db;
                    events[n].cursor_idx = current->index;
                    ++n;
                    tdb_cursor_next(cur);
                }else{
                    /*
                    update the timestamp of the current cursor and
                    return it to the heap
                    */
                    current->timestamp = event->timestamp;
                    pqueue_insert(mc->queue, current);
                    break;
                }
            }else{
                /*
                no events left in the buffer, we must stop iterating
                to avoid the previous events from becoming invalid
                */
                mc->popped_node = current;
                goto done;
            }
        }
    }
done:
    return n;
}

TDB_EXPORT void tdb_multi_cursor_free(tdb_multi_cursor *mc)
{
    if (mc){
        if (mc->queue)
            pqueue_free(mc->queue);
        free(mc->nodes);
        free(mc);
    }
}
