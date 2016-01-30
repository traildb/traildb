#include "tdb_internal.h"
#include "tdb_huffman.h"

static inline uint64_t tdb_get_trail_offs(const tdb *db, uint64_t trail_id)
{
    if (db->trails.size < UINT32_MAX)
        return ((const uint32_t*)db->toc.data)[trail_id];
    else
        return ((const uint64_t*)db->toc.data)[trail_id];
}

static int event_satisfies_filter(const tdb_item *event,
                                  const tdb_item *filter,
                                  uint64_t filter_len)
{
    uint64_t i = 0;
    while (i < filter_len){
        uint64_t clause_len = filter[i++];
        uint64_t next_clause = i + clause_len;
        int match = 0;
        if (next_clause > filter_len)
            return 0;

        while (i < next_clause){
            uint64_t is_negative = filter[i++];
            uint64_t filter_item = filter[i++];
            tdb_field field = tdb_item_field(filter_item);
            if (field){
                if ((event[field] == filter_item) != is_negative){
                    match = 1;
                    break;
                }
            } else {
                if (is_negative) {
                    match = 1;
                    break;
                }
            }
        }
        if (!match){
            return 0;
        }
        i = next_clause;
    }
    return 1;
}

#if 0
int tdb_get_trail(const tdb *db,
                  uint64_t trail_id,
                  tdb_item **items,
                  uint64_t *items_buf_len,
                  uint64_t *num_items,
                  int edge_encoded)
{
    return tdb_get_trail_filtered(db,
                                  trail_id,
                                  items,
                                  items_buf_len,
                                  num_items,
                                  edge_encoded,
                                  db->filter,
                                  db->filter_len);
}

int tdb_get_trail_filtered(const tdb *db,
                           uint64_t trail_id,
                           tdb_item **items,
                           uint64_t *items_buf_len,
                           uint64_t *num_items,
                           int edge_encoded,
                           const tdb_item *filter,
                           uint64_t filter_len)
{
    static const uint64_t INITIAL_ITEMS_BUF_LEN = 1U << 16;
    int r;

    if (!*items_buf_len){
        if (!(*items = malloc(INITIAL_ITEMS_BUF_LEN * sizeof(tdb_item))))
            return TDB_ERR_NOMEM;
        *items_buf_len = INITIAL_ITEMS_BUF_LEN;
    }
    while (1){
        if ((r = tdb_decode_trail_filtered(db,
                                           trail_id,
                                           *items,
                                           *items_buf_len,
                                           num_items,
                                           edge_encoded,
                                           filter,
                                           filter_len)))
            return r;

        if (*num_items < *items_buf_len)
            return 0;
        else{
            *items_buf_len *= 2;
            free(*items);
            if (!(*items = malloc(*items_buf_len * sizeof(tdb_item)))){
                *items_buf_len = 0;
                return TDB_ERR_NOMEM;
            }
        }
    }
}

int tdb_decode_trail(const tdb *db,
                     uint64_t trail_id,
                     tdb_item *dst,
                     uint64_t dst_size,
                     uint64_t *num_items,
                     int edge_encoded)
{
    return tdb_decode_trail_filtered(db,
                                     trail_id,
                                     dst,
                                     dst_size,
                                     num_items,
                                     edge_encoded,
                                     db->filter,
                                     db->filter_len);
}

int tdb_decode_trail_filtered(const tdb *db,
                              uint64_t trail_id,
                              tdb_item *dst,
                              uint64_t dst_size,
                              uint64_t *num_items,
                              int edge_encoded,
                              const tdb_item *filter,
                              uint64_t filter_len)
#endif

tdb_cursor *tdb_cursor_new(const tdb *db)
{
    tdb_cursor *c = NULL;

    if (!(c = calloc(1, sizeof(tdb_cursor))))
        goto err;

    if (!(c->state = calloc(1, sizeof(struct tdb_decode_state) +
                               db->num_fields * sizeof(tdb_item))))
        goto err;

    c->state->db = db;
    c->state->edge_encoded = db->opt_edge_encoded;
    c->state->events_buffer_len = db->opt_cursor_event_buffer_size;

    if (!(c->state->events_buffer = calloc(c->state->events_buffer_len,
                                           (db->num_fields + 1) *
                                           sizeof(tdb_item))))
        goto err;

    return c;
err:
    tdb_cursor_free(c);
    return NULL;
}

void tdb_cursor_free(tdb_cursor *c)
{
    if (c){
        free(c->state->events_buffer);
        free(c->state);
        free(c);
    }
}

tdb_error tdb_get_trail(tdb_cursor *cursor, uint64_t trail_id)
{
    struct tdb_decode_state *s = cursor->state;
    const tdb *db = s->db;

    if (trail_id < db->num_trails){
        /* initialize cursor for a new trail */

        uint64_t trail_size;
        tdb_field field;

        /*
        edge encoding: some fields may be inherited from previous events.
        Keep track what we have seen in the past. Start with NULL values.
        */
        for (field = 1; field < db->num_fields; field++)
            s->previous_items[field] = tdb_make_item(field, 0);

        s->data = &db->trails.data[tdb_get_trail_offs(db, trail_id)];
        trail_size = tdb_get_trail_offs(db, trail_id + 1) -
                     tdb_get_trail_offs(db, trail_id);
        s->size = 8 * trail_size - read_bits(s->data, 0, 3);
        s->offset = 3;
        s->tstamp = db->min_timestamp;

        s->trail_id = trail_id;
        cursor->num_events_left = 0;
        cursor->next_event = s->events_buffer;

        return 0;
    }else
        return TDB_ERR_INVALID_TRAIL_ID;
}

uint64_t tdb_get_trail_length(tdb_cursor *cursor)
{
    uint64_t count = 0;
    while (_tdb_cursor_next_batch(cursor))
        count += cursor->num_events_left;
    return count;
}

int _tdb_cursor_next_batch(tdb_cursor *cursor)
{
    struct tdb_decode_state *s = cursor->state;
    const struct huff_codebook *codebook =
        (const struct huff_codebook*)s->db->codebook.data;
    const struct field_stats *fstats = s->db->field_stats;
    uint64_t *dst = (uint64_t*)s->events_buffer;
    uint64_t i = 0;
    uint64_t num_events = 0;
    tdb_field field;
    tdb_item item;
    const int edge_encoded = s->edge_encoded;

    /* decode the trail - exit early if destination buffer runs out of space */
    while (s->offset < s->size && num_events < s->events_buffer_len){
        /* Every event starts with a timestamp.
           Timestamp may be the first member of a bigram */
        __uint128_t gram = huff_decode_value(codebook,
                                             s->data,
                                             &s->offset,
                                             fstats);
        uint64_t orig_i = i;
        uint64_t delta = tdb_item_val(HUFF_BIGRAM_TO_ITEM(gram));
        uint64_t *num_items;

        /*
        events buffer format:

           [ [ timestamp | num_items | items ... ] tdb_event 1
             [ timestamp | num_items | items ... ] tdb_event 2
             ...
             [ timestamp | num_items | items ... ] tdb_event N ]

        note that events may have a varying number of items, due to
        edge encoding
        */

        s->tstamp += delta;
        dst[i++] = s->tstamp;
        num_items = &dst[i++];

        item = HUFF_BIGRAM_OTHER_ITEM(gram);

        /* handle a possible latter part of the first bigram */
        if (item){
            field = tdb_item_field(item);
            s->previous_items[field] = item;
            if (edge_encoded)
                dst[i++] = item;
        }

        /* decode one event: timestamp is followed by at most num_ofields
           field values */
        while (s->offset < s->size){
            uint64_t prev_offs = s->offset;
            gram = huff_decode_value(codebook,
                                     s->data,
                                     &s->offset,
                                     fstats);
            item = HUFF_BIGRAM_TO_ITEM(gram);
            field = tdb_item_field(item);
            if (field){
                /* value may be either a unigram or a bigram */
                do{
                    s->previous_items[field] = item;
                    if (edge_encoded)
                        dst[i++] = item;
                    gram = item = HUFF_BIGRAM_OTHER_ITEM(gram);
                }while ((field = tdb_item_field(item)));
            }else{
                /* we hit the next timestamp, take a step back and break */
                s->offset = prev_offs;
                break;
            }
        }

        if (!s->filter || event_satisfies_filter(s->previous_items,
                                                 s->filter,
                                                 s->filter_len)){

            /* no filter or filter matches, finalize the event */
            if (!edge_encoded || s->first_satisfying){
                /* dump all the fields of this event in the result, if edge
                   encoding is not requested or this is the first event
                   that satisfies the filter */

                /* FIXME should we add this i = orig_i + 2;
                   otherwise we will get duplicate items in the dst.
                   TODO create a test for this
                */
                for (field = 1; field < s->db->num_fields; field++)
                    dst[i++] = s->previous_items[field];

                /*
                consider a sequence of events like

                (A, X), (A, Y), (B, X), (B, Y), (B, Y)

                and a CNF filter "B & Y". Without 'first_satisfying'
                special case, the query would return

                Y instead of (B, Y)

                when edge_encoded=1
                */
                s->first_satisfying = 0;
            }
            ++num_events;
            *num_items = (i - (orig_i + 2));
        }else{
            /* filter doesn't match - ignore this event */
            i = orig_i;
            /* FIXME should we add to cover all edges that may
            get filtered out */
            // s->first_satisfying = 1;
        }
    }

    cursor->next_event = s->events_buffer;
    cursor->num_events_left = num_events;
    return num_events > 0 ? 1: 0;
}

/*
the following ensures that tdb_cursor_next() is exported to
libtraildb.so

this is "strategy 3" from
http://www.greenend.org.uk/rjk/tech/inline.html
*/
extern const tdb_event *tdb_cursor_next(tdb_cursor *cursor);

