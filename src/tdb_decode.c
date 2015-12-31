#include "tdb_internal.h"
#include "tdb_huffman.h"
#include "util.h"

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
        if (!(*items = malloc(INITIAL_ITEMS_BUF_LEN * 4)))
            return -1;
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
            if (!(*items = malloc(*items_buf_len * 4))){
                *items_buf_len = 0;
                return -1;
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
{
    const char *data;
    const struct huff_codebook *codebook = (struct huff_codebook*)db->codebook.data;
    const struct field_stats *fstats = db->field_stats;
    uint64_t orig_i, i = 0;
    uint64_t tstamp = db->min_timestamp;
    uint64_t delta, prev_offs, offs, trail_size, size;
    int first_satisfying = 1;
    tdb_field field;
    tdb_item item;

    if (trail_id >= db->num_trails)
        /* TODO proper error code */
        return -1;

    data = &db->trails.data[tdb_get_trail_offs(db, trail_id)];
    trail_size = tdb_get_trail_offs(db, trail_id + 1) -
                 tdb_get_trail_offs(db, trail_id);
    size = 8 * trail_size - read_bits(data, 0, 3);
    offs = 3;

    /* edge encoding: some fields may be inherited from previous events. Keep
       track what we have seen in the past. Start with NULL values. */
    for (field = 1; field < db->num_fields; field++)
        db->previous_items[field] = tdb_make_item(field, 0);

    /* decode the trail - exit early if destination buffer runs out of space */
    while (offs < size && i < dst_size){
        /* Every event starts with a timestamp.
           Timestamp may be the first member of a bigram */
        orig_i = i;
        __uint128_t gram = huff_decode_value(codebook, data, &offs, fstats);
        delta = tdb_item_val(HUFF_BIGRAM_TO_ITEM(gram));
        tstamp += delta;
        dst[i++] = tstamp;
        item = HUFF_BIGRAM_OTHER_ITEM(gram);

        /* handle a possible latter part of the first bigram */
        if (item){
            field = tdb_item_field(item);
            db->previous_items[field] = item;
            if (edge_encoded && i < dst_size)
                dst[i++] = item;
        }

        /* decode one event: timestamp is followed by at most num_ofields
           field values */
        while (offs < size){
            prev_offs = offs;
            gram = huff_decode_value(codebook, data, &offs, fstats);
            item = HUFF_BIGRAM_TO_ITEM(gram);
            field = tdb_item_field(item);
            if (field){
                /* value may be either a unigram or a bigram */
                do{
                    db->previous_items[field] = item;
                    if (edge_encoded && i < dst_size)
                        dst[i++] = item;
                    item = HUFF_BIGRAM_OTHER_ITEM(gram);
                }while ((field = tdb_item_field(item)));
            }else{
                /* we hit the next timestamp, take a step back and break */
                offs = prev_offs;
                break;
            }
        }

        if (!filter ||
            event_satisfies_filter(db->previous_items, filter, filter_len)){
            /* no filter or filter matches, finalize the event */
            if (!edge_encoded || first_satisfying){
                /* dump all the fields of this event in the result, if edge
                   encoding is not requested or this is the first event
                   that satisfies the filter */
                for (field = 1; field < db->num_fields && i < dst_size; field++)
                    dst[i++] = db->previous_items[field];

                /*
                consider a sequence of events like

                (A, X), (A, Y), (B, X), (B, Y), (B, Y)

                and a CNF filter "B & Y". Without 'first_satisfying'
                special case, the query would return

                Y instead of (B, Y)

                when edge_encoded=1
                */
                first_satisfying = 0;
            }
            /* end the event with a zero */
            if (i < dst_size)
                dst[i++] = 0;
        }else
            /* filter doesn't match - ignore this event */
            i = orig_i;
    }

    *num_items = i;
    return 0;
}
