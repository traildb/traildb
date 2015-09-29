
#include "tdb_internal.h"
#include "huffman.h"
#include "util.h"

#define toc(cookie_id) tdb_get_cookie_offs(db, cookie_id)

static int event_satisfies_filter(const uint32_t *event,
                                  const uint32_t *filter,
                                  uint32_t filter_len)
{
    uint32_t i = 0;
    printf("f:");
    while (i < filter_len){
        uint32_t clause_len = filter[i++];
        uint32_t next_clause = i + clause_len;
        int match = 0;
        printf("\n%d %d\n", i, clause_len);
        if (next_clause > filter_len)
            return 0;

        while (i < clause_len){
            uint32_t is_negative = filter[i++];
            uint32_t filter_item = filter[i++];
            uint32_t field = tdb_item_field(filter_item);
            if (field){
                printf(" c[%d]", filter_item);
                if ((event[field] == filter_item) != is_negative){
                    match = 1;
                    break;
                }
            }
        }
        if (!match){
            printf(" NUH\n");
            return 0;
        }
        printf("next\n");
        i = next_clause;
    }
    return 1;
}

uint32_t tdb_decode_trail(const tdb *db,
                          uint64_t cookie_id,
                          uint32_t *dst,
                          uint32_t dst_size,
                          int edge_encoded)
{
    return tdb_decode_trail_filtered(db,
                                     cookie_id,
                                     dst,
                                     dst_size,
                                     edge_encoded,
                                     NULL,
                                     0);
}

uint32_t tdb_decode_trail_filtered(const tdb *db,
                                   uint64_t cookie_id,
                                   uint32_t *dst,
                                   uint32_t dst_size,
                                   int edge_encoded,
                                   const uint32_t *filter,
                                   uint32_t filter_len)
{
    const char *data;
    const struct huff_codebook *codebook = (struct huff_codebook*)db->codebook.data;
    const struct field_stats *fstats = db->field_stats;
    uint32_t k, j, orig_i, i = 0;
    uint32_t tstamp = db->min_timestamp;
    uint64_t delta, prev_offs, offs, size, item;
    tdb_field field;

    if (cookie_id >= db->num_cookies)
        return 0;

    if (filter == NULL && db->filter){
        filter = db->filter;
        filter_len = db->filter_len;
    }

    data = &db->trails.data[toc(cookie_id)];
    size = 8 * (toc(cookie_id + 1) - toc(cookie_id)) - read_bits(data, 0, 3);
    offs = 3;

    /* edge encoding: some fields may be inherited from previous events. Keep
       track what we have seen in the past */
    for (k = 1; k < db->num_fields; k++)
        db->previous_items[k] = k;

    /* decode the trail - exit early if destination buffer runs out of space */
    while (offs < size && i < dst_size){
        /* Every event starts with a timestamp.
           Timestamp may be the first member of a bigram */
        orig_i = i;
        item = huff_decode_value(codebook, data, &offs, fstats);
        delta = (item & UINT32_MAX) >> 8;
        if (delta == TDB_FAR_TIMEDELTA){
            dst[i++] = TDB_FAR_TIMESTAMP;
        }else{
            tstamp += delta;
            dst[i++] = tstamp;
        }
        item >>= 32;

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
            item = huff_decode_value(codebook, data, &offs, fstats);
            field = tdb_item_field(item);
            if (field){
                /* value may be either a unigram or a bigram */
                do{
                    db->previous_items[field] = item & UINT32_MAX;
                    if (edge_encoded && i < dst_size)
                        dst[i++] = item & UINT32_MAX;
                    item >>= 32;
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
            if (!edge_encoded){
                /* dump all the fields of this event in the result, if edge
                   encoding is not requested */
                for (j = 1; j < db->num_fields && i < dst_size; j++)
                    dst[i++] = db->previous_items[j];
            }
            /* end the event with a zero */
            if (i < dst_size)
                dst[i++] = 0;
        }else
            /* filter doesn't match - ignore this event */
            i = orig_i;
    }

    return i;
}

void *tdb_fold(const tdb *db, tdb_fold_fn fun, void *acc) {
    const char *data;
    const struct huff_codebook *codebook = (struct huff_codebook*)db->codebook.data;
    const struct field_stats *fstats = db->field_stats;
    tdb_field field;
    uint32_t k, tstamp;
    uint64_t cookie_id, delta, prev_offs, item, size, offs;

    for (cookie_id = 0; cookie_id < db->num_cookies; cookie_id++){
        tstamp = db->min_timestamp;
        data = &db->trails.data[toc(cookie_id)];
        size = 8 * (toc(cookie_id + 1) - toc(cookie_id)) - read_bits(data, 0, 3);
        offs = 3;

        for (k = 1; k < db->num_fields; k++)
            db->previous_items[k] = k;

        while (offs < size){
            item = huff_decode_value(codebook, data, &offs, fstats);
            delta = (item & UINT32_MAX) >> 8;
            if (delta == TDB_FAR_TIMEDELTA){
                db->previous_items[0] = TDB_FAR_TIMESTAMP;
            }else{
                tstamp += delta;
                db->previous_items[0] = tstamp;
            }
            item >>= 32;
            field = tdb_item_field(item);

            if (field)
                db->previous_items[field] = item;

            while (offs < size){
                prev_offs = offs;
                item = huff_decode_value(codebook, data, &offs, fstats);
                field = tdb_item_field(item);
                if (field){
                    do{
                        db->previous_items[field] = item & UINT32_MAX;
                        item >>= 32;
                    }while ((field = tdb_item_field(item)));
                }else{
                    offs = prev_offs;
                    break;
                }
            }

            acc = fun(db, cookie_id, db->previous_items, acc);
        }
    }
    return acc;
}
