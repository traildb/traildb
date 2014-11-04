
#include "tdb_internal.h"
#include "huffman.h"
#include "util.h"

uint32_t tdb_decode_trail(tdb *db,
                          uint64_t cookie_id,
                          uint32_t *dst,
                          uint32_t dst_size,
                          int edge_encoded)
{
    const uint32_t *toc;
    const char *data;
    uint64_t item, size, offs = 0;
    const struct huff_codebook *codebook =
        (const struct huff_codebook*)db->codebook.data;
    uint32_t k, j, i = 0;
    uint32_t tstamp = db->min_timestamp;
    uint64_t delta, prev_offs;
    const struct field_stats *fstats = db->field_stats;
    tdb_field field;

    if (cookie_id >= db->num_cookies)
        return 0;

    toc = (const uint32_t*)db->trails.data;
    data = &db->trails.data[toc[cookie_id]];
    size = 8 * (toc[cookie_id + 1] - toc[cookie_id]);
    size -= read_bits(data, 0, 3);
    offs = 3;
    if (edge_encoded){
        while (offs < size && i < dst_size){
            /* Every event starts with a timestamp.
               Timestamp may be the first member of a bigram */
            item = huff_decode_value(codebook, data, &offs, fstats);
            delta = (item & UINT32_MAX) >> 8;
            if (delta == TDB_FAR_TIMEDELTA){
                dst[i++] = 0;
            }else{
                tstamp += delta;
                dst[i++] = tstamp;
            }
            item >>= 32;
            if (item && i < dst_size)
                dst[i++] = item;

            /* timestamp is followed by at most num_ofields field values */
            while (offs < size && i < dst_size){
                prev_offs = offs;
                item = huff_decode_value(codebook, data, &offs, fstats);
                field = tdb_item_field(item);
                if (field){
                    /* value may be either a unigram or a bigram */
                    do{
                        dst[i++] = item & UINT32_MAX;
                        item >>= 32;
                    }while (item && i < dst_size);
                }else{
                    /* we hit the next timestamp, take a step back and break */
                    offs = prev_offs;
                    break;
                }
            }
            /* we mark the end of an event with zero. Zero can't occur in any
               of the field values since the timestamp field is 0 */
            if (i < dst_size)
                dst[i++] = 0;
        }
    }else{
        /* same thing as above but here we decode edge encoding */
        for (k = 1; k < db->num_fields; k++)
            db->previous_items[k] = k;

        while (offs < size && i < dst_size){
            item = huff_decode_value(codebook, data, &offs, fstats);
            delta = (item & UINT32_MAX) >> 8;
            if (delta == TDB_FAR_TIMEDELTA){
                dst[i++] = 0;
            }else{
                tstamp += delta;
                dst[i++] = tstamp;
            }
            item >>= 32;
            field = tdb_item_field(item);

            if (field)
                db->previous_items[field] = item;

            /* edge encoding: some fields may be inherited from previous
               events - keep track what we have seen in the past */
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

            for (j = 1; j < db->num_fields && i < dst_size; j++)
                dst[i++] = db->previous_items[j];

            if (i < dst_size)
                dst[i++] = 0;
        }
    }

    return i;
}

void *tdb_fold(tdb *db, tdb_fold_fn fun, void *acc) {
    const uint32_t *toc;
    const char *data;
    const struct huff_codebook *codebook =
        (const struct huff_codebook*)db->codebook.data;
    const struct field_stats *fstats = db->field_stats;
    tdb_field field;
    uint32_t k, tstamp = db->min_timestamp;
    uint64_t cookie_id, delta, prev_offs, item, size, offs = 0;

    for (cookie_id = 0; cookie_id < db->num_cookies; cookie_id++){
        toc = (const uint32_t*)db->trails.data;
        data = &db->trails.data[toc[cookie_id]];
        size = 8 * (toc[cookie_id + 1] - toc[cookie_id]);
        size -= read_bits(data, 0, 3);
        offs = 3;

        for (k = 1; k < db->num_fields; k++)
            db->previous_items[k] = k;

        while (offs < size){
            item = huff_decode_value(codebook, data, &offs, fstats);
            delta = (item & UINT32_MAX) >> 8;
            if (delta == TDB_FAR_TIMEDELTA){
                db->previous_items[0] = 0;
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