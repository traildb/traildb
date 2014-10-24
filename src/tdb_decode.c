
#include "ddb_bits.h"
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
    uint32_t j, i = 0;
    uint32_t tstamp = db->min_timestamp;
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
            tstamp += (item & UINT32_MAX) >> 8;
            dst[i++] = tstamp;
            item >>= 32;
            if (item && i < dst_size)
                dst[i++] = item;

            /* timestamp is followed by at most num_fields field values */
            while (offs < size && i < dst_size){
                uint64_t prev_offs = offs;
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
        memset(db->previous_items, 0, db->num_fields * 4);

        while (offs < size && i < dst_size){
            item = huff_decode_value(codebook, data, &offs, fstats);
            tstamp += (item & UINT32_MAX) >> 8;
            dst[i++] = tstamp;
            item >>= 32;
            field = tdb_item_field(item);

            if (item)
                db->previous_items[field - 1] = item;

            /* edge encoding: some fields may be inherited from previous
               events - keep track what we have seen in the past */
            while (offs < size){
                uint64_t prev_offs = offs;
                item = huff_decode_value(codebook, data, &offs, fstats);
                field = tdb_item_field(item);
                if (field){
                    do{
                        db->previous_items[field - 1] = item & UINT32_MAX;
                        item >>= 32;
                    }while ((field = tdb_item_field(item)));
                }else{
                    offs = prev_offs;
                    break;
                }
            }

            for (j = 0; j < db->num_fields && i < dst_size; j++)
                dst[i++] = db->previous_items[j];

            if (i < dst_size)
                dst[i++] = 0;
        }
    }

    return i;
}
