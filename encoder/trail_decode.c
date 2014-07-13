
#include <stdint.h>

#include "ddb_bits.h"
#include "huffman.h"
#include "ddb_profile.h"

#include "breadcrumbs_decoder.h"

uint32_t bd_trail_decode(struct breadcrumbs *bd,
                         uint32_t trail_index,
                         uint32_t *dst,
                         uint32_t dst_size,
                         int raw_values)
{
    const uint32_t *toc;
    const char *data;
    uint64_t size, offs = 0;
    const struct huff_codebook *codebook =
        (const struct huff_codebook*)bd->codebook.data;
    uint32_t j, i = 0;
    uint32_t tstamp = bd->min_timestamp;

    if (trail_index >= bd->num_cookies)
        return 0;

    toc = (const uint32_t*)bd->trails.data;
    data = &bd->trails.data[toc[trail_index]];
    size = 8 * (toc[trail_index + 1] - toc[trail_index]);
    size -= read_bits(data, 0, 3);
    offs = 3;

    if (raw_values){
        while (offs < size && i < dst_size){
            /* every logline starts with a timestamp */
            uint32_t val = huff_decode_value(codebook, data, &offs);
            tstamp += val >> 8;
            dst[i++] = tstamp;

            /* ..followed by at most num_fields field values, some of which
               may be inherited from the previous events (edge encoding) */
            while (offs < size && i < dst_size){
                uint64_t prev_offs = offs;
                val = huff_decode_value(codebook, data, &offs);
                if (val & 255)
                    dst[i++] = val;
                else{
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

        memset(bd->previous_values, 0, bd->num_fields * 4);

        while (offs < size && i < dst_size){
            uint32_t val = huff_decode_value(codebook, data, &offs);
            tstamp += val >> 8;
            dst[i++] = tstamp;

            while (offs < size){
                uint64_t prev_offs = offs;
                val = huff_decode_value(codebook, data, &offs);
                if (val & 255)
                    bd->previous_values[(val & 255) - 1] = val;
                else{
                    offs = prev_offs;
                    break;
                }
            }
            for (j = 0; j < bd->num_fields && i < dst_size; j++)
                dst[i++] = bd->previous_values[j];

            if (i < dst_size)
                dst[i++] = 0;
        }
    }
    return i;
}

#if 0
void bd_trail_all_freqs(struct breadcrumbs *bd)
{
    const uint32_t *toc = (const uint32_t*)bd->trails.data;
    uint64_t i;
    Pvoid_t freqs = NULL;
    const struct huff_codebook *codebook =
        (const struct huff_codebook*)bd->codebook.data;
    DDB_TIMER_DEF
    Word_t *ptr;
    Word_t idx;

    DDB_TIMER_START
    for (i = 0; i < bd->num_cookies; i++){
        const char *data = &bd->trails.data[toc[i]];
        uint64_t size = 8 * (toc[i + 1] - toc[i]);
        uint64_t offs = 3;

        size -= read_bits(data, 0, 3);

        while (offs < size){
            uint32_t val = huff_decode_value(codebook, data, &offs);

            JLI(ptr, freqs, val);
            ++*ptr;
        }
    }
    DDB_TIMER_END("decoder/trail_all_freqs");
    /*
    idx = 0;
    JLF(ptr, freqs, idx);
    while (ptr){
        printf("%llu %llu\n", idx, *ptr);
        JLN(ptr, freqs, idx);
    }
    */
}
#endif

uint32_t bd_trail_value_freqs(const struct breadcrumbs *bd,
                              uint32_t *trail_indices,
                              uint32_t num_trail_indices,
                              uint32_t *dst_values,
                              uint32_t *dst_freqs,
                              uint32_t dst_size)
{
    /* Use Judy1 to check that only one distinct value per cookie is added to freqs */
    /* no nulls, no timestamps */
    /* return number of top values added to dst (<= dst_size) */
    return 0;
}

#if 0
static void find_bigrams(struct breadcrumbs *bd, Pvoid_t freqs)
{
    const uint32_t *toc = (const uint32_t*)bd->trails.data;
    uint64_t i;
    Pvoid_t bi_freqs = NULL;
    const struct huff_codebook *codebook =
        (const struct huff_codebook*)bd->codebook.data;
    uint32_t *values;

    if (!(values = malloc(bd->num_fields)))
        DIE("Num fields malloc failed\n");

    for (i = 0; i < bd->num_cookies; i++){
        const char *data = &bd->trails.data[toc[i]];
        uint64_t size = 8 * (toc[i + 1] - toc[i]);
        uint64_t offs = 3;
        uint32_t val;

        size -= read_bits(data, 0, 3);

        while (offs < size){
            int k, j = 0;
            while (offs < size){
                uint64_t prev_offs = offs;
                val = huff_decode_value(codebook, data, &offs);
                if (j == 0 || val & 255)
                    values[j++] = val;
                else{
                    offs = prev_offs;
                    break;
                }
            }
            for (k = 0; k < j; k++){
                int h, tmp;
                J1T(tmp, top_vals, values[k]);
                if (tmp){
                    for (h = k + 1; h < j; h++){
                        J1T(tmp, top_vals, values[h]);
                        if (tmp){
                            Word_t *ptr;
                            // bigram = values[k] | values[h];
                            JLI(ptr, bigram_freqs, bigram);
                            ++*ptr;
                        }
                    }
                }
            }
        }
    }

    /* loop over bigram_freqs */
    /* substract first item from freqs unigram(A) - bigram(AB) */
    /* add bigram(AB) freq to freqs */

    free(values);
}
#endif
