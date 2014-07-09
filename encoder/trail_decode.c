
#include <stdint.h>

#include "ddb_bits.h"
#include "huffman.h"

#include "breadcrumbs_decoder.h"

uint32_t bd_trail_decode(struct breadcrumbs *bd,
                         uint32_t trail_index,
                         uint32_t *dst,
                         uint32_t dst_size)
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
    printf("resi %u\n",read_bits(data, 0, 3));
    size -= read_bits(data, 0, 3);
    offs = 3;

    printf("OFFS %llu\n", toc[trail_index]);

    memset(bd->previous_values, 0, bd->num_fields * 4);

    while (offs < size && i < dst_size){
        uint32_t val = huff_decode_value(codebook, data, &offs);
        //printf("val %u %llu\n", val, offs);
        /* every event starts with a timestamp */
        tstamp += val >> 8;
        dst[i++] = tstamp;
        printf("offs %llu size %llu ", offs, size);
        printf("t %u:", tstamp);

        /* ..followed by at most num_fields field values, some of which
           may be inherited from the previous events (edge encoding) */
        while (offs < size){
            uint64_t prev_offs = offs;
            val = huff_decode_value(codebook, data, &offs);
            if (val & 255){
                printf(" %u|%u", val & 255, val >> 8);
                bd->previous_values[val & 255] = val;
            }else{
                offs = prev_offs;
                break;
            }
        }
        printf("\n");
        for (j = 0; j < bd->num_fields && i < dst_size; j++)
            if (bd->previous_values[j])
                dst[i++] = bd->previous_values[j];

        /* we mark the end of an event with zero. Zero can't occur in any
           of the field values since the timestamp field is 0 */
        if (i < dst_size)
            dst[i++] = 0;
    }
    return i;
}

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
