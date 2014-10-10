
#ifndef __HUFFMAN_H__
#define __HUFFMAN_H__

#include <stdint.h>

#include <Judy.h>

#include "ddb_bits.h"

#define HUFF_CODEBOOK_SIZE 65536
#define HUFF_CODE(x) ((x) & 65535)
#define HUFF_BITS(x) (((x) & (65535 << 16)) >> 16)

struct huff_codebook{
    uint64_t symbol;
    uint32_t bits;
} __attribute__((packed));

struct field_stats{
    uint32_t field_id_bits;
    uint32_t field_bits[0];
};

/* ENCODE */

Pvoid_t huff_create_codemap(const Pvoid_t key_freqs);

void huff_encode_grams(const Pvoid_t codemap,
                       const uint64_t *grams,
                       uint32_t num_grams,
                       char *buf,
                       uint64_t *offs,
                       const struct field_stats *fstats);

struct huff_codebook *huff_create_codebook(const Pvoid_t codemap,
                                           uint32_t *size);

void huff_store_codebook(const Pvoid_t codemap,
                         const char *path);

struct field_stats *huff_field_stats(const uint64_t *field_cardinalities,
                                     uint32_t num_fields,
                                     uint32_t max_timestamp);

/* DECODE */

static inline uint64_t huff_decode_value(const struct huff_codebook *codebook,
                                         const char *data,
                                         uint64_t *offset,
                                         const struct field_stats *fstats)
{
    uint64_t enc = read_bits(data, *offset, 33);
    if (enc & 1){
        uint16_t idx = HUFF_CODE(enc >> 1);
        *offset += codebook[idx].bits + 1;
        return codebook[idx].symbol;
    }else{
        /* read literal:
           [0 (1 bit) | field-id (field_id_bits) | value (field_bits[field_id])]
        */
        const uint64_t field_id = (enc >> 1) &
                                  ((1LLU << fstats->field_id_bits) - 1);
        const uint64_t field_val = (enc >> (fstats->field_id_bits + 1)) &
                                   ((1LLU << fstats->field_bits[field_id]) - 1);
        *offset += 1 + fstats->field_id_bits + fstats->field_bits[field_id];
        return field_id | (field_val << 8);
    }
}

#endif /* __HUFFMAN_H__ */
