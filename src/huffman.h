
#ifndef __HUFFMAN_H__
#define __HUFFMAN_H__

#include <stdint.h>

#include <Judy.h>

#define HUFF_CODEBOOK_SIZE 65536
#define HUFF_CODE(x) ((uint32_t)((x) & 65535LU))
#define HUFF_BITS(x) ((uint32_t)(((x) & (65535LU << 16LU)) >> 16LU))

struct huff_codebook{
    uint64_t symbol;
    uint32_t bits;
} __attribute__((packed));

struct field_stats{
    uint32_t field_id_bits;
    uint32_t field_bits[0];
};

static inline uint32_t read_bits(const char *src, uint64_t offs, uint32_t bits)
{
    const uint64_t *src_w = (const uint64_t*)&src[offs >> 3];
    return (uint32_t)((*src_w >> (offs & 7)) & ((((uint64_t)1) << bits) - 1));
}

static inline void write_bits(char *dst, uint64_t offs, uint32_t val)
{
    uint64_t *dst_w = (uint64_t*)&dst[offs >> 3];
    *dst_w |= ((uint64_t)val) << (offs & 7);
}

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
