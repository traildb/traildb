
#ifndef __HUFFMAN_H__
#define __HUFFMAN_H__

#include <stdint.h>

#include "judy_128_map.h"
#include "tdb_types.h"
#include "tdb_bits.h"

/* ensure TDB_CODEBOOK_SIZE < UINT32_MAX */
#define HUFF_CODEBOOK_SIZE 65536
#define HUFF_CODE(x) ((uint32_t)((x) & 65535LU))
#define HUFF_BITS(x) ((uint32_t)(((x) & (65535LU << 16LU)) >> 16LU))
#define HUFF_IS_BIGRAM(x) ((x >> 64) & UINT64_MAX)
#define HUFF_BIGRAM_TO_ITEM(x) ((tdb_item)(x & UINT64_MAX))
#define HUFF_BIGRAM_OTHER_ITEM(x) ((tdb_item)(x >> 64))

struct huff_codebook{
    __uint128_t symbol;
    uint32_t bits;
} __attribute__((packed));

struct field_stats{
    uint32_t field_id_bits;
    uint32_t field_bits[0];
};

/* ENCODE */

int huff_create_codemap(const struct judy_128_map *gram_freqs,
                        struct judy_128_map *codemap);

void huff_encode_grams(const struct judy_128_map *codemap,
                       const __uint128_t *grams,
                       uint64_t num_grams,
                       char *buf,
                       uint64_t *offs,
                       const struct field_stats *fstats);

struct huff_codebook *huff_create_codebook(const struct judy_128_map *codemap,
                                           uint32_t *size);

struct field_stats *huff_field_stats(const uint64_t *field_cardinalities,
                                     uint64_t num_fields,
                                     uint64_t max_timestamp);

static inline uint64_t huff_encoded_max_bits(uint64_t num_grams)
{
    /*
    how many bits we need in the worst case to encode num_grams?

    - each gram may be a bigram encoded as two literals (* 2)
    - each literal takes 1 flag bit, 14 field bits, and 48 value bits
      in the worst case
    */
    return num_grams * 2 * (1 + 14 + 48);
}

/* DECODE */

/* this may return either an unigram or a bigram */
static inline __uint128_t huff_decode_value(const struct huff_codebook *codebook,
                                            const char *data,
                                            uint64_t *offset,
                                            const struct field_stats *fstats)
{
    /* TODO - we could have a special read_bits for this case */
    uint64_t enc = read_bits64(data, *offset, 64);
    if (enc & 1){
        uint16_t idx = HUFF_CODE(enc >> 1);
        *offset += codebook[idx].bits + 1;
        return codebook[idx].symbol;
    }else{
        /* read literal:
           [0 (1 bit) | field-id (field_id_bits) | value (field_bits[field_id])]
        */
        tdb_field field = (tdb_field)((enc >> 1) &
                                      ((1LLU << fstats->field_id_bits) - 1));
        tdb_val val = (enc >> (fstats->field_id_bits + 1)) &
                      ((1LLU << fstats->field_bits[field]) - 1);
        *offset += 1 + fstats->field_id_bits + fstats->field_bits[field];
        return tdb_make_item(field, val);
    }
}

#endif /* __HUFFMAN_H__ */
