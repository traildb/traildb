
#ifndef __BREADCRUMBS_HUFFMAN__
#define __BREADCRUMBS_HUFFMAN__

#include <stdint.h>

#include <Judy.h>

#include "ddb_bits.h"

#define HUFF_CODEBOOK_SIZE 65536
#define HUFF_CODE(x) ((x) & 65535)
#define HUFF_BITS(x) (((x) & (65535 << 16)) >> 16)

struct huff_codebook{
    uint32_t symbol;
    uint32_t bits;
} __attribute__((packed));

/* ENCODE */

Pvoid_t huff_create_codemap(const Pvoid_t key_freqs);

void huff_encode_values(const Pvoid_t codemap,
                        uint32_t timestamp,
                        const uint32_t *values,
                        uint32_t num_values,
                        char *buf,
                        uint64_t *offs);

struct huff_codebook *huff_create_codebook(const Pvoid_t codemap,
                                           uint32_t *size);

void huff_store_codebook(const Pvoid_t codemap,
                         const char *path);

/* DECODE */

static inline uint32_t huff_decode_value(const struct huff_codebook *codebook,
                                         const char *data,
                                         uint64_t *offset)
{
    uint64_t enc = read_bits(data, *offset, 33);
    if (enc & 1){
        uint16_t idx = HUFF_CODE(enc >> 1);
        *offset += codebook[idx].bits + 1;
        return codebook[idx].symbol;
    }else{
        *offset += 33;
        return (enc >> 1) & ((1LLU << 32) - 1);
    }
} __attribute__((unused))

#endif /* __BREADCRUMBS_HUFFMAN__ */
