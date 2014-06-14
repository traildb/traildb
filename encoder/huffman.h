
#ifndef __BREADCRUMBS_HUFFMAN__
#define __BREADCRUMBS_HUFFMAN__

#include <stdint.h>

#include <Judy.h>

#define HUFF_CODEBOOK_SIZE 65536
#define HUFF_CODE(x) ((x) & 65535)
#define HUFF_BITS(x) (((x) & (65535 << 16)) >> 16)

struct huff_codebook;

/* ENCODE */

Pvoid_t huff_create_codemap(const Pvoid_t key_freqs);

void huff_encode_values(const Pvoid_t codemap,
                        uint32_t timestamp,
                        const uint32_t *values,
                        uint32_t num_values,
                        char *buf,
                        uint64_t *offs);

void huff_store_codebook(const Pvoid_t codemap,
                         const char *path);

/* DECODE */

struct huff_codebook *huff_load_codebook(const char *path);

void huff_decode_fields(const struct huff_codebook *codebook,
                        const char *data,
                        uint32_t *data_size,
                        uint32_t **fields,
                        uint32_t *num_fields,
                        uint32_t *fields_size);

struct huff_codebook *huff_create_codebook(const Pvoid_t codemap,
                                           uint32_t *size);

#endif /* __BREADCRUMBS_HUFFMAN__ */
