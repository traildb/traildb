
#ifndef __DDB_BITS_H__
#define __DDB_BITS_H__

static inline uint64_t read_bits(const char *src, uint64_t offs, uint32_t bits)
{
    uint64_t *src_w = (uint64_t*)&src[offs >> 3];
    return (*src_w >> (offs & 7)) & ((1 << bits) - 1);
} __attribute__((unused))

static inline void write_bits(char *dst, uint64_t offs, uint32_t val)
{
    uint64_t *dst_w = (uint64_t*)&dst[offs >> 3];
    *dst_w |= ((uint64_t)val) << (offs & 7);
} __attribute__((unused))

#endif /* __DDB_BITS_H__ */
