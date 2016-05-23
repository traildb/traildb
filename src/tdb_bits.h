
#ifndef __TDB_BITS_H__
#define __TDB_BITS_H__

/* NOTE:
these functions may access extra 7 bytes beyond the end of the destionation.
To avoid undefined behavior, make sure there's always a padding of 7 zero bytes
after the last byte offset accessed.
*/
static inline uint64_t read_bits(const char *src, uint64_t offs, uint32_t bits)
{
    /* this assumes that bits <= 48 */
    const uint64_t *src_w = (const uint64_t*)&src[offs >> 3];
    return (*src_w >> (offs & 7)) & (((1LLU << bits) - 1));
}

static inline void write_bits(char *dst, uint64_t offs, uint64_t val)
{
    /* this assumes that (val >> 48) == 0 */
    uint64_t *dst_w = (uint64_t*)&dst[offs >> 3];
    *dst_w |= ((uint64_t)val) << (offs & 7);
}

/* TODO benchmark 64-bit versions against a version using __uint128_t */
static inline void write_bits64(char *dst, uint64_t offs, uint64_t val)
{
    write_bits(dst, offs, val);
    val >>= 48;
    if (val)
        write_bits(dst, offs + 48, val);
}

static inline uint64_t read_bits64(const char *src,
                                   uint64_t offs,
                                   uint32_t bits)
{
    if (bits > 48){
        uint64_t val = read_bits(src, offs + 48, bits - 48);
        val <<= 48;
        val |= read_bits(src, offs, 48);
        return val;
    }else
        return read_bits(src, offs, bits);
}

#endif /* __TDB_BITS_H__ */
