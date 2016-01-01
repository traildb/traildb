
#include <assert.h>
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <float.h>
#include <string.h>

#define DSFMT_MEXP 521
#include <dsfmt/dSFMT.h>

#include <tdb_bits.h>

#define NUM_WORDS 1000

static uint32_t popcount(uint64_t val)
{
    uint32_t c = 0;
    while (val){
        c += val & 1;
        val >>= 1;
    }
    return c;
}

uint64_t random_bytes(char *dst, uint64_t num_words, uint64_t seed)
{
    uint64_t popsum = 0;
    dsfmt_t state;
    dsfmt_init_gen_rand(&state, seed);

    while (num_words--){
        double tmp = dsfmt_genrand_close_open(&state) * DBL_MAX;
        uint64_t val;
        memcpy(&dst[num_words * 8], &tmp, 8);
        memcpy(&val, &tmp, 8);
        popsum += popcount(val);
    }
    return popsum;
}

int main(int argc, char **argv)
{
    static char src_rand[(NUM_WORDS + 1) * 8];
    static char dst_rand[(NUM_WORDS + 1) * 8];
    uint64_t i, width, shift;

    for (i = 0; i < 20; i++){
        uint64_t correct_popsum = random_bytes(src_rand, NUM_WORDS, 984345 + i);
        for (width = 1; width < 65; width++){
            for (shift = 0; shift < 9; shift++){
                uint64_t offs, popsum = 0;
                /* substract the first shift bits from the sum */
                uint64_t popsum_shift = popcount(src_rand[0] & ((1 << shift) - 1));

                memset(dst_rand, 0, (NUM_WORDS + 1) * 8);

                for (offs = shift; offs < NUM_WORDS * 64; offs += width){
                    uint64_t val = read_bits64(src_rand, offs, width);
                    write_bits64(dst_rand, offs, val);
                    assert(val == read_bits64(dst_rand, offs, width));
                }

                for (offs = shift; offs < NUM_WORDS * 64; offs += width)
                    popsum += popcount(read_bits64(dst_rand, offs, width));

                assert(popsum == (correct_popsum - popsum_shift));
            }
        }
    }

    return 0;
}
