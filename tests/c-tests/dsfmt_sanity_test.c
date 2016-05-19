
#include <stdlib.h>
#include <string.h>
#include <stdio.h>
#include <stdint.h>
#include <assert.h>
#include <float.h>

// explicitly set to non-maximum value
#define DSFMT_MEXP 521
#include <dsfmt/dSFMT.h>

int main(int argc, char **argv)
{
    // we are initializing an array of dsfmt_t 's
    // with a few close values and then generating values
    int size = 5;
    uint32_t seed = 4321;
    dsfmt_t dsfmt[size];

    dsfmt_init_gen_rand(&dsfmt[0], seed);
    for (int i = 1; i < size; i++) {
        dsfmt[i] = dsfmt[i-1];
    }

    for (int i = 0; i < size; i++) {
        printf("%f ", dsfmt_genrand_close_open(&dsfmt[i]));
    }
    printf("\n");

    return 0;
}
