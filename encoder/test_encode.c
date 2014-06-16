
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>

#include "hex_decode.h"

int main(int argc, char **argv)
{
    uint8_t res[16];
    int err = hex_decode(argv[1], res);
    int i;
    if (err){
        printf("not valid!\n");
    }else{
        for (i = 0; i < 16; i++)
            printf("%0.2x", res[i]);
        printf("\n");
    }
}
