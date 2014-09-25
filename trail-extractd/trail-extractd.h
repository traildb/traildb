#ifndef __TRAIL_EXTRACTD__
#define __TRAIL_EXTRACTD__

#include <stdint.h>

#include "extractd.h"

#define MAPPER_TIMEOUT 2 * 60 * 1000

struct extractd_ctx{
    int port;
    int show;
    uint32_t num_mappers;
    const char *groupby_str;
    const char *prefix;
    struct extractd *extd;
};

void grouper_output(struct extractd_ctx *ctx);
void grouper_process(struct extractd_ctx *ctx);

#endif /* __TRAIL_EXTRACTD__ */
