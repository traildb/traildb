#ifndef __TRAIL_MERGE_H__
#define __TRAIL_MERGE_H__

#include <stdint.h>

#include "extractd.h"

#define MAPPER_TIMEOUT 2 * 60 * 1000

struct extractd_ctx{
    int port;
    int show;
    uint32_t num_mappers;
    const char *groupby_str;
    const char *dir;
    struct extractd *extd;
};

void grouper_output(struct extractd_ctx *ctx);
void grouper_process(struct extractd_ctx *ctx);
const char *safe_filename(const char *value);

#endif /* __TRAIL_MERGE_H__ */
