
#ifndef __BREADCRUMBS__
#define __BREADCRUMBS__

#include <stdint.h>

#include "huffman.h"

#define MAX_PATH_SIZE 1024

#define BD_ERROR_SIZE (MAX_PATH_SIZE + 512)

struct bdfile{
    const char *data;
    uint64_t size;
};

struct breadcrumbs{
    uint32_t min_timestamp;
    uint32_t max_timestamp;
    uint32_t max_timestamp_delta;
    uint64_t num_cookies;
    uint64_t num_loglines;
    uint32_t num_fields;
    uint32_t *previous_values;

    struct bdfile cookies;
    struct bdfile codebook;
    struct bdfile trails;
    struct bdfile *lexicons;

    struct field_stats *fstats;

    int error_code;
    char error[BD_ERROR_SIZE];
};

#endif /* __BREADCRUMBS__ */

