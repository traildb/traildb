
#ifndef __BREADCRUMBS_ENCODER_H__
#define __BREADCRUMBS_ENCODER_H__

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include <Judy.h>

#include "util.h"

#define MAX_FIELD_SIZE 1024
#define MAX_PATH_SIZE 1024

struct logline{
    uint32_t values_offset;
    uint32_t num_values;
    uint32_t timestamp;
    uint32_t prev_logline_idx;
};

struct cookie{
    uint32_t last_logline_idx;
    uint32_t previous_values[0];
} __attribute((packed))__;

void store_lexicon(Pvoid_t lexicon, const char *path);

void store_trails(const uint32_t *fields,
                  uint32_t num_fields,
                  const struct cookie *cookies,
                  uint32_t num_cookies,
                  uint32_t cookie_size,
                  const struct logline *loglines,
                  uint32_t num_loglines,
                  const char *path);

#endif /* __BREADCRUMBS_ENCODER_H__ */
