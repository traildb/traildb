
#ifndef __BREADCRUMBS_ENCODER_H__
#define __BREADCRUMBS_ENCODER_H__

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include <Judy.h>

#define DIE_ON_ERROR(msg)\
    do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define DIE(msg, ...)\
    do { fprintf(stderr, "FAIL: ");\
         fprintf(stderr, msg, ##__VA_ARGS__);\
         exit(EXIT_FAILURE); } while (0)

#define SAFE_WRITE(ptr, size, path, f)\
    if (fwrite(ptr, size, 1, f) != 1){\
        DIE("Writing to %s failed\n", path);\
    }

#define MAX_FIELD_SIZE 1024

struct logline{
    uint32_t values_offset;
    uint32_t num_values;
    uint32_t timestamp;
    struct logline *prev;
};

struct cookie{
    struct logline *last;
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
