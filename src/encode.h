
#ifndef __TRAILDB_ENCODE_H__
#define __TRAILDB_ENCODE_H__

#include <stdint.h>

#include <Judy.h>

#define MAX_FIELD_SIZE 1024
#define MAX_NUM_FIELDS 255
#define MAX_NUM_INPUTS 10000000
#define INVALID_RATIO 0.005

/* We want to filter out all corrupted and invalid timestamps
   but we don't know the exact timerange we should be getting.
   Hence, we assume a reasonable range. */
#define TSTAMP_MIN 1325404800 /* 2012-01-01 */
#define TSTAMP_MAX 1483257600 /* 2017-01-01 */

struct logline{
    uint64_t values_offset;
    uint32_t num_values;
    uint32_t timestamp;
    uint64_t prev_logline_idx;
};

void store_cookies(const Pvoid_t cookie_index,
                   uint64_t num_cookies,
                   const char *path);

void store_lexicon(Pvoid_t lexicon, const char *path);

#define EDGE_INCREMENT 1000000

struct cookie_logline{
    uint64_t values_offset;
    uint32_t num_values;
    uint32_t timestamp;
    uint64_t cookie_id;
};

uint32_t edge_encode_fields(const uint32_t *values,
                            uint32_t **encoded,
                            uint32_t *encoded_size,
                            uint32_t *prev_values,
                            const struct cookie_logline *line);

void store_trails(const uint64_t *cookie_pointers,
                  uint64_t num_cookies,
                  struct logline *loglines,
                  uint64_t num_loglines,
                  const uint32_t *values,
                  uint64_t num_values,
                  uint32_t num_fields,
                  const uint64_t *field_cardinalities,
                  const char *root);

/* trail_encode_model */

struct gram_bufs{
    uint64_t *chosen;
    uint64_t *scores;
    uint8_t *covered;
    uint32_t num_fields;
};

void init_gram_bufs(struct gram_bufs *b, uint32_t num_fields);
void free_gram_bufs(struct gram_bufs *b);

uint32_t choose_grams(const uint32_t *encoded,
                      int num_encoded,
                      const Pvoid_t gram_freqs,
                      struct gram_bufs *g,
                      uint64_t *grams,
                      const struct cookie_logline *line);

Pvoid_t make_grams(FILE *grouped,
                   uint64_t num_loglines,
                   const uint32_t *values,
                   uint32_t num_fields,
                   const Pvoid_t unigram_freqs);

Pvoid_t collect_unigrams(FILE *grouped,
                         uint64_t num_loglines,
                         const uint32_t *values,
                         uint32_t num_fields);

#endif /* __TRAILDB_ENCODE_H__ */
