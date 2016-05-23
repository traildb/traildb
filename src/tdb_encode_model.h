
#ifndef __TDB_ENCODE_MODEL_H__
#define __TDB_ENCODE_MODEL_H__

#include <stdio.h>
#include <stdint.h>

#include <Judy.h>

#include "tdb_types.h"
#include "judy_128_map.h"

struct gram_bufs{
    __uint128_t *chosen;
    uint64_t *scores;
    /* size of the above two */
    uint64_t buf_len;

    uint8_t *covered;
    uint64_t num_fields;
};

int init_gram_bufs(struct gram_bufs *b, uint64_t num_fields);
void free_gram_bufs(struct gram_bufs *b);

int choose_grams_one_event(const tdb_item *encoded,
                           uint64_t num_encoded,
                           const struct judy_128_map *gram_freqs,
                           struct gram_bufs *g,
                           __uint128_t *grams,
                           uint64_t *num_grams,
                           const struct tdb_grouped_event *ev);

int make_grams(FILE *grouped,
               uint64_t num_events,
               const tdb_item *items,
               uint64_t num_fields,
               const Pvoid_t unigram_freqs,
               struct judy_128_map *final_freqs);

Pvoid_t collect_unigrams(FILE *grouped,
                         uint64_t num_events,
                         const tdb_item *items,
                         uint64_t num_fields);

#endif /* __TDB_ENCODE_MODEL_H__ */
