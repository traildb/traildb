
#ifndef __TDB_ENCODE_MODEL_H__
#define __TDB_ENCODE_MODEL_H__

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
                      const tdb_cookie_event *ev);

Pvoid_t make_grams(FILE *grouped,
                   uint64_t num_events,
                   const tdb_item *items,
                   uint32_t num_fields,
                   const Pvoid_t unigram_freqs);

Pvoid_t collect_unigrams(FILE *grouped,
                         uint64_t num_events,
                         const tdb_item *items,
                         uint32_t num_fields);

#endif /* __TDB_ENCODE_MODEL_H__ */
