
#ifndef __JUDY_STR_MAP_H__
#define __JUDY_STR_MAP_H__

#include <stdint.h>

#include <Judy.h>
#include <xxhash.h>

#define NUM_CUCKOO_MAPS 8
#define BUFFER_INITIAL_SIZE 65536

typedef void *(*judy_str_fold_fn)(uint64_t id,
                                  const char *value,
                                  uint64_t length,
                                  void *);

struct judy_str_map{
    char *buffer;
    uint64_t buffer_offset;
    uint64_t buffer_size;
    Pvoid_t small_map;
    Pvoid_t maps[NUM_CUCKOO_MAPS];
    uint64_t num_keys;
    XXH64_state_t hash_state;
};

int jsm_init(struct judy_str_map *jsm);

uint64_t jsm_insert(struct judy_str_map *jsm, const char *buf, uint64_t length);

void jsm_free(struct judy_str_map *jsm);

void *jsm_fold(const struct judy_str_map *jsm,
               judy_str_fold_fn fun,
               void *state);

#endif /* __JUDY_STR_MAP_H__ */
