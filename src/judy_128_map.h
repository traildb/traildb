
#ifndef __JUDY_128_MAP_H__
#define __JUDY_128_MAP_H__

#include <Judy.h>
#include <stdint.h>

typedef void *(*judy_128_fold_fn)(__uint128_t key, Word_t *value, void*);

struct judy_128_map{
    Pvoid_t hi_map;
};

void j128m_init(struct judy_128_map *j128m);

Word_t *j128m_insert(struct judy_128_map *j128m, __uint128_t key);

Word_t *j128m_get(struct judy_128_map *j128m, __uint128_t key);

void *j128m_fold(const struct judy_128_map *j128m,
                 judy_128_fold_fn fun,
                 void *state);

void j128m_free(struct judy_128_map *j128m);

#endif /* __JUDY_128_MAP_H__ */
