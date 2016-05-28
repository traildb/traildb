
#ifndef __JUDY_128_MAP_H__
#define __JUDY_128_MAP_H__

#include <Judy.h>
#include <stdint.h>

typedef void *(*judy_128_fold_fn)(__uint128_t key, Word_t *value, void*);

struct judy_128_map{
    Pvoid_t hi_map;
};

#define J128_EXPORT __attribute__((visibility("default")))

J128_EXPORT
void j128m_init(struct judy_128_map *j128m);

J128_EXPORT
Word_t *j128m_insert(struct judy_128_map *j128m, __uint128_t key);

J128_EXPORT
Word_t *j128m_get(const struct judy_128_map *j128m, __uint128_t key);

J128_EXPORT
int j128m_del(const struct judy_128_map *j128m, __uint128_t key);

J128_EXPORT
void j128m_find(const struct judy_128_map *j128m, PWord_t *pv, __uint128_t *key);

J128_EXPORT
void j128m_next(const struct judy_128_map *j128m, PWord_t *pv, __uint128_t *key);

J128_EXPORT
void *j128m_fold(const struct judy_128_map *j128m,
                 judy_128_fold_fn fun,
                 void *state);

J128_EXPORT
uint64_t j128m_num_keys(const struct judy_128_map *j128m);

J128_EXPORT
void j128m_free(struct judy_128_map *j128m);

#endif /* __JUDY_128_MAP_H__ */
