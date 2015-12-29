
#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <stdint.h>
#include <Judy.h>

#include "judy_128_map.h"


void j128m_init(struct judy_128_map *j128m)
{
    memset(j128m, 0, sizeof(struct judy_128_map));
}

Word_t *j128m_insert(struct judy_128_map *j128m, __uint128_t key)
{
    uint64_t hi_key = (key >> 64) & UINT64_MAX;
    uint64_t lo_key = key & UINT64_MAX;
    Word_t *lo_ptr;
    Word_t *hi_ptr;
    Pvoid_t lo_map;

    JLI(hi_ptr, j128m->hi_map, hi_key);
    lo_map = (Pvoid_t)*hi_ptr;
    JLI(lo_ptr, lo_map, lo_key);
    *hi_ptr = (Word_t)lo_map;

    return lo_ptr;
}

Word_t *j128m_get(struct judy_128_map *j128m, __uint128_t key)
{
    uint64_t hi_key = (key >> 64) & UINT64_MAX;
    uint64_t lo_key = key & UINT64_MAX;
    Word_t *lo_ptr;
    Word_t *hi_ptr;
    Pvoid_t lo_map;

    JLG(hi_ptr, j128m->hi_map, hi_key);
    if (hi_ptr){
        lo_map = (Pvoid_t)*hi_ptr;
        JLG(lo_ptr, lo_map, lo_key);
        if (lo_ptr)
            return lo_ptr;
    }

    return NULL;
}

void *j128m_fold(const struct judy_128_map *j128m,
                 judy_128_fold_fn fun,
                 void *state)
{
    uint64_t hi_key = 0;
    Word_t *hi_ptr;

    JLF(hi_ptr, j128m->hi_map, hi_key);
    while (hi_ptr){
        Pvoid_t lo_map = (Pvoid_t)*hi_ptr;
        uint64_t lo_key = 0;
        Word_t *lo_ptr;

        JLF(lo_ptr, lo_map, lo_key);
        while (lo_ptr){
            __uint128_t key = hi_key;
            key <<= 64;
            key |= lo_key;
            state = fun(key, lo_ptr, state);
            JLN(lo_ptr, lo_map, lo_key);
        }

        JLN(hi_ptr, j128m->hi_map, hi_key);
    }

    return state;
}

static void *num_keys_fun(__uint128_t key __attribute__((unused)),
                          Word_t *value __attribute__((unused)),
                          void *state)
{
    ++*(uint64_t*)state;
    return state;
}

uint64_t j128m_num_keys(const struct judy_128_map *j128m)
{
    uint64_t count = 0;
    j128m_fold(j128m, num_keys_fun, &count);
    return count;
}

void j128m_free(struct judy_128_map *j128m)
{
    uint64_t hi_key = 0;
    Word_t *hi_ptr;
    Word_t tmp;

    JLF(hi_ptr, j128m->hi_map, hi_key);
    while (hi_ptr){
        Pvoid_t lo_map = (Pvoid_t)*hi_ptr;
        JLFA(tmp, lo_map);
        JLN(hi_ptr, j128m->hi_map, hi_key);
    }
    JLFA(tmp, j128m->hi_map);
    j128m->hi_map = NULL;
}

