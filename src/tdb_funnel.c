#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "traildb.h"
#include "tdb_funnel.h"

#ifndef MAP_ANON
#define MAP_ANON MAP_ANONYMOUS
#endif

#define pinfo(fmt, ...) fprintf(stderr, fmt"\n", ##__VA_ARGS__)
#define pwarn(fmt, ...) fprintf(stderr, fmt"\n", ##__VA_ARGS__)

fdb *fdb_create(tdb *tdb, tdb_fold_fn probe, fdb_fid num_funnels, void *params) {
  fdb *db = NULL;
  fdb_cons cons = {};
  cons.counters = calloc(num_funnels, sizeof(fdb_counter));
  cons.funnels = calloc(num_funnels, sizeof(fdb_funnel));
  cons.params = params;
  if (cons.counters == NULL || cons.funnels == NULL)
    goto done;
  tdb_fold(tdb, probe, &cons);

  uint64_t data_size = 0, N = tdb_num_cookies(tdb), dense_size = N * sizeof(fdb_mask);
  fdb_counter *counter;
  fdb_funnel *funnel;
  fdb_fid i;
  for (i = 0; i < num_funnels; i++) {
    counter = &cons.counters[i];
    funnel = &cons.funnels[i];
    funnel->offs = data_size;
    funnel->length = counter->count;
    if (funnel->length * sizeof(fdb_elem) > dense_size) {
      funnel->flags |= FDB_DENSE;
      funnel->length = N;
      data_size += funnel->length * sizeof(fdb_mask);
    } else {
      funnel->flags |= FDB_SPARSE;
      data_size += funnel->length * sizeof(fdb_elem);
    }
    counter->count = -1;
    counter->last = -1;
  }

  uint64_t size = fdb_size(num_funnels, data_size);
  db = mmap(NULL, size, PROT_READ | PROT_WRITE, MAP_ANON | MAP_PRIVATE, -1, 0);
  if (db == MAP_FAILED) {
    perror("mmap");
    db = NULL;
    goto done;
  }
  db->data_offs = fdb_offs(num_funnels);
  db->data_size = data_size;
  if (params)
    memcpy(db->params, params, FDB_PARAMS);
  db->num_funnels = num_funnels;
  memcpy(db->funnels, cons.funnels, num_funnels * sizeof(fdb_funnel));

  cons.data = ((uint8_t *)db) + db->data_offs;
  cons.phase = 1;
  tdb_fold(tdb, probe, &cons);

 done:
  free(cons.counters);
  free(cons.funnels);
  return db;
}

void fdb_detect(fdb_fid funnel_id, fdb_eid id, fdb_mask bits, fdb_cons *cons) {
  fdb_counter *counter = &cons->counters[funnel_id];
  if (cons->phase == 0) {
    if (counter->last < id || counter->count == 0) {
      counter->last = id;
      counter->count++;
    }
  } else {
    fdb_funnel *funnel = &cons->funnels[funnel_id];
    if (funnel->flags & FDB_DENSE) {
      fdb_mask *mask = (fdb_mask *)&cons->data[funnel->offs];
      mask[id] |= bits;
    } else {
      fdb_elem *elem = (fdb_elem *)&cons->data[funnel->offs];
      if (counter->last < id)
        elem[++counter->count].id = counter->last = id;
      elem[counter->count].mask |= bits;
    }
  }
}

static
void *fdb_ezprobe(const tdb *db, uint64_t id, const tdb_item *items, void *acc) {
  fdb_cons *cons = (fdb_cons *)acc;
  fdb_ez *params = (fdb_ez *)cons->params;
  fdb_mask mask = 1LLU << tdb_item_val(items[params->mask_field]);
  fdb_fid key, *O = params->key_offs;
  tdb_field k, *K = params->key_fields;
  unsigned int i, j = 0, N = params->num_keys;
  for (i = 0; i < N; i++) {
    for (key = j ? O[j - 1] : 0; (k = K[j]); j++)
      key += tdb_item_val(items[k]) * O[j];
    j++;
    fdb_detect(key, id, mask, cons);
  }
  return acc;
}

fdb *fdb_easy(tdb *tdb, fdb_ez *params) {
  fdb_fid num_funnels = 0, size = 0, *O = params->key_offs;
  tdb_field k, *K = params->key_fields;
  unsigned int i, j = 0, N = params->num_keys;
  for (i = 0; i < N; i++) {
    for (size = 1; (k = K[j]); j++) {
      O[j] = size;
      size *= tdb_lexicon_size(tdb, k);
    }
    O[j++] = num_funnels += size;
  }
  if (tdb_lexicon_size(tdb, params->mask_field) > 8 * sizeof(fdb_mask)) {
    pwarn("mask field cardinality too high");
    return NULL;
  }
  return fdb_create(tdb, fdb_ezprobe, num_funnels, params);
}

fdb *fdb_dump(fdb *db, int fd) {
  size_t size = fdb_size(db->num_funnels, db->data_size), total = 0;
  ssize_t wrote = 0;
  while ((wrote = write(fd, (char *)db + total, size))) {
    if (wrote < 0) {
      perror("write");
      return NULL;
    }
    size -= wrote;
    total += wrote;
  }
  return db;
}

fdb *fdb_load(int fd) {
  fdb *db;
  struct stat buf;
  if (fstat(fd, &buf)) {
    perror("fstat");
    return NULL;
  }
  db = mmap(NULL, buf.st_size, PROT_READ, MAP_PRIVATE, fd, 0);
  if (db == MAP_FAILED) {
    perror("mmap");
    return NULL;
  }
  return db;
}

fdb *fdb_free(fdb *db) {
  munmap(db, fdb_size(db->num_funnels, db->data_size));
  return NULL;
}

static inline
int fdb_filter(uint64_t mask, const fdb_cnf *cnf) {
  unsigned int i;
  if (cnf)
    for (i = 0; i < cnf->num_clauses; i++)
      if (!((mask & cnf->clauses[i].terms) || (~mask & cnf->clauses[i].nterms)))
        return 0;
  return mask != 0;
}

static inline
fdb_elem *fdb_iter_next_simple(fdb_iter *iter) {
  const fdb_set_simple *s = &iter->set->simple;
  const fdb_funnel *funnel = &s->db->funnels[s->funnel_id];
  fdb_eid i, *index = &iter->index;
  fdb_elem *next = &iter->next;
  uint8_t *data = (uint8_t *)s->db + s->db->data_offs;
  if (funnel->flags & FDB_DENSE) {
    for (i = *index; i < funnel->length; i++) {
      fdb_mask *mask = (fdb_mask *)&data[funnel->offs];
      if (fdb_filter(mask[i], s->cnf)) {
        next->id = i;
        next->mask = mask[i];
        *index = i + 1;
        return next;
      }
    }
  } else {
    for (i = *index; i < funnel->length; i++) {
      fdb_elem *elem = (fdb_elem *)&data[funnel->offs];
      if (fdb_filter(elem[i].mask, s->cnf)) {
        next->id = elem[i].id;
        next->mask = elem[i].mask;
        *index = i + 1;
        return next;
      }
    }
  }
  return NULL;
}

static inline
fdb_elem *fdb_iter_next_complex(fdb_iter *iter) {
  const fdb_set_complex *c = &iter->set->complex;
  fdb_elem *next = &iter->next;
  fdb_eid min;
  unsigned int i, N = c->num_sets, argmin = 0;
  uint64_t membership;
  while (iter->num_left) {
    membership = 0;
    min = -1; // unsigned
    for (i = 0; i < N; i++)
      if (!(iter->empty & (1LLU << i)) && iter->iters[i]->next.id < min)
        min = iter->iters[argmin = i]->next.id;
    next->id = min;
    next->mask = 0;
    for (i = 0; i < N; i++) {
      if (!(iter->empty & (1LLU << i)) && iter->iters[i]->next.id == min) {
        if (!fdb_iter_next(iter->iters[i])) {
          iter->empty |= 1LLU << i;
          iter->num_left--;
        }
        next->mask |= iter->iters[i]->next.mask;
        membership |= 1LLU << i;
      }
    }
    if (fdb_filter(membership, c->cnf))
      return next;
  }
  return 0;
}

fdb_iter *fdb_iter_new(const fdb_set *set) {
  fdb_iter *iter = calloc(1, sizeof(fdb_iter));
  if (iter == NULL)
    return NULL;

  iter->set = set;

  if (set->flags & FDB_COMPLEX) {
    const fdb_set_complex *c = &iter->set->complex;
    iter->num_left = c->num_sets;
    iter->iters = calloc(c->num_sets, sizeof(fdb_iter));
    if (iter->iters == NULL || c->num_sets > 64)
      return fdb_iter_free(iter);

    unsigned int i;
    for (i = 0; i < c->num_sets; i++) {
      iter->iters[i] = fdb_iter_new(&c->sets[i]);
      if (!fdb_iter_next(iter->iters[i])) {
        iter->empty |= 1LLU << i;
        iter->num_left--;
      }
    }
  }
  return iter;
}

fdb_elem *fdb_iter_next(fdb_iter *iter) {
  if (iter->set->flags & FDB_SIMPLE)
    return fdb_iter_next_simple(iter);
  return fdb_iter_next_complex(iter);
}

fdb_iter *fdb_iter_free(fdb_iter *iter) {
  if (iter->set->flags & FDB_COMPLEX) {
    unsigned int i;
    for (i = 0; i < iter->set->complex.num_sets; i++)
      fdb_iter_free(iter->iters[i]);
  }
  free(iter->iters);
  free(iter);
  return NULL;
}

int fdb_count_set(const fdb_set *set, fdb_eid *count) {
  int k;
  fdb_iter *iter = fdb_iter_new(set);
  *count = 0;
  for (k = 0; fdb_iter_next(iter); k++)
    (*count)++;
  fdb_iter_free(iter);
  return 0;
}

int fdb_count_family(const fdb_family *family, fdb_eid *counts) {
  int i, k;
  fdb_set all = {
    .flags = FDB_SIMPLE,
    .simple = {.db = family->db, .funnel_id = family->funnel_id}
  };
  fdb_iter *iter = fdb_iter_new(&all);
  fdb_elem *next;
  memset(counts, 0, family->num_sets * sizeof(fdb_eid));
  for (k = 0; (next = fdb_iter_next(iter)); k++)
    for (i = 0; i < family->num_sets; i++)
      if (fdb_filter(next->mask, &family->cnfs[i]))
        counts[i]++;
  fdb_iter_free(iter);
  return 0;
}
