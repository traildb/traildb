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
  fdb_mask mask = 1 << tdb_item_val(items[params->mask_field]);
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
  if (write(fd, db, fdb_size(db->num_funnels, db->data_size)) < 0) {
    perror("write");
    return NULL;
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
int fdb_filter(fdb_mask mask, const fdb_cnf *cnf) {
  unsigned int i;
  if (cnf)
    for (i = 0; i < cnf->num_clauses; i++)
      if (!((mask & cnf->clauses[i].terms) || (~mask & cnf->clauses[i].nterms)))
        return 0;
  return 1;
}

int fdb_next(const fdb_set *set, fdb_eid *index, fdb_elem *next) {
  fdb_funnel *funnel = &set->db->funnels[set->funnel_id];
  fdb_eid i;
  uint8_t *data = (uint8_t *)set->db + set->db->data_offs;
  if (funnel->flags & FDB_DENSE) {
    for (i = *index; i < funnel->length; i++) {
      fdb_mask *mask = (fdb_mask *)&data[funnel->offs];
      if (fdb_filter(mask[i], set->cnf)) {
        next->id = i;
        next->mask = mask[i];
        return *index = i + 1;
      }
    }
  } else {
    for (i = *index; i < funnel->length; i++) {
      fdb_elem *elem = (fdb_elem *)&data[funnel->offs];
      if (fdb_filter(elem[i].mask, set->cnf)) {
        next->id = elem[i].id;
        next->mask = elem[i].mask;
        return *index = i + 1;
      }
    }
  }
  return 0;
}

fdb_eid fdb_combine(const fdb_set *sets, int num_sets, fdb_venn *venn) {
  int i, N = num_sets, Q = N - 1;
  unsigned long long k, run = 0, overlap = 0, diff = 0, isect = 0;
  char empty[num_sets];
  fdb_eid next[num_sets], argmin = 0, lastarg, min, last;
  fdb_elem heads[num_sets];
  for (i = 0; i < num_sets; i++) {
    empty[i] = next[i] = 0;
    if (!fdb_next(&sets[i], &next[i], &heads[i])) {
      empty[i] = 1;
      N--;
    }
  }
  for (k = 0; N; k++) {
    min = -1; // unsigned
    for (i = 0; i < num_sets; i++)
      if (!empty[i] && heads[i].id < min)
        min = heads[argmin = i].id;
    if (argmin == 0)
      diff++;
    if (k) {
      if (min == last) {
        run++;
        overlap++;
        if (argmin && lastarg == 0)
          diff--;
      } else {
        run = 0;
      }
    }
    if (run == Q) {
      run = 0;
      isect++;
    }
    if (!fdb_next(&sets[argmin], &next[argmin], &heads[argmin])) {
      empty[argmin] = 1;
      N--;
    }
    lastarg = argmin;
    last = min;
  }
  if (venn) {
    venn->union_size = k - overlap;
    venn->intersection_size = isect;
    venn->difference_size = diff;
  }
  return k;
}

fdb_eid fdb_count(const fdb_family *family, int num_sets, fdb_eid *counts) {
  int i, k;
  fdb_elem head;
  fdb_eid next = 0;
  fdb_set all = *family;
  all.cnf = NULL;
  memset(counts, 0, num_sets * sizeof(fdb_eid));
  for (k = 0; fdb_next(&all, &next, &head); k++)
    for (i = 0; i < num_sets; i++)
      if (fdb_filter(head.mask, &family->cnf[i]))
        counts[i]++;
  return k;
}
