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

/* FunnelDB creation uses a probe to determine which funnels an event occurs in.
   The total number of funnels must be known in advance.
   Params data is stored in the DB, but is limited to FDB_PARAMS bytes. */
fdb *fdb_create(tdb *tdb, tdb_fold_fn probe, fdb_fid num_funnels, void *params) {
  fdb *db = NULL;
  fdb_cons cons = {};
  cons.counters = calloc(num_funnels, sizeof(fdb_counter));
  cons.funnels = calloc(num_funnels, sizeof(fdb_funnel));
  cons.params = params;
  if (cons.counters == NULL || cons.funnels == NULL)
    goto done;

  /* Run through the TrailDB once to figure out how much space is needed,
     and whether each funnel is dense or sparse */
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

  /* Allocate memory and copy the header + table of contents
     mmap instead of malloc, so we can always free using munmap */
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

  /* Run through the TrailDB again to actually store the funnel data */
  cons.data = ((uint8_t *)db) + db->data_offs;
  cons.phase = 1;
  tdb_fold(tdb, probe, &cons);

 done:
  free(cons.counters);
  free(cons.funnels);
  return db;
}

/* Probe functions call fdb_detect when they want to emit a funnel for an event.
   fdb_detect takes care of the details of how to store the info. */
void fdb_detect(fdb_fid funnel_id, fdb_eid id, fdb_mask bits, fdb_cons *cons) {
  fdb_counter *counter = &cons->counters[funnel_id];
  if (cons->phase == 0) { /* allocation phase */
    if (counter->last < id || counter->count == 0) {
      counter->last = id;
      counter->count++;
    }
  } else {               /* storage phase */
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

/* This probe implements the most common use case for FunnelDB:
   The user specifies which fields (or combinations) they are interested in.
   Funnels are created for every possible value of those fields.
   The user also specifies which field to use for the mask.
   The mask uses 1 bit for every possible value in the mask field. */
static
void *fdb_ezprobe(const tdb *db, uint64_t id, const tdb_item *items, void *acc) {
  fdb_cons *cons = (fdb_cons *)acc;
  fdb_ez *params = (fdb_ez *)cons->params;
  fdb_mask mask = 1LLU << tdb_item_val(items[params->mask_field]);
  fdb_fid key, *O = params->key_offs;
  tdb_field k, *K = params->key_fields;
  unsigned int i, j = 0, N = params->num_keys;
  /* compute the funnel id: much like a 1-D index for N-D matrix elements */
  for (i = 0; i < N; i++) {
    for (key = j ? O[j - 1] : 0; (k = K[j]); j++)
      key += tdb_item_val(items[k]) * O[j];
    j++;
    fdb_detect(key, id, mask, cons); /* emit funnel + mask */
  }
  return acc;
}

/* The easy way to construct a FunnelDB using the fdb_ezprobe.
   Every field (or combination) the user is interested in,
   has number of funnels given by the product of the cardinality of the fields.
   Compute total number of funnels, sanity check mask, and create the DB. */
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

/* Apply a cnf filter to a particular mask.
   The mask just represents a set of variables which are present or absent. */
static inline
int fdb_filter(uint64_t mask, const fdb_cnf *cnf) {
  unsigned int i;
  if (cnf)
    for (i = 0; i < cnf->num_clauses; i++)
      if (!((mask & cnf->clauses[i].terms) || (~mask & cnf->clauses[i].nterms)))
        return 0;
  return mask != 0;
}

/* Check if a particular term is required by a cnf formula.
   This doesn't have to return true in all cases,
   but when we know a term is required we can exit some queries early. */
static inline
int fdb_required(unsigned int iid, const fdb_cnf *cnf) {
  unsigned int i;
  if (cnf)
    for (i = 0; i < cnf->num_clauses; i++)
      if (cnf->clauses[i].terms == (1LLU << iid) && !cnf->clauses[i].nterms)
        return 1;
  return 0;
}

static inline
fdb_elem *fdb_iter_next_simple(fdb_iter *iter) {
  const fdb_set_simple *s = &iter->set->simple;
  const fdb_funnel *funnel = &s->db->funnels[s->funnel_id];
  fdb_eid i, *index = &iter->index;
  fdb_elem *next = &iter->next;
  uint8_t *data = (uint8_t *)s->db + s->db->data_offs;
  if (funnel->flags & FDB_DENSE) {
    /* dense: copy the mask and write the implicit id */
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
   /* sparse: copy the element exactly as is */
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
  /* each term has an associated set, which we can iterate over in order
     we just merge these iterators to traverse the elements in order */
  while (iter->num_left) { /* as long as there are terms left */
    membership = 0;
    min = -1; /* unsigned, so begin with the largest possible value */
    for (i = 0; i < N; i++)
      if (!(iter->empty & (1LLU << i)) && iter->iters[i]->next.id < min)
        min = iter->iters[argmin = i]->next.id; /* found a new minimum */
    next->id = min;
    next->mask = 0;
    /* figure out all terms which contain the minimum */
    for (i = 0; i < N; i++) {
      if (!(iter->empty & (1LLU << i)) && iter->iters[i]->next.id == min) {
        /* found one, pop it */
        if (!fdb_iter_next(iter->iters[i])) {
          /* this was the last element in the iterator */
          iter->empty |= 1LLU << i;
          if (iter->num_left)
            iter->num_left--;
          /* check if this term is required by the cnf, so we can exit early */
          if (fdb_required(i, c->cnf))
            iter->num_left = 0;
        }
        /* mark the term as a member of this set */
        next->mask |= iter->iters[i]->next.mask;
        membership |= 1LLU << i;
      }
    }
    /* apply the cnf to our set membership mask */
    if (fdb_filter(membership, c->cnf))
      return next; /* return the next one that passes the test */
  }
  return NULL;
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
      return fdb_iter_free(iter); /* memory error or too many terms */

    /* load up the first element of each term, or mark it empty */
    unsigned int i;
    for (i = 0; i < c->num_sets; i++) {
      iter->iters[i] = fdb_iter_new(&c->sets[i]);
      if (!fdb_iter_next(iter->iters[i])) {
        iter->empty |= 1LLU << i;
        if (iter->num_left)
          iter->num_left--;
        if (fdb_required(i, c->cnf))
          iter->num_left = 0;
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

/* Straightforward counting of the number of elements in any set */
int fdb_count_set(const fdb_set *set, fdb_eid *count) {
  int k;
  fdb_iter *iter = fdb_iter_new(set);
  *count = 0;
  for (k = 0; fdb_iter_next(iter); k++)
    (*count)++;
  fdb_iter_free(iter);
  return 0;
}

/* Efficient counting of a family of related sets
   (i.e. multiple queries on the same row) */
int fdb_count_family(const fdb_family *family, fdb_eid *counts) {
  int i, k;
  fdb_set all = {
    .flags = FDB_SIMPLE,
    .simple = {.db = family->db, .funnel_id = family->funnel_id}
  };
  fdb_iter *iter = fdb_iter_new(&all); /* an iterator over the entire row */
  fdb_elem *next;
  memset(counts, 0, family->num_sets * sizeof(fdb_eid));
  for (k = 0; (next = fdb_iter_next(iter)); k++) /* look at each element once */
    for (i = 0; i < family->num_sets; i++)
      if (fdb_filter(next->mask, &family->cnfs[i])) /* evaluate each query */
        counts[i]++;
  fdb_iter_free(iter);
  return 0;
}
