#ifndef __FUNNELDB_H__
#define __FUNNELDB_H__

/*
 * FunnelDB provides efficient set operations on the keys of a TrailDB.
 * It's perfect for performing user funnel analysis.
 *
 * Sets in a FunnelDB are either simple or complex.
 * A simple set is a 'row' in the DB, plus a query term (filter).
 * A complex set is a combination of other sets (simple or complex).
 * Arbitrary logical operations can be computed using these building blocks.
 *
 * An individual row in FunnelDB contains many possible sets.
 * A row is like a miniatiure universe within the database (aka 'funnel').
 * Rows can represent many different kind of things, such as:
 *  - all cookies seen on a particular network
 *  - all cookies coming from a particular domain
 *  - all cookies associated with a particular browser or device
 * etc.
 * Information about the sets within a row is stored in the 'mask'.
 *
 * A mask is a small group of flags used to represent steps in a funnel.
 * Generally each bit in the mask indicates the presence of a 'type' of event.
 * In the digital advertising world, the mask might contain bits for:
 *  - the presence of a bid request on the cookie (within a row)
 *  - the presence of a bid response on the cookie (within a row)
 *  - the presence of an impression served to the cookie (within a row)
 * etc.
 *
 * Finally, rows can be either dense or sparse.
 * Each sparse row is an ordered lists of integers (+ mask).
 * Each dense row is an array of masks.
 * However, these are implementation details not exposed by the interface.
 * FunnelDB automatically determines the most efficient way to store each row.
 */

#include <stdint.h>

#include "traildb.h"

#define FDB_DENSE   1
#define FDB_SPARSE  2

#define FDB_SIMPLE  1
#define FDB_COMPLEX 2

#define FDB_PARAMS 1024

/* The offset after the header + table of contents */
#define fdb_offs(N) (sizeof(fdb) + N * sizeof(fdb_funnel))
/* The total size of a database */
#define fdb_size(N, S) (fdb_offs(N) + S)

typedef uint32_t fdb_eid; /* element id */
typedef uint32_t fdb_fid; /* funnel id */
typedef uint8_t fdb_mask; /* default mask is 8 bits */

typedef struct {
  fdb_eid id;
  fdb_mask mask;
} fdb_elem;

typedef struct {
  uint8_t flags;
  uint64_t offs;
  fdb_eid length;
} fdb_funnel;

typedef struct {
  int64_t count;
  int64_t last;
} fdb_counter;

typedef struct {
  int phase;
  fdb_counter *counters;
  fdb_funnel *funnels;
  uint8_t *data;
  void *params;
} fdb_cons;

typedef struct {
  unsigned int num_keys;
  fdb_fid key_offs[128];
  tdb_field key_fields[128];
  tdb_field mask_field;
  uint8_t padded[FDB_PARAMS];
} fdb_ez;

typedef struct {
  uint64_t data_offs;
  uint64_t data_size;
  uint8_t params[FDB_PARAMS];
  fdb_fid num_funnels;
  fdb_funnel funnels[];
} fdb;

typedef struct {
  uint64_t terms;   /* supports up to 64 vars */
  uint64_t nterms;
} fdb_clause;

typedef struct {
  unsigned int num_clauses;
  fdb_clause *clauses;
} fdb_cnf;

typedef struct _fdb_set fdb_set;
typedef struct _fdb_iter fdb_iter;

typedef struct _fdb_set_simple {
  fdb *db;
  fdb_fid funnel_id;
  fdb_cnf *cnf;
} fdb_set_simple;

typedef struct _fdb_set_complex {
  fdb *db;
  unsigned int num_sets;
  fdb_set *sets;
  fdb_cnf *cnf;
} fdb_set_complex;

typedef struct _fdb_family {
  fdb *db;
  unsigned int num_sets;
  fdb_fid funnel_id;
  fdb_cnf *cnfs;
} fdb_family;

struct _fdb_set {
  uint8_t flags;
  union {
    fdb_set_simple simple;
    fdb_set_complex complex;
  };
};

struct _fdb_iter {
  const fdb_set *set;
  unsigned int num_left;
  uint64_t empty;
  fdb_eid index;
  fdb_elem next;
  fdb_iter **iters;
};

fdb *fdb_create(tdb *tdb, tdb_fold_fn probe, fdb_fid num_funnels, void *params);
void fdb_detect(fdb_fid funnel_id, fdb_eid id, fdb_mask bits, fdb_cons *state);

fdb *fdb_easy(tdb *tdb, fdb_ez *params);
fdb *fdb_dump(fdb *db, int fd);
fdb *fdb_load(int fd);
fdb *fdb_free(fdb *db);

fdb_iter *fdb_iter_new(const fdb_set *set);
fdb_elem *fdb_iter_next(fdb_iter *iter);
fdb_iter *fdb_iter_free(fdb_iter *iter);

int fdb_count_set(const fdb_set *set, fdb_eid *count);
int fdb_count_family(const fdb_family *family, fdb_eid *counts);

#endif /* __FUNNELDB_H__ */
