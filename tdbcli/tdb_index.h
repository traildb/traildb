
#ifndef TDB_INDEX
#define TDB_INDEX

#include <stdint.h>

#include <traildb.h>

struct tdb_index;

char *tdb_index_find(const char *root);
struct tdb_index *tdb_index_open(const char *tdb_path, const char *index_path);
void tdb_index_close(struct tdb_index *index);

uint64_t *tdb_index_match_candidates(const struct tdb_index *index,
                                     const struct tdb_event_filter *filter,
                                     uint64_t *num_candidates);

tdb_error tdb_index_create(const char *tdb_path,
                           const char *index_path,
                           uint32_t num_shards);

#endif /* TDB_INDEX */
