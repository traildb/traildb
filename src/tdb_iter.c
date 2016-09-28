#include "tdb_internal.h"

TDB_EXPORT tdb_iter *tdb_iter_new(const tdb *db, const struct tdb_event_filter *filter) {
  tdb_iter *iter = calloc(1, sizeof(tdb_iter));
  if (iter == NULL)
    return NULL;
  if ((iter->cursor = tdb_cursor_new(db)) == NULL) {
    free(iter);
    return NULL;
  }
  if (filter)
    if (tdb_cursor_set_event_filter(iter->cursor, filter) != TDB_ERR_OK) {
      tdb_iter_free(iter);
      return NULL;
    }
  return iter;
}

TDB_EXPORT tdb_iter *tdb_iter_next(tdb_iter *restrict iter) {
  while (iter->marker++ < iter->cursor->state->db->num_trails) {
    if (tdb_get_trail(iter->cursor, iter->marker - 1) == TDB_ERR_OK)
      if (tdb_cursor_peek(iter->cursor))
        return iter;
  }
  return NULL;
}

TDB_EXPORT void tdb_iter_free(tdb_iter *iter) {
  free(iter->cursor);
  free(iter);
}
