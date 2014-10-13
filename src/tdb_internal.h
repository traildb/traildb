
#ifndef __TDB_INTERNAL_H__
#define __TDB_INTERNAL_H__

#include <stdint.h>

#include <Judy.h>

#include "traildb.h"

#include "ddb_profile.h"
#define TDB_TIMER_DEF   DDB_TIMER_DEF
#define TDB_TIMER_START DDB_TIMER_START
#define TDB_TIMER_END   DDB_TIMER_END

void tdb_err(tdb *db, char *fmt, ...);
void tdb_path(char path[TDB_MAX_PATH_SIZE], char *fmt, ...);
int tdb_mmap(const char *path, tdb_file *dst, tdb *db);

void tdb_encode(const uint64_t *cookie_pointers,
                uint64_t num_cookies,
                tdb_event *events,
                uint64_t num_events,
                const tdb_item *items,
                uint64_t num_items,
                uint32_t num_fields,
                const uint64_t *field_cardinalities,
                const char *root);

uint32_t edge_encode_items(const tdb_item *items,
                           uint32_t **encoded,
                           uint32_t *encoded_size,
                           tdb_item *prev_items,
                           const tdb_cookie_event *ev);

#endif /* __TDB_INTERNAL_H__ */
