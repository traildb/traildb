
#ifndef __TDB_SORT_H__
#define __TDB_SORT_H__

#include "tdb_types.h"

struct tdb_grouped_event;

void events_sort(struct tdb_grouped_event *buf, uint64_t num_events);

#endif /* __TDB_SORT_H__ */
