#define _DEFAULT_SOURCE /* mkstemp */
#define _GNU_SOURCE

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>

#include "tdb_internal.h"

static int compare(uint64_t t1, uint64_t t2)
{
    if (t1 > t2)
        return 1;
    if (t1 < t2)
        return -1;
    return 0;
}

#define SORT_TYPE      struct tdb_grouped_event
#define SORT_NAME      _events
#define SORT_CMP(x,y)  compare((x).timestamp, (y).timestamp)
#include "sort/sort.h"

void events_sort(struct tdb_grouped_event *buf, uint64_t num_events)
{
    /* We need to do a reversed stable sort. The textbook version would be
       to first reverse the whole buffer, and then apply a stable sort.
       But the underlying dataset can often be semi-sorted (eg: tdb_cons_append,
       or importing existing sorted datasets), hence we chose to use timsort
       that's very good at exploiting partially sorted datasets; if
       we reversed the whole buffer as first step, we would basically
       leave lots of performance on the table, as we would nullify the timsort
       advantage by destroying the initial partial sorting of events.
       So, instead, we do a stable sort first, and then go through the array
       and reverse sub-sequences of elements with the same timestamp; this
       keeps timsort fast, and in the (also likely) cases of sequences
       with no duplicated timestamps, we don't even do a single swap operation
       (after the sort). */
    _events_tim_sort(buf, num_events);
    for (int i = 0; i < num_events-1; i++){
        int j = i+1;
        if (buf[j].timestamp < buf[i].timestamp){
            /* double-check sort.h is not buggy, avoiding data corruption */
            fprintf(stderr, "critical: bug found in sort function, aborting\n");
            abort();
        } else if (buf[j].timestamp == buf[i].timestamp){
            do
                j++;
            while (j<num_events && buf[j].timestamp == buf[i].timestamp);
            int n = j-i;
            for (int k = 0; k < n/2; k++){
                int k1 = k;
                int k2 = n-k-1;
                struct tdb_grouped_event tmp = buf[i+k1];
                buf[i+k1] = buf[i+k2];
                buf[i+k2] = tmp;
            }
            i = j-1;
        }
    }
}
