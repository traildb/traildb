
#include <Judy.h>

#include "breadcrumbs_encoder.h"

#define INVALID_RATIO 0.0001

struct cookie_logline{
    uint32_t values_offset;
    uint32_t num_values;
    uint32_t timestamp;
    uint32_t cookie_id;
};

static int compare(const void *p1, const void *p2)
{
    const struct cookie_logline *x = (const struct cookie_logline*)p1;
    const struct cookie_logline *y = (const struct cookie_logline*)p2;

    if (x->timestamp > y->timestamp)
        return 1;
    else if (x->timestamp < y->timestamp)
        return -1;
    return 0;
}

static void sort_loglines(struct cookie_logline *loglines,
                          const void *cookies,
                          uint32_t num_cookies,
                          uint32_t cookie_size,
                          uint32_t base_timestamp)
{
    uint32_t i;
    uint32_t idx = 0;
    uint32_t num_invalid = 0;

    for (i = 0; i < num_cookies; i++){
        const struct logline *line =
            ((const struct cookie*)(cookies + i * cookie_size))->last;
        uint32_t idx0 = idx;
        uint32_t prev_timestamp;
        int j;

        while (line){
            loglines[idx].cookie_id = i;
            loglines[idx].values_offset = line->values_offset;
            loglines[idx].num_values = line->num_values;
            loglines[idx].timestamp = line->timestamp;
            ++idx;
            line = line->prev;
        }

        qsort(&loglines[idx0],
              idx - idx0,
              sizeof(struct cookie_logline),
              compare);

        for (prev_timestamp = base_timestamp, j = idx0; j < idx; j++){
            uint32_t prev = loglines[j].timestamp;
            loglines[j].timestamp -= prev_timestamp;
            if (loglines[j].timestamp < (1 << 24))
                loglines[j].timestamp <<= 8;
            else{
                /* mark logline as invalid */
                loglines[j].timestamp = 1;
                ++num_invalid;
            }
            prev_timestamp = prev;
        }
    }

    if (num_invalid / (float)idx > INVALID_RATIO)
        DIE("Too many invalid timestamps (base timestamp: %u)\n",
            base_timestamp);
}

static Pvoid_t collect_value_freqs(const uint32_t *values,
                                   uint32_t num_values,
                                   const struct cookie_logline *loglines,
                                   uint32_t num_loglines)
{
    Pvoid_t freqs = NULL;
    uint32_t i;
    Word_t *ptr;

    for (i = 0; i < num_values; i++){
        JLI(ptr, freqs, values[i]);
        ++*ptr;
    }

    for (i = 0; i < num_loglines; i++)
        /* consider only valid timestamps (first byte = 0) */
        if ((loglines[i].timestamp & 255) == 0){
            JLI(ptr, freqs, loglines[i].timestamp);
            ++*ptr;
        }

    return freqs;
}

static uint32_t find_base_timestamp(const struct logline *loglines,
                                    uint32_t num_loglines)
{
    int i;
    uint32_t base_timestamp = UINT32_MAX;

    for (i = 0; i < num_loglines; i++)
        if (loglines[i].timestamp < base_timestamp)
            base_timestamp = loglines[i].timestamp;

    return base_timestamp;
}

void store_trails(const uint32_t *values,
                  uint32_t num_values,
                  const struct cookie *cookies,
                  uint32_t num_cookies,
                  uint32_t cookie_size,
                  const struct logline *loglines,
                  uint32_t num_loglines,
                  const char *path)
{
    struct cookie_logline *grouped;
    uint32_t base_timestamp = find_base_timestamp(loglines, num_loglines);
    Pvoid_t freqs;

    if (!(grouped = malloc(num_loglines * sizeof(struct cookie_logline))))
        DIE("Couldn't malloc loglines\n");

    sort_loglines(grouped, cookies, num_cookies, cookie_size, base_timestamp);
    freqs = collect_value_freqs(values, num_values, grouped, num_loglines);



    /* - create an array loglines[num_loglines]
       - create an array for cookies {offset, length}[num_cookies]
          - populate loglines and cookies arrays
          - each cookie gets consecutive list of loglines, sort by timestamp
          - convert timestamps to deltas per cookie
       - collect field value frequencies -> key_freqs
       - collect timestamp delta frequencies -> key_freqs
       - run huffman:create_codemap
       - write codebook to path.codebook
       - for each cookie
          - for each logline
            - huffman:encode_fields
          - write results to path.trails
    */

    free(grouped);
}

