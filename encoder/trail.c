
#include <Judy.h>

#include "ddb_profile.h"
#include "huffman.h"
#include "ddb_bits.h"

#include "breadcrumbs_encoder.h"

#define INVALID_RATIO 0.0001
#define REALPATH_SIZE (MAX_PATH_SIZE + 32)

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

static void group_loglines(struct cookie_logline *grouped,
                           const struct logline *loglines,
                           const void *cookies,
                           uint32_t num_cookies,
                           uint32_t cookie_size,
                           uint32_t base_timestamp)
{
    uint32_t i;
    uint32_t idx = 0;
    uint32_t num_invalid = 0;

    for (i = 0; i < num_cookies; i++){
        const struct cookie *cookie =
            (const struct cookie*)(cookies + i * (uint64_t)cookie_size);
        const struct logline *line = &loglines[cookie->last_logline_idx - 1];
        uint32_t idx0 = idx;
        uint32_t prev_timestamp;
        int j;

        while (1){
            grouped[idx].cookie_id = i;
            grouped[idx].values_offset = line->values_offset;
            grouped[idx].num_values = line->num_values;
            grouped[idx].timestamp = line->timestamp;
            ++idx;
            if (line->prev_logline_idx)
                line = &loglines[line->prev_logline_idx - 1];
            else
                break;
        }

        /* sort events of a cookie by time */
        qsort(&grouped[idx0],
              idx - idx0,
              sizeof(struct cookie_logline),
              compare);

        /* delta-encode timestamps */
        for (prev_timestamp = base_timestamp, j = idx0; j < idx; j++){
            uint32_t prev = grouped[j].timestamp;
            grouped[j].timestamp -= prev_timestamp;
            if (grouped[j].timestamp < (1 << 24))
                grouped[j].timestamp <<= 8;
            else{
                /* mark logline as invalid */
                grouped[j].timestamp = 1;
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

static void encode_trails(const uint32_t *values,
                          const struct cookie_logline *grouped,
                          uint32_t num_loglines,
                          uint32_t num_cookies,
                          const Pvoid_t codemap,
                          const char *path)
{
    uint32_t i = 0;
    char *buf;
    FILE *out;
    uint64_t file_offs = (num_cookies + 1) * 4;

    if (!(out = fopen(path, "w")))
        DIE("Could not create trail file: %s\n", path);

    /* reserve space for TOC */
    SAFE_SEEK(out, file_offs, path);

    /* huff_encode_values guarantees that it writes fewer
       than UINT32_MAX bits per buffer, or it fails */
    if (!(buf = calloc(1, UINT32_MAX / 8 + 8)))
        DIE("Could not allocate 512MB in encode_trails\n");

    while (i < num_loglines){
        /* encode trail for one cookie (multiple loglines) */

        /* reserve 3 bits in the head of the trail for a length residual:
           Length of a trail is measured in bytes but the last byte may
           be short. The residual indicates how many bits in the end we
           should ignore. */
        uint64_t offs = 3;
        uint32_t cookie_id = grouped[i].cookie_id;
        uint32_t trail_size;

        /* write offset to TOC */
        SAFE_SEEK(out, cookie_id * 4, path);
        SAFE_WRITE(&file_offs, 4, path, out);

        while (i < num_loglines && grouped[i].cookie_id == cookie_id){
            huff_encode_values(codemap,
                               grouped[i].timestamp,
                               &values[grouped[i].values_offset],
                               grouped[i].num_values,
                               buf,
                               &offs);
            ++i;
        }
        /* write the length residual */
        if (offs & 7){
            trail_size = offs / 8 + 1;
            write_bits(buf, 0, 8 - (offs & 7));
        }else
            trail_size = offs / 8;

        /* append trail to the end of file */
        if (fseek(out, 0, SEEK_END) == -1)
            DIE("Seeking to the end of %s failed\n", path);
        SAFE_WRITE(buf, trail_size, path, out);

        file_offs += trail_size;
        if (file_offs >= UINT32_MAX)
            DIE("Trail file %s over 4GB!\n", path);

        memset(buf, 0, trail_size);
    }
    /* write the redundant last offset in the TOC, so we can determine
       trail length with toc[i + 1] - toc[i]. */
    SAFE_SEEK(out, num_cookies * 4, path);
    SAFE_WRITE(&file_offs, 4, path, out);

    fclose(out);
}

static void store_codebook(const Pvoid_t codemap, const char *path)
{
    FILE *out;
    uint32_t size;
    struct huff_codebook *book = huff_create_codebook(codemap, &size);

    if (!(out = fopen(path, "w")))
        DIE("Could not create codebook file: %s\n", path);

    SAFE_WRITE(book, size, path, out);

    free(book);
    fclose(out);
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
    char realpath[REALPATH_SIZE];
    struct cookie_logline *grouped;
    uint32_t base_timestamp;
    Pvoid_t freqs;
    Pvoid_t codemap;
    Word_t tmp;

    DDB_TIMER_DEF

    if (!(grouped = malloc(num_loglines * sizeof(struct cookie_logline))))
        DIE("Couldn't malloc loglines\n");

    /* 1. find minimum timestamp (for delta-encoding) */
    DDB_TIMER_START
    base_timestamp = find_base_timestamp(loglines, num_loglines);
    DDB_TIMER_END("trail/find_base_timestamp");

    /* 2. group loglines by cookie, sort events of each cookie by time,
          and delta-encode timestamps */
    DDB_TIMER_START
    group_loglines(grouped,
                   loglines,
                   cookies,
                   num_cookies,
                   cookie_size,
                   base_timestamp);
    DDB_TIMER_END("trail/group_loglines");

    /* 3. collect value frequencies, including delta-encoded timestamps */
    DDB_TIMER_START
    freqs = collect_value_freqs(values, num_values, grouped, num_loglines);
    DDB_TIMER_END("trail/collect_value_freqs");

    /* 4. build a huffman codebook for values */
    DDB_TIMER_START
    codemap = huff_create_codemap(freqs);
    DDB_TIMER_END("trail/huff_create_codemap");

    /* 5. encode and write trails to disk */
    DDB_TIMER_START
    if (snprintf(realpath, REALPATH_SIZE, "%s.data", path) >= REALPATH_SIZE)
        DIE("Trail path too long (%s.data)!\n", path);
    encode_trails(values,
                  grouped,
                  num_loglines,
                  num_cookies,
                  codemap,
                  realpath);
    DDB_TIMER_END("trail/encode_trails");

    /* 6. write huffman codebook to disk */
    DDB_TIMER_START
    if (snprintf(realpath, REALPATH_SIZE, "%s.codebook", path) >= REALPATH_SIZE)
        DIE("Trail path too long (%s.codebook)!\n", path);
    store_codebook(codemap, realpath);
    DDB_TIMER_END("trail/store_codebook");

    JLFA(tmp, freqs);
    JLFA(tmp, codemap);
    free(grouped);
}

