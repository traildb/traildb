
#include <Judy.h>

#include "ddb_profile.h"
#include "huffman.h"
#include "ddb_bits.h"
#include "util.h"

#include "breadcrumbs_encoder.h"

#define EDGE_INCREMENT 1000000

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
                           const uint32_t *cookie_pointers,
                           uint32_t num_cookies,
                           const struct logline *loglines,
                           uint32_t base_timestamp)
{
    uint32_t i;
    uint32_t idx = 0;
    uint32_t num_invalid = 0;

    for (i = 0; i < num_cookies; i++){
        const struct logline *line = &loglines[cookie_pointers[i]];
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

static uint32_t edge_encode_values(const uint32_t *values,
                                   uint32_t **encoded,
                                   uint32_t *encoded_size,
                                   uint32_t *prev_values,
                                   const struct cookie_logline *line)
{
    uint32_t n = 0;

    /* consider only valid timestamps (first byte = 0) */
    if ((line->timestamp & 255) == 0){
        uint32_t j = line->values_offset;

        for (; j < line->values_offset + line->num_values; j++){
            uint32_t field = values[j] & 255;

            if (prev_values[field] != values[j]){
                if (n == *encoded_size){
                    *encoded_size += EDGE_INCREMENT;
                    if (!(*encoded = realloc(*encoded, *encoded_size * 4)))
                        DIE("Could not allocate encoding buffer of %u items\n",
                            *encoded_size);
                }
                (*encoded)[n++] = prev_values[field] = values[j];
            }
        }
    }
    return n;
}

static Pvoid_t collect_value_freqs(const struct cookie_logline *grouped,
                                   uint32_t num_loglines,
                                   const uint32_t *values,
                                   uint32_t num_fields)
{
    uint32_t *prev_values = NULL;
    uint32_t *encoded = NULL;
    uint32_t encoded_size = 0;
    Pvoid_t freqs = NULL;
    uint32_t i = 0;
    Word_t *ptr;

    if (!(prev_values = malloc(num_fields * 4)))
        DIE("Could not allocated %u fields in edge_encode_values\n",
            num_fields);

    while (i < num_loglines){
        uint32_t cookie_id = grouped[i].cookie_id;

        memset(prev_values, 0, num_fields * 4);

        for (;i < num_loglines && grouped[i].cookie_id == cookie_id; i++){
            uint32_t n = edge_encode_values(values,
                                            &encoded,
                                            &encoded_size,
                                            prev_values,
                                            &grouped[i]);
            if (n > 0){
                while (n--){
                    JLI(ptr, freqs, encoded[n]);
                    ++*ptr;
                }
                JLI(ptr, freqs, grouped[i].timestamp);
                ++*ptr;
            }
        }
    }

    free(encoded);
    free(prev_values);
    return freqs;
}

static void info(const struct logline *loglines,
                 uint32_t num_loglines,
                 uint32_t num_cookies,
                 uint32_t *min_timestamp,
                 uint32_t *max_timestamp,
                 const char *path)
{
    int i;
    FILE *out;

    *min_timestamp = UINT32_MAX;
    *max_timestamp = 0;

    for (i = 0; i < num_loglines; i++){
        if (loglines[i].timestamp < *min_timestamp)
            *min_timestamp = loglines[i].timestamp;
        if (loglines[i].timestamp > *max_timestamp)
            *max_timestamp = loglines[i].timestamp;
    }

    if (!(out = fopen(path, "w")))
        DIE("Could not create info file: %s\n", path);

    SAFE_FPRINTF(out,
                 path,
                 "%u %u %u %u\n",
                 num_cookies,
                 num_loglines,
                 *min_timestamp,
                 *max_timestamp);

    SAFE_CLOSE(out, path);
}

static void encode_trails(const uint32_t *values,
                          const struct cookie_logline *grouped,
                          uint32_t num_loglines,
                          uint32_t num_cookies,
                          uint32_t num_fields,
                          const Pvoid_t codemap,
                          const char *path)
{
    uint32_t *prev_values = NULL;
    uint32_t *encoded = NULL;
    uint32_t encoded_size = 0;
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

    if (!(prev_values = malloc(num_fields * 4)))
        DIE("Could not allocated %u fields in edge_encode_values\n",
            num_fields);

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

        /*
        if (grouped[i].cookie_id == 42859){
            int k, j = i;
            uint32_t tt = 1399395599;
            while (j < num_loglines && grouped[j].cookie_id == cookie_id){
                tt += grouped[j].timestamp >> 8;
                printf("timest %u (%u):", tt, grouped[j].num_values);
                for (k = 0; k < grouped[j].num_values; k++)
                    printf(" %u", values[grouped[j].values_offset + k]);
                printf("\n");
                ++j;
            }
        }
        */
        memset(prev_values, 0, num_fields * 4);

        for (;i < num_loglines && grouped[i].cookie_id == cookie_id; i++){
            uint32_t n = edge_encode_values(values,
                                            &encoded,
                                            &encoded_size,
                                            prev_values,
                                            &grouped[i]);
            huff_encode_values(codemap,
                               grouped[i].timestamp,
                               encoded,
                               n,
                               buf,
                               &offs);
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

    SAFE_CLOSE(out, path);

    free(encoded);
    free(prev_values);
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
    SAFE_CLOSE(out, path);
}

void store_trails(const uint32_t *cookie_pointers,
                  uint32_t num_cookies,
                  const struct logline *loglines,
                  uint32_t num_loglines,
                  const uint32_t *values,
                  uint32_t num_values,
                  uint32_t num_fields,
                  const char *root)
{
    char path[MAX_PATH_SIZE];
    struct cookie_logline *grouped;
    uint32_t min_timestamp, max_timestamp;
    Pvoid_t freqs;
    Pvoid_t codemap;
    Word_t tmp;

    DDB_TIMER_DEF

    if (!(grouped = malloc(num_loglines * sizeof(struct cookie_logline))))
        DIE("Couldn't malloc loglines\n");

    /* 1. find minimum timestamp (for delta-encoding) */
    DDB_TIMER_START
    make_path(path, "%s/info", root);
    info(loglines,
         num_loglines,
         num_cookies,
         &min_timestamp,
         &max_timestamp,
         path);
    DDB_TIMER_END("trail/info");

    /* 2. group loglines by cookie, sort events of each cookie by time,
          and delta-encode timestamps */
    DDB_TIMER_START
    group_loglines(grouped,
                   cookie_pointers,
                   num_cookies,
                   loglines,
                   min_timestamp);
    DDB_TIMER_END("trail/group_loglines");

    /* 3. collect value frequencies, including delta-encoded timestamps */
    DDB_TIMER_START
    freqs = collect_value_freqs(grouped, num_loglines, values, num_fields);
    DDB_TIMER_END("trail/collect_value_freqs");

    /* 4. build a huffman codebook for values */
    DDB_TIMER_START
    codemap = huff_create_codemap(freqs);
    DDB_TIMER_END("trail/huff_create_codemap");

    /* 5. encode and write trails to disk */
    DDB_TIMER_START
    make_path(path, "%s/trails.data", root);
    encode_trails(values,
                  grouped,
                  num_loglines,
                  num_cookies,
                  num_fields,
                  codemap,
                  path);
    DDB_TIMER_END("trail/encode_trails");

    /* 6. write huffman codebook to disk */
    DDB_TIMER_START
    make_path(path, "%s/trails.codebook", root);
    store_codebook(codemap, path);
    DDB_TIMER_END("trail/store_codebook");

    JLFA(tmp, freqs);
    JLFA(tmp, codemap);
    free(grouped);
}

