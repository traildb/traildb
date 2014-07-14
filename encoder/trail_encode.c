
#include <Judy.h>

#include "ddb_profile.h"
#include "huffman.h"
#include "ddb_bits.h"
#include "util.h"

#include "breadcrumbs_encoder.h"
#include "trail_encode.h"

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
                           const uint64_t *cookie_pointers,
                           uint64_t num_cookies,
                           const struct logline *loglines,
                           uint32_t base_timestamp)
{
    uint64_t i;
    uint64_t idx = 0;
    uint64_t num_invalid = 0;

    for (i = 0; i < num_cookies; i++){
        /* find the last logline belonging to this cookie */
        const struct logline *line = &loglines[cookie_pointers[i]];
        uint64_t j, idx0 = idx;
        uint32_t prev_timestamp;

        /* loop through all loglines belonging to this cookie,
           following back-links */
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

        /* sort events of this cookie by time */
        qsort(&grouped[idx0],
              idx - idx0,
              sizeof(struct cookie_logline),
              compare);

        /* delta-encode timestamps */
        for (prev_timestamp = base_timestamp, j = idx0; j < idx; j++){
            uint32_t prev = grouped[j].timestamp;
            grouped[j].timestamp -= prev_timestamp;
            /* timestamps can be at most 2**24 seconds apart (194 days).
               It is not a problem since data should be partitioned by time */
            if (grouped[j].timestamp < (1 << 24)){
                grouped[j].timestamp <<= 8;
                prev_timestamp = prev;
            }else{
                /* mark logline as invalid if it is too far in the future,
                   most likely because of a corrupted timestamp */
                grouped[j].timestamp = 1;
                ++num_invalid;
            }
        }
    }

    if (num_invalid / (float)idx > INVALID_RATIO)
        DIE("Too many invalid timestamps (base timestamp: %u)\n",
            base_timestamp);
}

uint32_t edge_encode_fields(const uint32_t *values,
                            uint32_t **encoded,
                            uint32_t *encoded_size,
                            uint32_t *prev_values,
                            const struct cookie_logline *line)
{
    uint32_t n = 0;

    /* consider only valid timestamps (first byte = 0) */
    if ((line->timestamp & 255) == 0){
        uint64_t j = line->values_offset;

        /* edge encode values: keep only fields that are different from
           the previous logline */
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

static void info(const struct logline *loglines,
                 uint64_t num_loglines,
                 uint64_t num_cookies,
                 uint32_t *min_timestamp,
                 uint32_t *max_timestamp,
                 const char *path)
{
    uint64_t i;
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
                 "%llu %llu %u %u\n",
                 (long long unsigned int)num_cookies,
                 (long long unsigned int)num_loglines,
                 *min_timestamp,
                 *max_timestamp);

    SAFE_CLOSE(out, path);
}

static void encode_trails(const uint32_t *values,
                          const struct cookie_logline *grouped,
                          uint64_t num_loglines,
                          uint64_t num_cookies,
                          uint32_t num_fields,
                          const Pvoid_t codemap,
                          const Pvoid_t gram_freqs,
                          const char *path)
{
    uint64_t *grams = NULL;
    uint32_t *prev_values = NULL;
    uint32_t *encoded = NULL;
    uint32_t encoded_size = 0;
    uint64_t i = 0;
    char *buf;
    FILE *out;
    uint64_t file_offs = (num_cookies + 1) * 4;
    struct gram_bufs gbufs;

    init_gram_bufs(&gbufs, num_fields);

    if (file_offs >= UINT32_MAX)
        DIE("Trail file %s over 4GB!\n", path);

    if (!(out = fopen(path, "wx")))
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

    if (!(grams = malloc(num_fields * 8)))
        DIE("Could not allocate %u grams\n", num_fields);

    while (i < num_loglines){
        /* encode trail for one cookie (multiple loglines) */

        /* reserve 3 bits in the head of the trail for a length residual:
           Length of a trail is measured in bytes but the last byte may
           be short. The residual indicates how many bits in the end we
           should ignore. */
        uint64_t offs = 3;
        uint64_t cookie_id = grouped[i].cookie_id;
        uint64_t trail_size;

        /* write offset to TOC */
        SAFE_SEEK(out, cookie_id * 4, path);
        SAFE_WRITE(&file_offs, 4, path, out);

        memset(prev_values, 0, num_fields * 4);

        for (;i < num_loglines && grouped[i].cookie_id == cookie_id; i++){

            /* 1) produce an edge-encoded set of values for this logline */
            uint32_t n = edge_encode_fields(values,
                                            &encoded,
                                            &encoded_size,
                                            prev_values,
                                            &grouped[i]);

            /* 2) cover the encoded set with a set of unigrams and bigrams */
            uint32_t m = choose_grams(encoded,
                                      n,
                                      gram_freqs,
                                      &gbufs,
                                      grams);

            /* 3) huffman-encode grams */
            huff_encode_grams(codemap,
                              grouped[i].timestamp,
                              grams,
                              m,
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

        SAFE_SEEK(out, file_offs, path);
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

    free_gram_bufs(&gbufs);
    free(grams);
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

void store_trails(const uint64_t *cookie_pointers,
                  uint64_t num_cookies,
                  const struct logline *loglines,
                  uint64_t num_loglines,
                  const uint32_t *values,
                  uint64_t num_values,
                  uint32_t num_fields,
                  const char *root)
{
    char path[MAX_PATH_SIZE];
    struct cookie_logline *grouped;
    uint32_t min_timestamp, max_timestamp;
    Pvoid_t unigram_freqs;
    Pvoid_t gram_freqs;
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

    /* 3. collect value (unigram) frequencies, including delta-encoded
          timestamps */
    DDB_TIMER_START
    unigram_freqs = collect_unigrams(grouped, num_loglines, values, num_fields);
    DDB_TIMER_END("trail/collect_unigrams");

    /* 4. construct uni/bi-grams */
    DDB_TIMER_START
    gram_freqs = make_grams(grouped,
                            num_loglines,
                            values,
                            num_fields,
                            unigram_freqs);
    DDB_TIMER_END("trail/gram_freqs");

    /* 5. build a huffman codebook for grams */
    DDB_TIMER_START
    codemap = huff_create_codemap(gram_freqs);
    DDB_TIMER_END("trail/huff_create_codemap");

    /* 6. encode and write trails to disk */
    DDB_TIMER_START
    make_path(path, "%s/trails.data", root);
    encode_trails(values,
                  grouped,
                  num_loglines,
                  num_cookies,
                  num_fields,
                  codemap,
                  gram_freqs,
                  path);
    DDB_TIMER_END("trail/encode_trails");

    /* 7. write huffman codebook to disk */
    DDB_TIMER_START
    make_path(path, "%s/trails.codebook", root);
    store_codebook(codemap, path);
    DDB_TIMER_END("trail/store_codebook");

    JLFA(tmp, unigram_freqs);
    JLFA(tmp, gram_freqs);
    JLFA(tmp, codemap);
    free(grouped);
}

