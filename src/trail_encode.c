
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>

#include "ddb_bits.h"
#include "encode.h"
#include "huffman.h"
#include "util.h"

#define GROUPBUF_INCREMENT 10000000
#define READ_BUFFER_SIZE (1000000 * sizeof(struct cookie_logline))

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

static void group_loglines(FILE *grouped_w,
                           const char *path,
                           const struct logline *loglines,
                           const uint64_t *cookie_pointers,
                           uint64_t num_cookies,
                           uint32_t base_timestamp,
                           uint32_t *max_timestamp_delta)
{
    uint64_t i;
    uint64_t idx = 0;
    uint64_t num_invalid = 0;
    struct cookie_logline *buf = NULL;
    uint32_t buf_size = 0;

    *max_timestamp_delta = 0;

    for (i = 0; i < num_cookies; i++){
        /* find the last logline belonging to this cookie */
        const struct logline *line = &loglines[cookie_pointers[i]];
        uint32_t j = 0;
        uint32_t num_lines = 0;
        uint32_t prev_timestamp;

        /* loop through all loglines belonging to this cookie,
           following back-links */
        while (1){
            if (j >= buf_size){
                buf_size += GROUPBUF_INCREMENT;
                if (!(buf = realloc(buf,
                                    buf_size * sizeof(struct cookie_logline))))
                    DIE("Couldn't realloc group buffer of %u items\n",
                        buf_size);
            }
            buf[j].cookie_id = i;
            buf[j].values_offset = line->values_offset;
            buf[j].num_values = line->num_values;
            buf[j].timestamp = line->timestamp;
            ++j;
            if (line->prev_logline_idx)
                line = &loglines[line->prev_logline_idx - 1];
            else
                break;
        }
        num_lines = j;

        /* sort events of this cookie by time */
        qsort(buf, num_lines, sizeof(struct cookie_logline), compare);

        /* delta-encode timestamps */
        for (prev_timestamp = base_timestamp, j = 0; j < num_lines; j++){
            uint32_t prev = buf[j].timestamp;
            buf[j].timestamp -= prev_timestamp;
            /* timestamps can be at most 2**24 seconds apart (194 days).
               It is not a problem since data should be partitioned by time */
            if (buf[j].timestamp < (1 << 24)){

                if (buf[j].timestamp > *max_timestamp_delta)
                    *max_timestamp_delta = buf[j].timestamp;

                buf[j].timestamp <<= 8;
                prev_timestamp = prev;
            }else{
                /* mark logline as invalid if it is too far in the future,
                   most likely because of a corrupted timestamp */
                buf[j].timestamp = 1;
                ++num_invalid;
            }
        }

        SAFE_WRITE(buf,
                   num_lines * sizeof(struct cookie_logline),
                   path,
                   grouped_w);
    }

    if (num_invalid / (float)idx > INVALID_RATIO)
        DIE("Too many invalid timestamps (base timestamp: %u)\n",
            base_timestamp);

    free(buf);
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

static void timestamp_range(const struct logline *loglines,
                            uint64_t num_loglines,
                            uint32_t *min_timestamp,
                            uint32_t *max_timestamp)
{
    uint64_t i;
    *min_timestamp = UINT32_MAX;
    *max_timestamp = 0;

    for (i = 0; i < num_loglines; i++){
        if (loglines[i].timestamp < *min_timestamp)
            *min_timestamp = loglines[i].timestamp;
        if (loglines[i].timestamp > *max_timestamp)
            *max_timestamp = loglines[i].timestamp;
    }
}

static void store_info(uint64_t num_loglines,
                       uint64_t num_cookies,
                       uint32_t min_timestamp,
                       uint32_t max_timestamp,
                       uint32_t max_timestamp_delta,
                       const char *path)
{
    FILE *out;

    if (!(out = fopen(path, "w")))
        DIE("Could not create info file: %s\n", path);

    SAFE_FPRINTF(out,
                 path,
                 "%llu %llu %u %u %u\n",
                 (long long unsigned int)num_cookies,
                 (long long unsigned int)num_loglines,
                 min_timestamp,
                 max_timestamp,
                 max_timestamp_delta);

    SAFE_CLOSE(out, path);
}

static void encode_trails(const uint32_t *values,
                          FILE *grouped,
                          uint64_t num_loglines,
                          uint64_t num_cookies,
                          uint32_t num_fields,
                          const Pvoid_t codemap,
                          const Pvoid_t gram_freqs,
                          const struct field_stats *fstats,
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
    struct cookie_logline line;

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
        DIE("Could not allocate %u fields in edge_encode_values\n",
            num_fields);

    if (!(grams = malloc(num_fields * 8)))
        DIE("Could not allocate %u grams\n", num_fields);

    rewind(grouped);
    fread(&line, sizeof(struct cookie_logline), 1, grouped);

    while (i < num_loglines){
        /* encode trail for one cookie (multiple loglines) */

        /* reserve 3 bits in the head of the trail for a length residual:
           Length of a trail is measured in bytes but the last byte may
           be short. The residual indicates how many bits in the end we
           should ignore. */
        uint64_t offs = 3;
        uint64_t cookie_id = line.cookie_id;
        uint64_t trail_size;

        /* write offset to TOC */
        SAFE_SEEK(out, cookie_id * 4, path);
        SAFE_WRITE(&file_offs, 4, path, out);

        memset(prev_values, 0, num_fields * 4);

        for (;i < num_loglines && line.cookie_id == cookie_id; i++){

            /* 1) produce an edge-encoded set of values for this logline */
            uint32_t n = edge_encode_fields(values,
                                            &encoded,
                                            &encoded_size,
                                            prev_values,
                                            &line);

            /* 2) cover the encoded set with a set of unigrams and bigrams */
            uint32_t m = choose_grams(encoded,
                                      n,
                                      gram_freqs,
                                      &gbufs,
                                      grams,
                                      &line);

            /* 3) huffman-encode grams */
            huff_encode_grams(codemap,
                              grams,
                              m,
                              buf,
                              &offs,
                              fstats);

            fread(&line, sizeof(struct cookie_logline), 1, grouped);
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
                  struct logline *loglines,
                  uint64_t num_loglines,
                  const uint32_t *values,
                  uint64_t num_values,
                  uint32_t num_fields,
                  const uint64_t *field_cardinalities,
                  const char *root)
{
    char path[MAX_PATH_SIZE];
    char grouped_path[MAX_PATH_SIZE];
    struct field_stats *fstats;
    uint32_t min_timestamp, max_timestamp, max_timestamp_delta;
    Pvoid_t unigram_freqs;
    Pvoid_t gram_freqs;
    Pvoid_t codemap;
    Word_t tmp;
    FILE *grouped_w;
    FILE *grouped_r;
    char *read_buf;

    DDB_TIMER_DEF

    /* 1. find minimum timestamp (for delta-encoding) */
    DDB_TIMER_START
    timestamp_range(loglines, num_loglines, &min_timestamp, &max_timestamp);
    DDB_TIMER_END("trail/timestamp_range");

    /* 2. group loglines by cookie, sort events of each cookie by time,
          and delta-encode timestamps */
    DDB_TIMER_START

    make_path(grouped_path, "%s/tmp.grouped.%d", root, getpid());
    if (!(grouped_w = fopen(grouped_path, "w")))
        DIE("Could not open tmp file at %s\n", path);

    group_loglines(grouped_w,
                   grouped_path,
                   loglines,
                   cookie_pointers,
                   num_cookies,
                   min_timestamp,
                   &max_timestamp_delta);

    SAFE_CLOSE(grouped_w, grouped_path);
    if (!(grouped_r = fopen(grouped_path, "r")))
        DIE("Could not open tmp file at %s\n", path);
    if (!(read_buf = malloc(READ_BUFFER_SIZE)))
        DIE("Could not allocate read buffer of %lu bytes\n", READ_BUFFER_SIZE);
    setvbuf(grouped_r, read_buf, _IOFBF, READ_BUFFER_SIZE);

    DDB_TIMER_END("trail/group_loglines");

    /* not the most clean separation of ownership here, but loglines is huge
       so keeping it around unecessarily is expensive */
    free(loglines);

    /* 3. store metatadata */
    DDB_TIMER_START
    make_path(path, "%s/info", root);
    store_info(num_loglines,
               num_cookies,
               min_timestamp,
               max_timestamp,
               max_timestamp_delta,
               path);
    DDB_TIMER_END("trail/info");

    /* 4. collect value (unigram) frequencies, including delta-encoded
          timestamps */
    DDB_TIMER_START
    unigram_freqs = collect_unigrams(grouped_r, num_loglines, values, num_fields);
    DDB_TIMER_END("trail/collect_unigrams");

    /* 5. construct uni/bi-grams */
    DDB_TIMER_START
    gram_freqs = make_grams(grouped_r,
                            num_loglines,
                            values,
                            num_fields,
                            unigram_freqs);
    DDB_TIMER_END("trail/gram_freqs");

    /* 6. build a huffman codebook and stats struct for encoding grams */
    DDB_TIMER_START
    codemap = huff_create_codemap(gram_freqs);
    fstats = huff_field_stats(field_cardinalities,
                              num_fields,
                              max_timestamp_delta);
    DDB_TIMER_END("trail/huff_create_codemap");

    /* 7. encode and write trails to disk */
    DDB_TIMER_START
    make_path(path, "%s/trails.data", root);
    encode_trails(values,
                  grouped_r,
                  num_loglines,
                  num_cookies,
                  num_fields,
                  codemap,
                  gram_freqs,
                  fstats,
                  path);
    DDB_TIMER_END("trail/encode_trails");

    /* 8. write huffman codebook to disk */
    DDB_TIMER_START
    make_path(path, "%s/trails.codebook", root);
    store_codebook(codemap, path);
    DDB_TIMER_END("trail/store_codebook");

    JLFA(tmp, unigram_freqs);
    JLFA(tmp, gram_freqs);
    JLFA(tmp, codemap);

    fclose(grouped_r);
    unlink(grouped_path);

    free(read_buf);
    free(fstats);
}
