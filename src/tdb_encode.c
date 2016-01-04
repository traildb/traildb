#define _GNU_SOURCE /* mkostemp() */

#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>
#include <stdlib.h>
#include <string.h>

#include "tdb_internal.h"
#include "tdb_encode_model.h"
#include "tdb_huffman.h"
#include "tdb_error.h"
#include "tdb_io.h"

#include "judy_str_map.h"

#define EDGE_INCREMENT     1000000
#define GROUPBUF_INCREMENT 1000000
#define READ_BUFFER_SIZE  (1000000 * sizeof(tdb_event))

#define INITIAL_ENCODING_BUF_BITS 8 * 1024 * 1024

struct jm_fold_state{
    FILE *grouped_w;

    tdb_event *buf;
    uint64_t buf_size;

    uint64_t trail_id;
    const tdb_cons_event *events;
    const uint64_t min_timestamp;
    uint64_t max_timestamp;
    uint64_t max_timedelta;

    int ret;
};

static int compare(const void *p1, const void *p2)
{
    const tdb_event *x = (const tdb_event*)p1;
    const tdb_event *y = (const tdb_event*)p2;

    if (x->timestamp > y->timestamp)
        return 1;
    else if (x->timestamp < y->timestamp)
        return -1;
    return 0;
}

static void *groupby_uuid_handle_one_trail(
    __uint128_t uuid __attribute__((unused)),
    Word_t *value,
    void *state)
{
    struct jm_fold_state *s = (struct jm_fold_state*)state;
    /* find the last event belonging to this trail */
    const tdb_cons_event *ev = &s->events[*value - 1];
    uint64_t j = 0;
    uint64_t num_events = 0;
    int ret = 0;

    if (s->ret)
        return s;

    /* loop through all events belonging to this trail,
       following back-links */
    while (1){
        if (j >= s->buf_size){
            s->buf_size += GROUPBUF_INCREMENT;
            if (!(s->buf = realloc(s->buf, s->buf_size * sizeof(tdb_event)))){
                ret = TDB_ERR_NOMEM;
                goto done;
            }
        }
        s->buf[j].trail_id = s->trail_id;
        s->buf[j].item_zero = ev->item_zero;
        s->buf[j].num_items = ev->num_items;
        s->buf[j].timestamp = ev->timestamp;

        /* TODO write a test for an extra long (>2^32) trail */
        if (++j == TDB_MAX_TRAIL_LENGTH){
            ret = TDB_ERR_TRAIL_TOO_LONG;
            goto done;
        }

        if (ev->prev_event_idx)
            ev = &s->events[ev->prev_event_idx - 1];
        else
            break;
    }
    num_events = j;

    /* sort events of this trail by time */
    /* TODO make this stable sort */
    /* TODO this could really benefit from Timsort since raw data
       is often partially sorted */
    qsort(s->buf, num_events, sizeof(tdb_event), compare);

    /* delta-encode timestamps */
    uint64_t prev_timestamp = s->min_timestamp;
    for (j = 0; j < num_events; j++){
        uint64_t timestamp = s->buf[j].timestamp;
        uint64_t delta = timestamp - prev_timestamp;
        if (delta < TDB_MAX_TIMEDELTA){
            if (timestamp > s->max_timestamp)
                s->max_timestamp = timestamp;
            if (delta > s->max_timedelta)
                s->max_timedelta = delta;
            prev_timestamp = timestamp;
            /* convert the delta value to a proper item */
            s->buf[j].timestamp = tdb_make_item(0, delta);
        }else{
            ret = TDB_ERR_TIMESTAMP_TOO_LARGE;
            goto done;
        }
    }

    TDB_WRITE(s->grouped_w, s->buf, num_events * sizeof(tdb_event));
    ++s->trail_id;

done:
    s->ret = ret;
    return s;
}

static int groupby_uuid(FILE *grouped_w,
                        const tdb_cons_event *events,
                        tdb_cons *cons,
                        uint64_t *num_trails,
                        uint64_t *max_timestamp,
                        uint64_t *max_timedelta)
{
    struct jm_fold_state state = {
        .grouped_w = grouped_w,
        .events = events,
        .min_timestamp = cons->min_timestamp
    };

    /* we require (min_timestamp - 0 < TDB_MAX_TIMEDELTA) */
    if (cons->min_timestamp >= TDB_MAX_TIMEDELTA)
        return TDB_ERR_TIMESTAMP_TOO_LARGE;

    j128m_fold(&cons->trails, groupby_uuid_handle_one_trail, &state);

    *num_trails = state.trail_id;
    *max_timestamp = state.max_timestamp;
    *max_timedelta = state.max_timedelta;

    free(state.buf);
    return state.ret;
}

int edge_encode_items(const tdb_item *items,
                      tdb_item **encoded,
                      uint64_t *num_encoded,
                      uint64_t *encoded_size,
                      tdb_item *prev_items,
                      const tdb_event *ev)
{
    uint64_t n = 0;
    uint64_t j = ev->item_zero;
    /* edge encode items: keep only fields that are different from
       the previous event */
    for (; j < ev->item_zero + ev->num_items; j++){
        tdb_field field = tdb_item_field(items[j]);
        if (prev_items[field] != items[j]){
            if (n == *encoded_size){
                *encoded_size += EDGE_INCREMENT;
                if (!(*encoded = realloc(*encoded,
                                         *encoded_size * sizeof(tdb_item))))
                    return TDB_ERR_NOMEM;
            }
            (*encoded)[n++] = prev_items[field] = items[j];
        }
    }
    *num_encoded = n;
    return 0;
}

static int store_info(const char *path,
                      uint64_t num_trails,
                      uint64_t num_events,
                      uint64_t min_timestamp,
                      uint64_t max_timestamp,
                      uint64_t max_timedelta)
{
    FILE *out = NULL;
    int ret = 0;

    TDB_OPEN(out, path, "w");
    TDB_FPRINTF(out,
                "%"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64" %"PRIu64"\n",
                num_trails,
                num_events,
                min_timestamp,
                max_timestamp,
                max_timedelta);
done:
    TDB_CLOSE_FINAL(out);
    return ret;
}

static int encode_trails(const tdb_item *items,
                         FILE *grouped,
                         uint64_t num_events,
                         uint64_t num_trails,
                         uint64_t num_fields,
                         const struct judy_128_map *codemap,
                         const struct judy_128_map *gram_freqs,
                         const struct field_stats *fstats,
                         const char *path,
                         const char *toc_path)
{
    __uint128_t *grams = NULL;
    tdb_item *prev_items = NULL;
    uint64_t *encoded = NULL;
    uint64_t encoded_size = 0;
    uint64_t buf_size = INITIAL_ENCODING_BUF_BITS;
    uint64_t i = 1;
    char *buf = NULL;
    FILE *out = NULL;
    uint64_t file_offs = 0;
    uint64_t *toc = NULL;
    struct gram_bufs gbufs;
    tdb_event ev;
    int ret = 0;

    if ((ret = init_gram_bufs(&gbufs, num_fields)))
        goto done;

    TDB_OPEN(out, path, "w");

    if (!(buf = calloc(1, buf_size / 8 + 8))){
        ret = TDB_ERR_NOMEM;
        goto done;
    }
    if (!(prev_items = malloc(num_fields * sizeof(tdb_item)))){
        ret = TDB_ERR_NOMEM;
        goto done;
    }
    if (!(grams = malloc(num_fields * 16))){
        ret = TDB_ERR_NOMEM;
        goto done;
    }
    if (!(toc = malloc((num_trails + 1) * 8))){
        ret = TDB_ERR_NOMEM;
        goto done;
    }

    rewind(grouped);
    if (num_events)
        TDB_READ(grouped, &ev, sizeof(tdb_event));

    while (i <= num_events){
        /* encode trail for one UUID (multiple events) */

        /* reserve 3 bits in the head of the trail for a length residual:
           Length of a trail is measured in bytes but the last byte may
           be short. The residual indicates how many bits in the end we
           should ignore. */
        uint64_t offs = 3;
        uint64_t trail_id = ev.trail_id;
        uint64_t n, m, trail_size;

        toc[trail_id] = file_offs;
        memset(prev_items, 0, num_fields * sizeof(tdb_item));

        while (ev.trail_id == trail_id){

            /* 1) produce an edge-encoded set of items for this event */
            if ((ret = edge_encode_items(items,
                                         &encoded,
                                         &n,
                                         &encoded_size,
                                         prev_items,
                                         &ev)))
                goto done;

            /* 2) cover the encoded set with a set of unigrams and bigrams */
            if ((ret = choose_grams_one_event(encoded,
                                              n,
                                              gram_freqs,
                                              &gbufs,
                                              grams,
                                              &m,
                                              &ev)))
                goto done;

            uint64_t bits_needed = offs + huff_encoded_max_bits(m) + 64;
            if (bits_needed > buf_size){
                char *new_buf;
                buf_size = bits_needed * 2;
                if (!(new_buf = calloc(1, buf_size / 8 + 8))){
                    ret = TDB_ERR_NOMEM;
                    goto done;
                }
                memcpy(new_buf, buf, offs / 8 + 1);
                free(buf);
                buf = new_buf;
            }

            /* 3) huffman-encode grams */
            huff_encode_grams(codemap,
                              grams,
                              m,
                              buf,
                              &offs,
                              fstats);

            if (i++ < num_events){
                TDB_READ(grouped, &ev, sizeof(tdb_event));
            }else
                break;
        }

        /* write the length residual */
        if (offs & 7){
            trail_size = offs / 8 + 1;
            write_bits(buf, 0, 8 - (uint32_t)(offs & 7LLU));
        }else{
            trail_size = offs / 8;
        }

        /* append trail to the end of file */
        TDB_WRITE(out, buf, trail_size);

        file_offs += trail_size;
        memset(buf, 0, trail_size);

    }
    /* keep the redundant last offset in the TOC, so we can determine
       trail length with toc[i + 1] - toc[i]. */
    toc[num_trails] = file_offs;

    /* write an extra 8 null bytes: huffman may require up to 7 when reading */
    uint64_t zero = 0;
    TDB_WRITE(out, &zero, 8);
    file_offs += 8;
    TDB_CLOSE(out);

    TDB_OPEN(out, toc_path, "w");
    size_t offs_size = file_offs < UINT32_MAX ? 4 : 8;
    for (i = 0; i < num_trails + 1; i++)
        TDB_WRITE(out, &toc[i], offs_size);

done:
    TDB_CLOSE_FINAL(out);

    free_gram_bufs(&gbufs);
    free(grams);
    free(encoded);
    free(prev_items);
    free(buf);
    free(toc);

    return ret;
}

static int store_codebook(const struct judy_128_map *codemap, const char *path)
{
    FILE *out = NULL;
    uint32_t size;
    struct huff_codebook *book = huff_create_codebook(codemap, &size);
    int ret = 0;

    TDB_OPEN(out, path, "w");
    TDB_WRITE(out, book, size);

done:
    TDB_CLOSE_FINAL(out);
    free(book);
    return ret;
}

int tdb_encode(tdb_cons *cons, tdb_item *items)
{
    char path[TDB_MAX_PATH_SIZE];
    char grouped_path[TDB_MAX_PATH_SIZE];
    char toc_path[TDB_MAX_PATH_SIZE];
    char *root = cons->root;
    char *read_buf = NULL;
    struct field_stats *fstats = NULL;
    uint64_t num_trails = 0;
    uint64_t num_events = cons->events.next;
    uint64_t num_fields = cons->num_ofields + 1;
    uint64_t max_timestamp = 0;
    uint64_t max_timedelta = 0;
    uint64_t *field_cardinalities = NULL;
    uint64_t i;
    Pvoid_t unigram_freqs;
    struct judy_128_map gram_freqs;
    struct judy_128_map codemap;
    Word_t tmp;
    FILE *grouped_w = NULL;
    FILE *grouped_r = NULL;
    int fd, ret = 0;
    TDB_TIMER_DEF

    j128m_init(&gram_freqs);
    j128m_init(&codemap);

    if (!(field_cardinalities = calloc(cons->num_ofields, 8))){
        ret = TDB_ERR_NOMEM;
        goto done;
    }

    for (i = 0; i < cons->num_ofields; i++)
        field_cardinalities[i] = jsm_num_keys(&cons->lexicons[i]);

    /* 1. group events by trail, sort events of each trail by time,
          and delta-encode timestamps */
    TDB_TIMER_START

    TDB_PATH(grouped_path, "%s/tmp.grouped.XXXXXX", root);
    if ((fd = mkostemp(grouped_path, 0)) == -1){
        ret = TDB_ERR_IO_OPEN;
        goto done;
    }
    if (!(grouped_w = fdopen(fd, "w"))){
        ret = TDB_ERR_IO_OPEN;
        goto done;
    }

    if (cons->events.data)
        if ((ret = groupby_uuid(grouped_w,
                                (tdb_cons_event*)cons->events.data,
                                cons,
                                &num_trails,
                                &max_timestamp,
                                &max_timedelta)))
            goto done;

    /*
    not the most clean separation of ownership here, but these objects
    can be huge so keeping them around unecessarily is expensive
    */
    free(cons->events.data);
    cons->events.data = NULL;
    j128m_free(&cons->trails);

    TDB_CLOSE(grouped_w);
    grouped_w = NULL;

    TDB_OPEN(grouped_r, grouped_path, "r");
    if (!(read_buf = malloc(READ_BUFFER_SIZE))){
        ret = TDB_ERR_NOMEM;
        goto done;
    }

    setvbuf(grouped_r, read_buf, _IOFBF, READ_BUFFER_SIZE);
    TDB_TIMER_END("trail/groupby_uuid");

    /* 2. store metatadata */
    TDB_TIMER_START
    TDB_PATH(path, "%s/info", root);
    if ((ret = store_info(path,
                          num_trails,
                          num_events,
                          cons->min_timestamp,
                          max_timestamp,
                          max_timedelta)))
        goto done;
    TDB_TIMER_END("trail/info");

    /* 3. collect value (unigram) freqs, including delta-encoded timestamps */
    TDB_TIMER_START
    unigram_freqs = collect_unigrams(grouped_r, num_events, items, num_fields);
    TDB_TIMER_END("trail/collect_unigrams");

    /* 4. construct uni/bi-grams */
    TDB_TIMER_START
    if ((ret = make_grams(grouped_r,
                          num_events,
                          items,
                          num_fields,
                          unigram_freqs,
                          &gram_freqs)))
        goto done;
    TDB_TIMER_END("trail/gram_freqs");

    /* 5. build a huffman codebook and stats struct for encoding grams */
    TDB_TIMER_START
    if ((ret = huff_create_codemap(&gram_freqs, &codemap)))
        goto done;
    if (!(fstats = huff_field_stats(field_cardinalities,
                                    num_fields,
                                    max_timedelta))){
        ret = TDB_ERR_NOMEM;
        goto done;
    }
    TDB_TIMER_END("trail/huff_create_codemap");

    /* 6. encode and write trails to disk */
    TDB_TIMER_START
    TDB_PATH(path, "%s/trails.data", root);
    TDB_PATH(toc_path, "%s/trails.toc", root);
    if ((ret = encode_trails(items,
                             grouped_r,
                             num_events,
                             num_trails,
                             num_fields,
                             &codemap,
                             &gram_freqs,
                             fstats,
                             path,
                             toc_path)))
        goto done;
    TDB_TIMER_END("trail/encode_trails");

    /* 7. write huffman codebook to disk */
    TDB_TIMER_START
    tdb_path(path, "%s/trails.codebook", root);
    if ((ret = store_codebook(&codemap, path)))
        goto done;
    TDB_TIMER_END("trail/store_codebook");

done:
    TDB_CLOSE_FINAL(grouped_w);
    TDB_CLOSE_FINAL(grouped_r);
    j128m_free(&gram_freqs);
    j128m_free(&codemap);
    JLFA(tmp, unigram_freqs);

    unlink(grouped_path);

    free(field_cardinalities);
    free(read_buf);
    free(fstats);

    return ret;
}
