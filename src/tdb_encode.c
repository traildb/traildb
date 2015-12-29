
#include <fcntl.h>
#include <unistd.h>
#include <sys/types.h>

#include "tdb_internal.h"
#include "tdb_encode_model.h"
#include "huffman.h"
#include "util.h"
#include "judy_str_map.h"

#define EDGE_INCREMENT     1000000
#define GROUPBUF_INCREMENT 10000000
#define READ_BUFFER_SIZE  (1000000 * sizeof(tdb_event))

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

struct jm_fold_state{
    FILE *grouped_w;
    const char *path;

    tdb_event *buf;
    uint64_t buf_size;

    uint64_t trail_id;
    const tdb_cons_event *events;
    const uint32_t min_timestamp;
    uint32_t max_timestamp;
    uint32_t max_timedelta;

    uint64_t num_invalid;
    int ret;
};

static void *groupby_uuid_handle_one_trail(
    __uint128_t uuid __attribute__((unused)),
    Word_t *value,
    void *state)
{
    struct jm_fold_state *s = (struct jm_fold_state*)state;
    /* find the last event belonging to this trail */
    const tdb_cons_event *ev = &s->events[*value - 1];
    uint32_t j = 0;
    uint32_t num_events = 0;

    /* if any of the trails fail, all fail, so it is pointless to continue */
    if (s->ret)
        return state;

    /* loop through all events belonging to this trail,
       following back-links */
    while (1){
        if (j >= s->buf_size){
            s->buf_size += GROUPBUF_INCREMENT;
            if (!(s->buf = realloc(s->buf, s->buf_size * sizeof(tdb_event))))
                DIE("Couldn't realloc group buffer of %"PRIu64" items",
                    s->buf_size);
        }
        s->buf[j].trail_id = s->trail_id;
        s->buf[j].item_zero = ev->item_zero;
        s->buf[j].num_items = ev->num_items;
        s->buf[j].timestamp = ev->timestamp;

        if (++j == TDB_MAX_TRAIL_LENGTH){
            s->ret = -1;
            return state;
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
                s->max_timestamp = (uint32_t)timestamp;
            if (delta > s->max_timedelta)
                s->max_timedelta = (uint32_t)delta;

            /* Convert to the delta value index */
            prev_timestamp = timestamp;
            s->buf[j].timestamp = (uint32_t)(delta << 8);
        }else{
            /* Use the out of range delta, perhaps a corrupted timestamp */
            s->max_timedelta = TDB_FAR_TIMEDELTA;
            s->buf[j].timestamp = TDB_FAR_TIMEDELTA << 8;
            ++s->num_invalid;
        }
    }

    SAFE_WRITE(s->buf, num_events * sizeof(tdb_event), s->path, s->grouped_w);
    ++s->trail_id;
    return state;
}

static int groupby_uuid(FILE *grouped_w,
                        const char *path,
                        const tdb_cons_event *events,
                        tdb_cons *cons,
                        uint64_t *num_trails,
                        uint32_t *max_timestamp,
                        uint32_t *max_timedelta)
{
    struct jm_fold_state state = {
        .grouped_w = grouped_w,
        .events = events,
        .path = path,
        .min_timestamp = cons->min_timestamp
    };

    j128m_fold(&cons->trails, groupby_uuid_handle_one_trail, &state);

    *num_trails = state.trail_id;
    *max_timestamp = state.max_timestamp;
    *max_timedelta = state.max_timedelta;

    free(state.buf);
    return state.ret;
}

uint32_t edge_encode_items(const tdb_item *items,
                           uint32_t **encoded,
                           uint32_t *encoded_size,
                           tdb_item *prev_items,
                           const tdb_event *ev)
{
    uint32_t n = 0;
    /* consider only valid timestamps (field == 0)
       XXX: use invalid timestamps again when we add
            the flag in finalize to skip OOD data */
    if (tdb_item_field(ev->timestamp) == 0){
        uint64_t j = ev->item_zero;
        /* edge encode items: keep only fields that are different from
           the previous event */
        for (; j < ev->item_zero + ev->num_items; j++){
            tdb_field field = tdb_item_field(items[j]);
            if (prev_items[field] != items[j]){
                if (n == *encoded_size){
                    *encoded_size += EDGE_INCREMENT;
                    if (!(*encoded = realloc(*encoded, *encoded_size * 4)))
                        DIE("Could not allocate encoding buffer of %u items",
                            *encoded_size);
                }
                (*encoded)[n++] = prev_items[field] = items[j];
            }
        }
    }
    return n;
}

static void store_info(const char *path,
                       uint64_t num_trails,
                       uint64_t num_events,
                       uint32_t min_timestamp,
                       uint32_t max_timestamp,
                       uint32_t max_timedelta)
{
    FILE *out;

    if (!(out = fopen(path, "w")))
        DIE("Could not create info file: %s", path);

    SAFE_FPRINTF(out,
                 path,
                 "%"PRIu64" %"PRIu64" %"PRIu32" %"PRIu32" %"PRIu32"\n",
                 num_trails,
                 num_events,
                 min_timestamp,
                 max_timestamp,
                 max_timedelta);

    SAFE_CLOSE(out, path);
}

static void encode_trails(const tdb_item *items,
                          FILE *grouped,
                          uint64_t num_events,
                          uint64_t num_trails,
                          uint32_t num_fields,
                          const Pvoid_t codemap,
                          const Pvoid_t gram_freqs,
                          const struct field_stats *fstats,
                          const char *path,
                          const char *toc_path)
{
    uint64_t *grams = NULL;
    tdb_item *prev_items = NULL;
    uint32_t *encoded = NULL;
    uint32_t encoded_size = 0;
    uint64_t i = 1;
    char *buf;
    FILE *out;
    uint64_t file_offs = 0, *toc;
    struct gram_bufs gbufs;
    tdb_event ev;

    init_gram_bufs(&gbufs, num_fields);

    if (!(out = fopen(path, "w")))
        DIE("Could not create trail file: %s", path);

    /* huff_encode_grams guarantees that it writes fewer
       than UINT32_MAX bits per buffer, or it fails */
    if (!(buf = calloc(1, UINT32_MAX / 8 + 8)))
        DIE("Could not allocate 512MB in encode_trails");

    if (!(prev_items = malloc(num_fields * sizeof(tdb_item))))
        DIE("Could not allocate %u fields", num_fields);

    if (!(grams = malloc(num_fields * 8)))
        DIE("Could not allocate %u grams", num_fields);

    if (!(toc = malloc((num_trails + 1) * 8)))
        DIE("Could not allocate %"PRIu64" offsets", num_trails + 1);

    rewind(grouped);
    if (num_events)
        SAFE_FREAD(grouped, path, &ev, sizeof(tdb_event));

    while (i <= num_events){
        /* encode trail for one UUID (multiple events) */

        /* reserve 3 bits in the head of the trail for a length residual:
           Length of a trail is measured in bytes but the last byte may
           be short. The residual indicates how many bits in the end we
           should ignore. */
        uint64_t offs = 3;
        uint64_t trail_id = ev.trail_id;
        uint64_t trail_size;

        toc[trail_id] = file_offs;
        memset(prev_items, 0, num_fields * sizeof(tdb_item));

        while (ev.trail_id == trail_id){

            /* 1) produce an edge-encoded set of items for this event */
            uint32_t n = edge_encode_items(items,
                                           &encoded,
                                           &encoded_size,
                                           prev_items,
                                           &ev);

            /* 2) cover the encoded set with a set of unigrams and bigrams */
            uint32_t m = (uint32_t)choose_grams(encoded,
                                                n,
                                                gram_freqs,
                                                &gbufs,
                                                grams,
                                                &ev);

            /* 3) huffman-encode grams */
            huff_encode_grams(codemap,
                              grams,
                              m,
                              buf,
                              &offs,
                              fstats);

            if (i++ < num_events) {
                SAFE_FREAD(grouped, path, &ev, sizeof(tdb_event));
            } else {
                break;
            }

        }

        /* write the length residual */
        if (offs & 7){
            trail_size = offs / 8 + 1;
            write_bits(buf, 0, 8 - (uint32_t)(offs & 7LLU));
        }else{
            trail_size = offs / 8;
        }

        /* append trail to the end of file */
        SAFE_WRITE(buf, trail_size, path, out);

        file_offs += trail_size;
        memset(buf, 0, trail_size);

    }
    /* keep the redundant last offset in the TOC, so we can determine
       trail length with toc[i + 1] - toc[i]. */
    toc[num_trails] = file_offs;

    /* write an extra 8 null bytes: huffman may require up to 7 when reading */
    uint64_t zero = 0;
    SAFE_WRITE(&zero, 8, path, out);
    SAFE_CLOSE(out, path);

    if (!(out = fopen(toc_path, "w")))
        DIE("Could not create trail TOC: %s", toc_path);
    size_t offs_size = file_offs < UINT32_MAX ? 4 : 8;
    for (i = 0; i < num_trails + 1; i++)
        SAFE_WRITE(&toc[i], offs_size, toc_path, out);
    SAFE_CLOSE(out, toc_path);

    free_gram_bufs(&gbufs);
    free(grams);
    free(encoded);
    free(prev_items);
    free(buf);
    free(toc);
}

static void store_codebook(const Pvoid_t codemap, const char *path)
{
    FILE *out;
    uint32_t size;
    struct huff_codebook *book = huff_create_codebook(codemap, &size);

    if (!(out = fopen(path, "w")))
        DIE("Could not create codebook file: %s", path);

    SAFE_WRITE(book, size, path, out);

    free(book);
    SAFE_CLOSE(out, path);
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
    uint32_t num_fields = cons->num_ofields + 1;
    uint32_t max_timestamp = 0;
    uint32_t max_timedelta = 0;
    uint64_t *field_cardinalities = NULL;
    uint32_t i;
    Pvoid_t unigram_freqs = NULL;
    Pvoid_t gram_freqs = NULL;
    Pvoid_t codemap = NULL;
    Word_t tmp;
    FILE *grouped_w = NULL;
    FILE *grouped_r = NULL;
    int ret = 0;
    TDB_TIMER_DEF

    if (!(field_cardinalities = calloc(cons->num_ofields, 8)))
        DIE("Couldn't malloc field_cardinalities");

    /* TODO: this wouldn't include OVERFLOW_VALUE */
    for (i = 0; i < cons->num_ofields; i++)
        field_cardinalities[i] = jsm_num_keys(&cons->lexicons[i]);

    /* 1. group events by trail, sort events of each trail by time,
          and delta-encode timestamps */
    TDB_TIMER_START

    tdb_path(grouped_path, "%s/tmp.grouped.%d", root, getpid());
    if (!(grouped_w = fopen(grouped_path, "w")))
        DIE("Could not open tmp file at %s", path);

    if (cons->events.data)
        if ((ret = groupby_uuid(grouped_w,
                                grouped_path,
                                (tdb_cons_event*)cons->events.data,
                                cons,
                                &num_trails,
                                &max_timestamp,
                                &max_timedelta)))
            goto error;

    /*
    not the most clean separation of ownership here, but these objects
    can be huge so keeping them around unecessarily is expensive
    */
    free(cons->events.data);
    cons->events.data = NULL;
    j128m_free(&cons->trails);

    SAFE_CLOSE(grouped_w, grouped_path);
    if (!(grouped_r = fopen(grouped_path, "r")))
        DIE("Could not open tmp file at %s", path);
    if (!(read_buf = malloc(READ_BUFFER_SIZE)))
        DIE("Could not allocate read buffer of %lu bytes", READ_BUFFER_SIZE);
    setvbuf(grouped_r, read_buf, _IOFBF, READ_BUFFER_SIZE);
    TDB_TIMER_END("trail/groupby_uuid");

    /* 2. store metatadata */
    TDB_TIMER_START
    tdb_path(path, "%s/info", root);
    store_info(path,
               num_trails,
               num_events,
               cons->min_timestamp,
               max_timestamp,
               max_timedelta);
    TDB_TIMER_END("trail/info");

    /* 3. collect value (unigram) freqs, including delta-encoded timestamps */
    TDB_TIMER_START
    unigram_freqs = collect_unigrams(grouped_r, num_events, items, num_fields);
    TDB_TIMER_END("trail/collect_unigrams");

    /* 4. construct uni/bi-grams */
    TDB_TIMER_START
    gram_freqs = make_grams(grouped_r,
                            num_events,
                            items,
                            num_fields,
                            unigram_freqs);
    TDB_TIMER_END("trail/gram_freqs");

    /* 5. build a huffman codebook and stats struct for encoding grams */
    TDB_TIMER_START
    codemap = huff_create_codemap(gram_freqs);
    fstats = huff_field_stats(field_cardinalities,
                              num_fields,
                              max_timedelta);
    TDB_TIMER_END("trail/huff_create_codemap");

    /* 6. encode and write trails to disk */
    TDB_TIMER_START
    tdb_path(path, "%s/trails.data", root);
    tdb_path(toc_path, "%s/trails.toc", root);
    encode_trails(items,
                  grouped_r,
                  num_events,
                  num_trails,
                  num_fields,
                  codemap,
                  gram_freqs,
                  fstats,
                  path,
                  toc_path);
    TDB_TIMER_END("trail/encode_trails");

    /* 7. write huffman codebook to disk */
    TDB_TIMER_START
    tdb_path(path, "%s/trails.codebook", root);
    store_codebook(codemap, path);
    TDB_TIMER_END("trail/store_codebook");

error:
    JLFA(tmp, unigram_freqs);
    JLFA(tmp, gram_freqs);
    JLFA(tmp, codemap);

    if (grouped_r)
        fclose(grouped_r);
    unlink(grouped_path);

    free(field_cardinalities);
    free(read_buf);
    free(fstats);

    return ret;
}
