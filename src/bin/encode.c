
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#include "tdb_internal.h"
#include "arena.h"
#include "hex.h"
#include "util.h"

/* We want to filter out all corrupted and invalid timestamps
   but we don't know the exact timerange we should be getting.
   Hence, we assume a reasonable range. */
#define TSTAMP_MIN 1325404800 /* 2012-01-01 */
#define TSTAMP_MAX 1483257600 /* 2017-01-01 */

#define MAX_LEXICON_SIZE    UINT32_MAX
#define MAX_FIELD_NAME_SIZE 1024
#define MAX_INVALID_RATIO   0.005

static Pvoid_t cookie_index;
static uint64_t num_cookies;
static Pvoid_t *lexicon;
static Word_t *lexicon_counters;

static struct arena events = {.item_size = sizeof(tdb_event)};
static struct arena items = {.item_size = sizeof(tdb_item)};

static int parse_num_fields(const char *str)
{
    int num_fields = 0;
    int i = 0;
    while (str[i])
        if (str[i++] == ' ')
            ++num_fields;

    return num_fields + 1;
}

static int parse_line(char *line, int num_fields)
{
    static uint64_t cookie_bytes[2];
    int i;
    char *tmp;
    int64_t tstamp;
    Word_t *cookie_ptr_hi;
    Word_t *cookie_ptr_lo;
    Pvoid_t cookie_index_lo;
    tdb_event *event;
    char *cookie_str = strsep(&line, " ");

    if (strlen(cookie_str) != 32 || !line)
        return 1;

    if (hex_decode(cookie_str, (uint8_t *)cookie_bytes))
        return 1;

    tstamp = strtoll(strsep(&line, " "), &tmp, 10);

    if (tstamp < TSTAMP_MIN || tstamp > TSTAMP_MAX)
        return 1;

    /* Cookie index, which maps 16-byte cookies to event indices,
       has the following structure:
          JudyL -> JudyL -> Word_t
    */
    JLI(cookie_ptr_hi, cookie_index, cookie_bytes[0]);
    cookie_index_lo = (Pvoid_t)*cookie_ptr_hi;
    JLI(cookie_ptr_lo, cookie_index_lo, cookie_bytes[1]);
    *cookie_ptr_hi = (Word_t)cookie_index_lo; // XXX: what does this do?

    if (!*cookie_ptr_lo)
        ++num_cookies;

    event = (tdb_event*)arena_add_item(&events);

    event->item_zero = items.next;
    event->num_items = 0;
    event->timestamp = (uint32_t)tstamp;
    event->prev_event_idx = *cookie_ptr_lo;
    *cookie_ptr_lo = events.next;

    for (i = 0; line && i < num_fields - 2; i++){
        char *field_name = strsep(&line, " ");
        size_t field_size = strlen(field_name);
        tdb_field field = i + 1;
        tdb_item item = field;

        if (field_size >= MAX_FIELD_NAME_SIZE)
            field_name[MAX_FIELD_NAME_SIZE - 1] = 0;

        if (field_size > 0){
            Word_t *val_p;
            JSLI(val_p, lexicon[i], (uint8_t*)field_name);
            if (!*val_p){
                *val_p = ++lexicon_counters[i];
                if (*val_p >= TDB_MAX_NUM_VALUES)
                    DIE("Too many values for field %d!\n", i);
            }
            item |= (*val_p) << 8;
        }
        *((tdb_item*)arena_add_item(&items)) = item;
        ++event->num_items;
    }

    /* lines with too few or too many fields are considered invalid */
    if (i < num_fields - 2 || line != NULL)
        return 1;
    return 0;
}

static void read_input(int num_fields)
{
    char *line = NULL;
    size_t line_size = 0;
    uint64_t num_events = 0;
    uint64_t num_invalid = 0;
    ssize_t n;

    while ((n = getline(&line, &line_size, stdin)) > 0){
        if (num_events >= TDB_MAX_NUM_EVENTS)
            DIE("Too many events (%llu)\n", num_events);

        /* remove trailing newline */
        line[n - 1] = 0;

        ++num_events;
        if (parse_line(line, num_fields))
          ++num_invalid;

        if (!(num_events & 65535))
            fprintf(stderr, "%llu lines processed (%llu invalid)\n",
                    num_events, num_invalid);
    }

    if (num_invalid / (float)num_events > MAX_INVALID_RATIO)
        DIE("Too many invalid lines (%llu / %llu)\n",
            num_invalid,
            num_events);
    else
        fprintf(stderr,
                "All inputs consumed successfully: "
                "%llu valid lines, %llu invalid\n",
                num_events,
                num_invalid);
}

void store_cookies(const Pvoid_t cookie_index,
                   uint64_t num_cookies,
                   const char *path)
{
    Word_t cookie_bytes[2];
    FILE *out;
    Word_t *ptr;

    if (!(out = fopen(path, "w")))
        DIE("Could not create cookie file: %s\n", path);

    if (num_cookies > TDB_MAX_NUM_COOKIES)
        DIE("Too many cookies (%llu)\n", num_cookies);

    if (ftruncate(fileno(out), num_cookies * 16LLU))
        DIE("Could not init lexicon (%llu cookies): %s\n", num_cookies, path);

    cookie_bytes[0] = 0;
    JLF(ptr, cookie_index, cookie_bytes[0]);
    while (ptr){
        const Pvoid_t cookie_index_lo = (const Pvoid_t)*ptr;
        cookie_bytes[1] = 0;
        JLF(ptr, cookie_index_lo, cookie_bytes[1]);
        while (ptr){
            SAFE_WRITE(cookie_bytes, 16, path, out);
            JLN(ptr, cookie_index_lo, cookie_bytes[1]);
        }
        JLN(ptr, cookie_index, cookie_bytes[0]);
    }
    SAFE_CLOSE(out, path);
}

static uint64_t lexicon_size(const Pvoid_t lexicon, uint64_t *size)
{
    uint8_t token[MAX_FIELD_NAME_SIZE];
    Word_t *ptr;
    uint64_t count = 0;

    token[0] = 0;
    JSLF(ptr, lexicon, token);
    while (ptr){
        *size += strlen((char*)token);
        ++count;
        JSLN(ptr, lexicon, token);
    }
    return count;
}

void store_lexicon(const Pvoid_t lexicon, const char *path)
{
    uint8_t token[MAX_FIELD_NAME_SIZE];
    uint64_t size = 0;
    uint64_t count = lexicon_size(lexicon, &size);
    uint64_t offset;
    FILE *out;
    Word_t *token_id;

    size += (count + 1) * 4;

    if (size > MAX_LEXICON_SIZE)
        DIE("Lexicon %s would be huge! %llu items, %llu bytes\n",
            path,
            (long long unsigned int)count,
            (long long unsigned int)size);

    if (!(out = fopen(path, "w")))
        DIE("Could not create lexicon file: %s\n", path);

    if (ftruncate(fileno(out), size))
        DIE("Could not initialize lexicon file (%llu bytes): %s\n",
            (long long unsigned int)size,
            path);

    SAFE_WRITE(&count, 4, path, out);

    token[0] = 0;
    offset = (count + 1) * 4;
    JSLF(token_id, lexicon, token);
    while (token_id){
        uint32_t len = strlen((char*)token);

        /* NOTE: token IDs start 1, otherwise we would need to +1 */
        SAFE_SEEK(out, *token_id * 4, path);
        SAFE_WRITE(&offset, 4, path, out);

        SAFE_SEEK(out, offset, path);
        SAFE_WRITE(token, len + 1, path, out);

        offset += len + 1;
        JSLN(token_id, lexicon, token);
    }
    SAFE_CLOSE(out, path);
}

static void store_lexicons(char *fields,
                           int num_lexicons,
                           const char *root)
{
    int i;
    FILE *out;
    char path[TDB_MAX_PATH_SIZE];

    tdb_path(path, "%s/fields", root);
    if (!(out = fopen(path, "w")))
        DIE("Could not open %s\n", path);

    /* skip cookie */
    strsep(&fields, " ");
    /* skip timestamp */
    strsep(&fields, " ");

    for (i = 0; i < num_lexicons; i++){
        const char *field = strsep(&fields, " ");
        tdb_path(path, "%s/lexicon.%s", root, field);
        store_lexicon(lexicon[i], path);
        fprintf(out, "%s\n", field);
    }
    SAFE_CLOSE(out, path);

    tdb_path(path, "%s/cookies", root);
    store_cookies(cookie_index, num_cookies, path);
}

void dump_cookie_pointers(uint64_t *cookie_pointers)
{
    Word_t cookie_bytes[2];
    Word_t *ptr;
    Word_t tmp;
    uint64_t idx = 0;

    /* NOTE: The code below must populate cookie_pointers
       in the same order as what gets stored by store_cookies() */
    cookie_bytes[0] = 0;
    JLF(ptr, cookie_index, cookie_bytes[0]);
    while (ptr){
        Pvoid_t cookie_index_lo = (Pvoid_t)*ptr;

        cookie_bytes[1] = 0;
        JLF(ptr, cookie_index_lo, cookie_bytes[1]);
        while (ptr){
            cookie_pointers[idx++] = *ptr - 1;
            JLN(ptr, cookie_index_lo, cookie_bytes[1]);
        }

        JLFA(tmp, cookie_index_lo);
        JLN(ptr, cookie_index, cookie_bytes[0]);
    }
    JLFA(tmp, cookie_index);
}

int main(int argc, char **argv)
{
    char *outdir;
    char items_tmp_path[TDB_MAX_PATH_SIZE];
    tdb_file items_mmaped;
    uint64_t *cookie_pointers;
    int i, num_fields;
    TDB_TIMER_DEF

    if (argc < 3)
        DIE("Usage: %s 'fields' outdir\n", argv[0]);

    num_fields = parse_num_fields(argv[1]);

    if (num_fields < 2)
        DIE("Too few fields: At least cookie and timestamp are required\n");

    if (num_fields > TDB_MAX_NUM_FIELDS)
        DIE("Too many fields (%d)\n", num_fields);

    /* Opportunistically try to create the output directory. We don't
       care if it fails, e.g. because it already exists */
    outdir = argv[2];
    mkdir(outdir, 0755);

    tdb_path(items_tmp_path, "%s/tmp.items.%d", outdir, getpid());
    if (!(items.fd = fopen(items_tmp_path, "wx")))
        DIE("Could not open temp file at %s\n", items_tmp_path);

    if (!(lexicon = calloc(num_fields - 2, sizeof(Pvoid_t))))
        DIE("Could not allocate lexicon\n");

    if (!(lexicon_counters = calloc(num_fields - 2, sizeof(Word_t))))
        DIE("Could not allocate lexicon counters\n");

    TDB_TIMER_START
    read_input(num_fields);
    TDB_TIMER_END("encoder/read_inputs")

    /* finalize event items */
    arena_flush(&items);
    if (fclose(items.fd))
        DIE("Closing %s failed\n", items_tmp_path);

    if (tdb_mmap(items_tmp_path, &items_mmaped, NULL))
        DIE("Mmapping %s failed\n", items_tmp_path);

    /* finalize lexicon */
    TDB_TIMER_START
    store_lexicons(argv[1], num_fields - 2, outdir);
    TDB_TIMER_END("encoder/store_lexicons")

    for (i = 0; i < num_fields - 2; i++){
        Word_t tmp;
        JSLFA(tmp, lexicon[i]);
    }

    /* serialize cookie pointers, freeing cookie_index */
    if (!(cookie_pointers = malloc(num_cookies * 8)))
        DIE("Could not allocate cookie array\n");
    dump_cookie_pointers(cookie_pointers);

    TDB_TIMER_START
    tdb_encode(cookie_pointers,                    /* last event by cookie */
               num_cookies,                        /* number of cookies */
               (tdb_event*)events.data,            /* all events */
               events.next,                        /* number of events */
               (const tdb_item*)items_mmaped.data, /* all items */
               items.next,                         /* number of items */
               num_fields,                         /* number of fields */
               (const uint64_t*)lexicon_counters,  /* field cardinalities */
               outdir);                            /* output directory */
    TDB_TIMER_END("encoder/store_trails")

    unlink(items_tmp_path);
    free(cookie_pointers);
    free(lexicon_counters);

    return 0;
}
