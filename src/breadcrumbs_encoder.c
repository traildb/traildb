
#define _GNU_SOURCE

#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>

#include <Judy.h>

#include "ddb_profile.h"
#include "hex_decode.h"
#include "util.h"

#include "breadcrumbs_encoder.h"
#include "trail_encode.h"
#include "lexicon_encode.h"

#define ARENA_INCREMENT 100000000
#define ARENA_DISK_BUFFER (1 << 23) /* must be a power of two */

struct arena{
    void *data;
    uint64_t size;
    uint64_t next;
    uint32_t item_size;
    FILE *fd;
};

static Pvoid_t cookie_index;
static uint64_t num_cookies;
static Pvoid_t *lexicon;
static Word_t *lexicon_counters;

static struct arena loglines = {.item_size = sizeof(struct logline)};
static struct arena values = {.item_size = 4};

static void flush_arena(const struct arena *a)
{
    if (a->fd){
        uint64_t size = (((a->next - 1) & (ARENA_DISK_BUFFER - 1)) + 1) *
                        (uint64_t)a->item_size;
        SAFE_WRITE(a->data,
                   size,
                   "tmp.values",
                   a->fd);
    }
}

static void *add_item(struct arena *a)
{
    if (a->fd){
        if (a->size == 0){
            a->size = ARENA_DISK_BUFFER;
            if (!(a->data = malloc(a->item_size * (uint64_t)a->size)))
                DIE("Arena malloc failed\n");
        }else if ((a->next & (ARENA_DISK_BUFFER - 1)) == 0)
            flush_arena(a);
        return a->data + a->item_size * (a->next++ & (ARENA_DISK_BUFFER - 1));
    }else{
        if (a->next >= a->size){
            a->size += ARENA_INCREMENT;
            if (!(a->data = realloc(a->data, a->item_size * (uint64_t)a->size)))
                DIE("Arena realloc failed\n");
        }
        return a->data + a->item_size * a->next++;
    }
}

static long parse_int_arg(const char *str, const char *type, long max)
{
    char *endp;
    long n = strtol(str, &endp, 10);
    if (n < 1)
        DIE("invalid number of %s: %s\n", type, str);
    if (n >= max)
        DIE("too many %s: %ld >= %ld\n", type, n, max);
    return n;
}

static long parse_num_fields(const char *str)
{
    long num_fields = 0;
    int i = 0;
    while (str[i])
        if (str[i++] == ' ')
            ++num_fields;

    return num_fields + 1;
}

static int parse_line(char *line, long num_fields)
{
    static Word_t cookie_bytes[2];
    int i;
    char *tmp;
    int64_t tstamp;
    Word_t *cookie_ptr_hi;
    Word_t *cookie_ptr_lo;
    Pvoid_t cookie_index_lo;
    struct logline *logline;
    char *cookie_str = strsep(&line, " ");

    if (strlen(cookie_str) != 32 || !line)
        return 1;

    if (hex_decode(cookie_str, (uint8_t*)cookie_bytes))
        return 1;

    //printf("Str %s -> %016lx%016lx\n", cookie_str, cookie_bytes[0], cookie_bytes[1]);

    tstamp = strtoll(strsep(&line, " "), &tmp, 10);

    if (tstamp < TSTAMP_MIN || tstamp > TSTAMP_MAX)
        return 1;

    /* Cookie index, which maps 16-byte cookies to indices,
       has the following structure:
          JudyL -> JudyL -> Word_t
    */
    JLI(cookie_ptr_hi, cookie_index, cookie_bytes[0]);
    cookie_index_lo = (Pvoid_t)*cookie_ptr_hi;
    JLI(cookie_ptr_lo, cookie_index_lo, cookie_bytes[1]);
    *cookie_ptr_hi = (Word_t)cookie_index_lo;

    if (!*cookie_ptr_lo)
        ++num_cookies;

    logline = (struct logline*)add_item(&loglines);

    logline->values_offset = values.next;
    logline->num_values = 0;
    logline->timestamp = (uint32_t)tstamp;
    logline->prev_logline_idx = *cookie_ptr_lo;
    *cookie_ptr_lo = loglines.next;

    for (i = 0; line && i < num_fields - 2; i++){
        char *field = strsep(&line, " ");
        uint32_t field_size = strlen(field);
        uint32_t value = i + 1;

        if (field_size >= MAX_FIELD_SIZE)
            field[MAX_FIELD_SIZE - 1] = 0;

        if (field_size > 0){
            Word_t *token_id;
            JSLI(token_id, lexicon[i], (uint8_t*)field);
            if (!*token_id){
                *token_id = ++lexicon_counters[i];
                if (*token_id >= (1 << 24) - 1)
                    DIE("Too many values for the %dth field!\n", i);
            }
            value |= (*token_id) << 8;
        }
        *((uint32_t*)add_item(&values)) = value;
        ++logline->num_values;
    }

    /* lines with too few or too many fields are considered invalid */
    if (i < num_fields - 2 || line != NULL)
        return 1;
    else
        return 0;
}

static void read_inputs(long num_fields, long num_inputs)
{
    char *line = NULL;
    size_t line_size = 0;
    long long unsigned int num_lines = 0;
    long long unsigned int num_invalid = 0;

    while (num_inputs){
        ssize_t n = getline(&line, &line_size, stdin);

        if (n < 1)
            DIE("Truncated input\n");

        /* remove trailing newline */
        line[n - 1] = 0;

        if (!strcmp(line, "**DONE**"))
            --num_inputs;
        else{
            ++num_lines;
            if (parse_line(line, num_fields))
                ++num_invalid;
        }

        if (!(num_lines & 65535))
            fprintf(stderr, "%llu lines processed (%llu invalid)\n",
                    num_lines, num_invalid);

        if (num_lines >= UINT32_MAX)
            DIE("Over 2^32 loglines!\n");
    }

    if (num_invalid / (float)num_lines > INVALID_RATIO)
        DIE("Too many invalid lines: %llu invalid, %llu valid\n",
            num_invalid,
            num_lines);
    else
        fprintf(stderr,
                "All inputs consumed successfully: "
                "%llu valid lines, %llu invalid\n",
                num_lines,
                num_invalid);
}


static void store_lexicons(char *fields,
                           int num_lexicons,
                           const char *root)
{
    int i;
    FILE *out;
    char path[MAX_PATH_SIZE];

    make_path(path, "%s/fields", root);
    if (!(out = fopen(path, "w")))
        DIE("Could not open %s\n", path);

    /* skip cookie */
    strsep(&fields, " ");
    /* skip timestamp */
    strsep(&fields, " ");

    for (i = 0; i < num_lexicons; i++){
        const char *field = strsep(&fields, " ");
        make_path(path, "%s/lexicon.%s", root, field);
        store_lexicon(lexicon[i], path);
        fprintf(out, "%s\n", field);
    }
    SAFE_CLOSE(out, path);

    make_path(path, "%s/cookies", root);
    store_cookies(cookie_index, num_cookies, path);
}

void dump_cookie_pointers(uint64_t *cookie_pointers)
{
    Word_t cookie_bytes[2];
    Word_t *ptr;
    Word_t tmp;
    uint64_t idx = 0;

    /* NOTE: The code below must populate cookie_pointers
       in the same order as what gets stored by store_cookies()
    */
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
    char values_tmp_path[MAX_PATH_SIZE];
    struct bdfile values_mmaped;
    uint64_t *cookie_pointers;
    long num_fields;
    long num_inputs;
    FILE *in;
    int i;
    DDB_TIMER_DEF

    if (argc < 4)
        DIE("Usage: breadcrumbs_encoder "
            "list-of-fields number-of-inputs output-dir\n");

    num_fields = parse_num_fields(argv[1]);
    num_inputs = parse_int_arg(argv[2], "inputs", MAX_NUM_INPUTS);

    if (num_fields < 2)
        DIE("Too few fields: At least cookie and timestamp are required\n");

    if (num_fields > MAX_NUM_FIELDS)
        DIE("Too many fields: %ld > %d\n", num_fields, MAX_NUM_FIELDS);

    /* Open our own stdin for writing: This ensures that FIFO won't
       get EOF'ed ever. */
    in = fopen("/dev/stdin", "r+");
    if (!in)
        DIE("Could not initialize stdin\n");

    /* Opportunistically try to create the output directory. We don't
       care if it fails, e.g. because it already exists */
    mkdir(argv[3], 0755);

    make_path(values_tmp_path, "%s/tmp.values.%d", argv[3], getpid());
    if (!(values.fd = fopen(values_tmp_path, "wx")))
        DIE("Could not open temp file at %s\n", values_tmp_path);

    if (!(lexicon = calloc(num_fields - 2, sizeof(Pvoid_t))))
        DIE("Could not allocate lexicon\n");

    if (!(lexicon_counters = calloc(num_fields - 2, sizeof(Word_t))))
        DIE("Could not allocate lexicon counters\n");

    DDB_TIMER_START
    read_inputs(num_fields, num_inputs);
    DDB_TIMER_END("encoder/read_inputs")

    /* finalize logline values */
    flush_arena(&values);
    if (fclose(values.fd))
        DIE("Closing %s failed\n", values_tmp_path);

    if (mmap_file(values_tmp_path, &values_mmaped, NULL))
        DIE("Mmapping %s failed\n", values_tmp_path);

    /* finalize lexicon */
    DDB_TIMER_START
    store_lexicons(argv[1], num_fields - 2, argv[3]);
    DDB_TIMER_END("encoder/store_lexicons")

    for (i = 0; i < num_fields - 2; i++){
        Word_t tmp;
        JSLFA(tmp, lexicon[i]);
    }

    /* serialize cookie pointers, freeing cookie_index */
    if (!(cookie_pointers = malloc(num_cookies * 8)))
        DIE("Could not allocate cookie values array\n");
    dump_cookie_pointers(cookie_pointers);

    DDB_TIMER_START
    store_trails(cookie_pointers, /* pointer to last event by cookie */
                 num_cookies, /* number of cookies */
                 (const struct logline*)loglines.data, /* loglines */
                 loglines.next, /* number of loglines */
                 (const uint32_t*)values_mmaped.data, /* field values */
                 values.next, /* number of values */
                 num_fields, /* number of fields */
                 (const uint64_t*)lexicon_counters, /* field cardinalities */
                 argv[3]); /* output directory */
    DDB_TIMER_END("encoder/store_trails")

    unlink(values_tmp_path);
    free(cookie_pointers);
    free(lexicon_counters);

    return 0;
}

