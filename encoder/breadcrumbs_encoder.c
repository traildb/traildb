
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdint.h>

#include <Judy.h>

#include "breadcrumbs_encoder.h"

#define MAX_NUM_FIELDS 255
#define MAX_NUM_INPUTS 10000000
#define TSTAMP_MAX (UINT32_MAX - 1)
#define ARENA_INCREMENT 10000000
#define INVALID_RATIO 0.01

struct logline{
    uint32_t fields_offset;
    uint32_t num_fields;
    uint32_t timestamp;
    struct logline *prev;
};

struct cookie{
    struct logline *last;
    uint32_t previous_values[0];
} __attribute((packed))__;

struct arena{
    void *data;
    uint32_t size;
    uint32_t next;
    uint32_t item_size;
};

static Pvoid_t cookie_index;
static Pvoid_t *lexicon;
static Word_t *lexicon_counters;

static struct arena cookies = {.item_size = sizeof(struct cookie)};
static struct arena loglines = {.item_size = sizeof(struct logline)};
static struct arena fields = {.item_size = 4};

static void *add_item(struct arena *a)
{
    if (a->next >= a->size){
        a->size += ARENA_INCREMENT;
        if (!(a->data = realloc(a->data, a->item_size * a->size)))
            DIE("Realloc failed\n");
    }
    return a->data + a->item_size * a->next++;
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

static int parse_line(char *line, long num_fields)
{
    int i;
    char *tmp;
    int64_t tstamp;
    Word_t *cookie_ptr;
    struct cookie *cookie;
    struct logline *logline;
    char *cookie_str = strsep(&line, " ");

    if (strlen(cookie_str) != 32 || !line)
        return 1;

    tstamp = strtoll(strsep(&line, " "), &tmp, 10);

    if (tstamp < 0 || tstamp > TSTAMP_MAX)
        return 1;

    JSLI(cookie_ptr, cookie_index, (uint8_t*)cookie_str);
    if (!*cookie_ptr){
        cookie = (struct cookie*)add_item(&cookies);
        memset(cookie, 0, cookies.item_size);
        *cookie_ptr = (Word_t)cookie;
    }
    cookie = (struct cookie*)*cookie_ptr;
    logline = (struct logline*)add_item(&loglines);
    cookie->last = logline;

    logline->fields_offset = fields.next;
    logline->num_fields = 0;
    logline->timestamp = (uint32_t)tstamp;
    logline->prev = cookie->last;

    for (i = 0; line && i < num_fields - 2; i++){
        char *field = strsep(&line, " ");
        uint32_t value = i + 1;

        if (field[0]){
            Word_t *token_id;
            JSLI(token_id, lexicon[i], (uint8_t*)field);
            if (!*token_id){
                *token_id = ++lexicon_counters[i];
                if (*token_id >= (1 << 24))
                    DIE("Too many values for the %dth field!\n", i);
            }
            value |= (*token_id) << 8;
        }

        if (cookie->previous_values[i] != value){
            *((uint32_t*)add_item(&fields)) = value;
            cookie->previous_values[i] = value;
            ++logline->num_fields;
        }
    }

    if (i < num_fields - 2)
        return 1;
    else
        return 0;
}

static void read_inputs(long num_fields, long num_inputs)
{
    char *line = NULL;
    size_t line_size = 0;
    uint32_t num_lines = 0;
    uint32_t num_invalid = 0;

    while (num_inputs){
        ssize_t n = getline(&line, &line_size, stdin);

        if (n < 1)
            DIE("Truncated input\n");

        if (!strcmp(line, "**DONE**\n"))
            --num_inputs;
        else
            if (parse_line(line, num_fields))
                ++num_invalid;

        ++num_lines;
    }

    if (num_invalid / (float)num_lines > INVALID_RATIO)
        DIE("Too many invalid lines: %u invalid, %u valid\n",
            num_invalid,
            num_lines);
}

int main(int argc, char **argv)
{
    long num_fields;
    long num_inputs;

    if (argc < 3)
        DIE("Usage: breadcrumbs_encoder number-of-fields number-of-inputs\n");

    num_fields = parse_int_arg(argv[1], "fields", MAX_NUM_FIELDS);
    num_inputs = parse_int_arg(argv[2], "inputs", MAX_NUM_INPUTS);

    FILE *in = fopen("/dev/stdin", "r+");
    if (!in)
        DIE("Could not initialize stdin\n");

    cookies.item_size += (num_fields - 2) * 4;

    if (!(lexicon = calloc(num_fields - 2, sizeof(Pvoid_t))))
        DIE("Could not allocate lexicon\n");

    if (!(lexicon_counters = calloc(num_fields - 2, sizeof(Word_t))))
        DIE("Could not allocate lexicon counters\n");

    read_inputs(num_fields, num_inputs);

    return 0;
}

