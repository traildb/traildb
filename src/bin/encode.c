
#include <string.h>

#include "tdb_internal.h"
#include "util.h"

/* We want to filter out all corrupted and invalid timestamps
   but we don't know the exact timerange we should be getting.
   Hence, we assume a reasonable range. */
#define TSTAMP_MIN 1325404800 /* 2012-01-01 */
#define TSTAMP_MAX 1483257600 /* 2017-01-01 */
#define MAX_INVALID_RATIO 0.005

static int parse_line(tdb_cons *cons, char *line)
{
    static uint8_t cookie[16];
    char *hexcookie = strsep(&line, " "), *values;
    int64_t tstamp, i;

    if (strlen(hexcookie) != 32 || !line)
        return 1;

    if (tdb_cookie_raw((uint8_t*)hexcookie, cookie))
        return 1;

    tstamp = strtoll(strsep(&line, " "), NULL, 10);

    if (tstamp < TSTAMP_MIN || tstamp > TSTAMP_MAX)
        return 1;

    values = line;
    for (i = 0; i < cons->num_ofields; i++)
        if (line)
            strsep(&line, " ");
        else
            return 1; // Not enough fields in line

    if (line != NULL)
        return 1;     // Too many fields in line

    if (tdb_cons_add(cons, cookie, tstamp, values))
        return 1;     // Too many values in lexicon

    return 0;
}

static void read_input(tdb_cons *cons)
{
    char *line = NULL;
    size_t line_size = 0;
    uint64_t num_events = 0;
    uint64_t num_invalid = 0;
    ssize_t n;

    while ((n = getline(&line, &line_size, stdin)) > 0){
        if (num_events >= TDB_MAX_NUM_EVENTS)
            DIE("Too many events (%"PRIu64")", num_events);

        /* remove trailing newline */
        line[n - 1] = 0;

        ++num_events;
        if (parse_line(cons, line))
          ++num_invalid;

        if (!(num_events & 65535))
            INFO("%"PRIu64" lines processed (%"PRIu64" invalid)",
                 num_events,
                 num_invalid);
    }

    if (num_invalid / (float)num_events > MAX_INVALID_RATIO)
        DIE("Too many invalid lines (%"PRIu64" / %"PRIu64")",
            num_invalid,
            num_events);
    else
        fprintf(stderr,
                "All inputs consumed successfully: "
                "%"PRIu64" valid lines, %"PRIu64" invalid",
                num_events,
                num_invalid);
}

int main(int argc, char **argv)
{
    if (argc < 3)
        DIE("Usage: %s 'fields' outdir", argv[0]);

    int num_fields = 0;
    char *field_names = NULL, *spec = argv[1];
    for (spec = argv[1]; spec; strsep(&spec, " "))
        if (num_fields++ == 2)
            field_names = spec;

    if (num_fields < 2)
        DIE("Too few fields: At least cookie and timestamp are required");

    if (num_fields > TDB_MAX_NUM_FIELDS)
        DIE("Too many fields (%d)", num_fields);

    tdb_cons *cons = tdb_cons_new(argv[2], field_names, num_fields - 2);

    TDB_TIMER_DEF
    TDB_TIMER_START
    read_input(cons);
    TDB_TIMER_END("encoder/read_inputs")

    tdb_cons_finalize(cons, 0);
    tdb_cons_free(cons);

    return 0;
}
