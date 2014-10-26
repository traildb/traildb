
#include <sys/stat.h>
#include <unistd.h>

#include "tdb_internal.h"
#include "arena.h"
#include "util.h"

static uint64_t lexicon_size(const Pvoid_t lexicon, uint64_t *size)
{
    uint8_t value[TDB_MAX_VALUE_SIZE];
    uint64_t count = 0;
    Word_t *ptr;

    value[0] = 0;
    JSLF(ptr, lexicon, value);
    while (ptr){
        *size += strlen((char*)value);
        ++count;
        JSLN(ptr, lexicon, value);
    }
    return count;
}

static void lexicon_store(const Pvoid_t lexicon, const char *path)
{
    uint8_t value[TDB_MAX_VALUE_SIZE];
    tdb_val *val;
    uint64_t size = 0, offset;
    uint64_t count = lexicon_size(lexicon, &size);
    FILE *out;

    size += (count + 1) * 4;

    if (size > TDB_MAX_LEXICON_SIZE)
        DIE("Lexicon %s would be huge! %"PRIu64" items, %"PRIu64" bytes",
            path,
            count,
            size);

    SAFE_OPEN(out, path, "w");

    if (ftruncate(fileno(out), size))
        DIE("Could not initialize lexicon file (%"PRIu64" bytes): %s",
            size,
            path);

    SAFE_WRITE(&count, 4, path, out);

    value[0] = 0;
    offset = (count + 1) * 4;
    JSLF(val, lexicon, value);
    while (val){
        size_t len = strlen((char*)value);

        /* NOTE: vals start at 1, otherwise we would need to +1 */
        SAFE_SEEK(out, *val * 4, path);
        SAFE_WRITE(&offset, 4, path, out);

        SAFE_SEEK(out, offset, path);
        SAFE_WRITE(value, len + 1, path, out);

        offset += len + 1;
        JSLN(val, lexicon, value);
    }
    SAFE_CLOSE(out, path);
}

static int store_lexicons(tdb_cons *cons)
{
    int i;
    FILE *out;
    const char *field_name = cons->field_names;
    char path[TDB_MAX_PATH_SIZE];

    tdb_path(path, "%s/fields", cons->root);
    SAFE_OPEN(out, path, "w");

    for (i = 0; i < cons->num_fields; i++){
        size_t j = strlen(field_name);
        tdb_path(path, "%s/lexicon.%s", cons->root, field_name);
        lexicon_store(cons->lexicons[i], path);
        fprintf(out, "%s\n", field_name);
        field_name += j + 1;
    }
    SAFE_CLOSE(out, path);

    return 0;
}

static int store_cookies(tdb_cons *cons)
{
    Word_t cookie[2];
    Word_t *ptr;
    FILE *out;
    char path[TDB_MAX_PATH_SIZE];

    tdb_path(path, "%s/cookies", cons->root);
    SAFE_OPEN(out, path, "w");

    if (cons->num_cookies > TDB_MAX_NUM_COOKIES) {
        WARN("Too many cookies (%"PRIu64")", cons->num_cookies);
        return 1;
    }

    if (ftruncate(fileno(out), cons->num_cookies * 16LLU)) {
        WARN("Could not init (%"PRIu64" cookies): %s", cons->num_cookies, path);
        return -1;
    }

    cookie[0] = 0;
    JLF(ptr, cons->cookie_index, cookie[0]);
    while (ptr){
        const Pvoid_t cookie_index_lo = (Pvoid_t)*ptr;
        cookie[1] = 0;
        JLF(ptr, cookie_index_lo, cookie[1]);
        while (ptr){
            SAFE_WRITE(cookie, 16, path, out);
            JLN(ptr, cookie_index_lo, cookie[1]);
        }
        JLN(ptr, cons->cookie_index, cookie[0]);
    }

    SAFE_CLOSE(out, path);
    return 0;
}

static int dump_cookie_pointers(tdb_cons *cons)
{
    Word_t cookie[2];
    Word_t *ptr;
    Word_t tmp;
    uint64_t idx = 0;

    /* serialize cookie pointers, freeing cookie_index */

    if ((cons->cookie_pointers = malloc(cons->num_cookies * 8)) == NULL) {
        WARN("Could not allocate cookie array");
        return -1;
    }

    /* NOTE: The code below must populate cookie_pointers
       in the same order as what gets stored by store_cookies() */
    cookie[0] = 0;
    JLF(ptr, cons->cookie_index, cookie[0]);
    while (ptr){
        Pvoid_t cookie_index_lo = (Pvoid_t)*ptr;

        cookie[1] = 0;
        JLF(ptr, cookie_index_lo, cookie[1]);
        while (ptr){
            cons->cookie_pointers[idx++] = *ptr - 1;
            JLN(ptr, cookie_index_lo, cookie[1]);
        }

        JLFA(tmp, cookie_index_lo);
        JLN(ptr, cons->cookie_index, cookie[0]);
    }
    JLFA(tmp, cons->cookie_index);
    return 0;
}

tdb_cons *tdb_cons_new(const char *root,
                       const char *field_names,
                       uint32_t num_fields)
{
    tdb_cons *cons;
    if ((cons = calloc(1, sizeof(tdb_cons))) == NULL)
        return NULL;

    cons->root = strdup(root);
    cons->field_names = dupstrs(field_names, num_fields);
    cons->min_timestamp = UINT32_MAX;
    cons->max_timestamp = 0;
    cons->max_timedelta = 0;
    cons->num_fields = num_fields;
    cons->events.item_size = sizeof(tdb_cons_event);
    cons->items.item_size = sizeof(tdb_item);

    /* Opportunistically try to create the output directory.
       We don't care if it fails, e.g. because it already exists */
    mkdir(root, 0755);
    tdb_path(cons->tempfile, "%s/tmp.items.%d", root, getpid());
    if (!(cons->items.fd = fopen(cons->tempfile, "wx")))
        goto error;
    if (!(cons->lexicons = calloc(cons->num_fields, sizeof(Pvoid_t))))
        goto error;
    if (!(cons->lexicon_counters = calloc(cons->num_fields, sizeof(Word_t))))
        goto error;
    return cons;

 error:
    tdb_cons_free(cons);
    return NULL;
}

void tdb_cons_free(tdb_cons *cons)
{
    if (cons->tempfile)
        unlink(cons->tempfile);
    free(cons->root);
    free(cons->field_names);
    free(cons->cookie_pointers);
    free(cons->lexicon_counters);
    free(cons);
}

int tdb_cons_add(tdb_cons *cons,
                 const uint8_t cookie[16],
                 const uint32_t timestamp,
                 const char *values)
{
    const Word_t *cookie_words = (Word_t*)cookie;
    Word_t *cookie_ptr_hi;
    Word_t *cookie_ptr_lo;
    Pvoid_t cookie_index_lo;

    if (timestamp < cons->min_timestamp)
        cons->min_timestamp = timestamp;

    /* Cookie index, which maps 16-byte cookies to event indices,
       has the following structure:
          JudyL -> JudyL -> Word_t
    */
    JLI(cookie_ptr_hi, cons->cookie_index, cookie_words[0]);
    cookie_index_lo = (Pvoid_t)*cookie_ptr_hi;
    JLI(cookie_ptr_lo, cookie_index_lo, cookie_words[1]);
    *cookie_ptr_hi = (Word_t)cookie_index_lo;

    if (!*cookie_ptr_lo)
        ++cons->num_cookies;

    tdb_cons_event *event = (tdb_cons_event*)arena_add_item(&cons->events);
    event->item_zero = cons->items.next;
    event->num_items = 0;
    event->timestamp = timestamp;
    event->prev_event_idx = *cookie_ptr_lo;
    *cookie_ptr_lo = cons->events.next;

    int i;
    Word_t *val_p;
    const char *value = values;
    for (i = 0; i < cons->num_fields; i++){
        size_t j = strlen(value);
        tdb_field field = i + 1;
        tdb_item item = field;
        if (j){
            JSLI(val_p, cons->lexicons[i], (uint8_t*)value);
            if (*val_p == 0){
                *val_p = ++cons->lexicon_counters[i];
                if (*val_p >= TDB_MAX_NUM_VALUES)
                    return field;
            }
            item |= (*val_p) << 8;
        }
        *((tdb_item*)arena_add_item(&cons->items)) = item;
        ++event->num_items;
        value += j + 1;
    }

    return 0;
}

int tdb_cons_finalize(tdb_cons *cons, uint64_t flags)
{
    tdb_file items_mmapped;
    Word_t lexsize;
    int i;

    cons->num_events = cons->events.next;

    /* finalize event items */
    arena_flush(&cons->items);
    if (fclose(cons->items.fd)) {
        WARN("Closing %s failed", cons->tempfile);
        return -1;
    }

    if (tdb_mmap(cons->tempfile, &items_mmapped, NULL)) {
        WARN("Mmapping %s failed", cons->tempfile);
        return -1;
    }

    TDB_TIMER_DEF

    /* finalize lexicons */
    TDB_TIMER_START
    if (store_lexicons(cons))
        goto error;
    TDB_TIMER_END("encoder/store_lexicons")

    TDB_TIMER_START
    if (store_cookies(cons))
        goto error;
    TDB_TIMER_END("encoder/store_cookies")

    /* free lexicons */
    for (i = 0; i < cons->num_fields; i++)
        JSLFA(lexsize, cons->lexicons[i]);

    if (dump_cookie_pointers(cons))
        goto error;

    TDB_TIMER_START
    tdb_encode(cons, (tdb_item*)items_mmapped.data);
    TDB_TIMER_END("encoder/encode")

    return 0;

 error:
    tdb_cons_free(cons);
    return -1;
}
