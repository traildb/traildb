
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>

#include "judy_str_map.h"
#include "tdb_internal.h"
#include "arena.h"
#include "util.h"

struct lexicon_store{
    const char *path;
    FILE *out;
    uint64_t offset;
};

static Word_t *lookup_uuid(tdb_cons *cons, const void *uuid)
{
    const Word_t *uuid_words = (const Word_t*)uuid;
    Word_t *uuid_ptr_hi, *uuid_ptr_lo;
    Pvoid_t uuid_index_lo;

    /*
    UUID index, which maps 16-byte UUIDs to event indices,
    has the following structure:

    JudyL -> JudyL -> Word_t
    */
    JLI(uuid_ptr_hi, cons->uuid_index, uuid_words[0]);
    uuid_index_lo = (Pvoid_t)*uuid_ptr_hi;
    JLI(uuid_ptr_lo, uuid_index_lo, uuid_words[1]);
    *uuid_ptr_hi = (Word_t)uuid_index_lo;

    if (!*uuid_ptr_lo)
        ++cons->num_trails;

    return uuid_ptr_lo;
}

static void *lexicon_store_fun(uint64_t id,
                               const char *value,
                               uint64_t len,
                               void *state)
{
    struct lexicon_store *s = (struct lexicon_store*)state;

    /* NOTE: vals start at 1, otherwise we would need to +1 */
    SAFE_SEEK(s->out, id * 4, s->path);
    SAFE_WRITE(&s->offset, 4, s->path, s->out);

    SAFE_SEEK(s->out, s->offset, s->path);
    SAFE_WRITE(value, len, s->path, s->out);

    s->offset += len;
    return state;
}

static void lexicon_store(const struct judy_str_map *lexicon, const char *path)
{
    /*
    Lexicon format:
    [ number of values N ] 4 bytes
    [ value offsets ...  ] N * 4 bytes
    [ last value offset  ] 4 bytes
    [ values ...         ] X bytes
    */

    struct lexicon_store state;
    uint64_t count = jsm_num_keys(lexicon);
    uint64_t size = (count + 2) * 4;

    state.path = path;
    state.offset = (count + 2) * 4;

    /* TODO remove this limit - we could allow arbitrary-size lexicons */
    if (size > TDB_MAX_LEXICON_SIZE)
        DIE("Lexicon %s would be huge! %"PRIu64" items, %"PRIu64" bytes",
            path,
            count,
            size);

    SAFE_OPEN(state.out, path, "w");

    if (ftruncate(fileno(state.out), size))
        DIE("Could not initialize lexicon file (%"PRIu64" bytes): %s",
            size,
            path);

    SAFE_WRITE(&count, 4, path, state.out);

    jsm_fold(lexicon, lexicon_store_fun, &state);

    SAFE_SEEK(state.out, (count + 1) * 4, path);
    SAFE_WRITE(&state.offset, 4, path, state.out);

    SAFE_CLOSE(state.out, path);
}

static int store_lexicons(tdb_cons *cons)
{
    tdb_field i;
    FILE *out;
    char path[TDB_MAX_PATH_SIZE];

    tdb_path(path, "%s/fields", cons->root);
    SAFE_OPEN(out, path, "w");

    for (i = 0; i < cons->num_ofields; i++){
        tdb_path(path, "%s/lexicon.%s", cons->root, cons->ofield_names[i]);
        lexicon_store(&cons->lexicons[i], path);
        fprintf(out, "%s\n", cons->ofield_names[i]);
    }
    SAFE_CLOSE(out, path);

    return 0;
}

static int store_version(tdb_cons *cons)
{
    FILE *out;
    char path[TDB_MAX_PATH_SIZE];

    tdb_path(path, "%s/version", cons->root);
    SAFE_OPEN(out, path, "w");
    SAFE_FPRINTF(out, path, "%llu", TDB_VERSION_LATEST);
    SAFE_CLOSE(out, path);
    return 0;
}

static int store_uuids(tdb_cons *cons)
{
    Word_t uuid_words[2]; // NB: word must be 64-bit
    Word_t *ptr;
    FILE *out;
    char path[TDB_MAX_PATH_SIZE];

    tdb_path(path, "%s/uuids", cons->root);
    SAFE_OPEN(out, path, "w");

    if (cons->num_trails > TDB_MAX_NUM_TRAILS) {
        WARN("Too many trails (%"PRIu64")", cons->num_trails);
        return 1;
    }

    if (ftruncate(fileno(out), cons->num_trails * 16LLU)) {
        WARN("Could not init (%"PRIu64" trails): %s", cons->num_trails, path);
        return -1;
    }

    /* TODO reverse words here */
    uuid_words[0] = 0;
    JLF(ptr, cons->uuid_index, uuid_words[0]);
    while (ptr){
        const Pvoid_t uuid_index_lo = (Pvoid_t)*ptr;
        uuid_words[1] = 0;
        JLF(ptr, uuid_index_lo, uuid_words[1]);
        while (ptr){
            SAFE_WRITE(uuid_words, 16, path, out);
            JLN(ptr, uuid_index_lo, uuid_words[1]);
        }
        JLN(ptr, cons->uuid_index, uuid_words[0]);
    }

    SAFE_CLOSE(out, path);
    return 0;
}

static int dump_trail_pointers(tdb_cons *cons)
{
    Word_t uuid_words[2]; // NB: word must be 64-bit
    Word_t *ptr;
    Word_t tmp;
    uint64_t idx = 0;

    /* serialize trail pointers, freeing uuid_index */

    if ((cons->trail_pointers = malloc(cons->num_trails * 8)) == NULL) {
        WARN("Could not allocate trail pointers");
        return -1;
    }

    /* NOTE: The code below must populate trail_pointers
       in the same order as what gets stored by store_uuids() */
    uuid_words[0] = 0;
    JLF(ptr, cons->uuid_index, uuid_words[0]);
    while (ptr){
        Pvoid_t uuid_index_lo = (Pvoid_t)*ptr;

        uuid_words[1] = 0;
        JLF(ptr, uuid_index_lo, uuid_words[1]);
        while (ptr){
            cons->trail_pointers[idx++] = *ptr - 1;
            JLN(ptr, uuid_index_lo, uuid_words[1]);
        }

        JLFA(tmp, uuid_index_lo);
        JLN(ptr, cons->uuid_index, uuid_words[0]);
    }
    JLFA(tmp, cons->uuid_index);
    return 0;
}

static int is_fieldname_invalid(const char* field)
{
    uint32_t i;

    if (!strcmp(field, "time"))
        return 1;

    for (i = 0; i < TDB_MAX_FIELDNAME_LENGTH && field[i]; i++)
        if (!index(TDB_FIELDNAME_CHARS, field[i]))
            return 1;

    if (i == 0 || i == TDB_MAX_FIELDNAME_LENGTH)
        return 1;

    return 0;
}

static int find_duplicate_fieldnames(const char **ofield_names,
                                     uint32_t num_ofields)
{
    Pvoid_t check = NULL;
    tdb_field i;
    Word_t tmp;

    for (i = 0; i < num_ofields; i++){
        Word_t *ptr;
        JSLI(ptr, check, ofield_names[i]);
        if (*ptr){
            JSLFA(tmp, check);
            return 1;
        }
        *ptr = 1;
    }
    JSLFA(tmp, check);
    return 0;
}

/* make this const char** */
tdb_cons *tdb_cons_new(const char *root,
                       const char **ofield_names,
                       uint32_t num_ofields)
{
    tdb_cons *cons;
    tdb_field i;

    if (num_ofields > TDB_MAX_NUM_FIELDS)
        return NULL;

    if (find_duplicate_fieldnames(ofield_names, num_ofields))
        return NULL;

    if ((cons = calloc(1, sizeof(tdb_cons))) == NULL)
        return NULL;

    if (!(cons->ofield_names = calloc(num_ofields, sizeof(char*))))
        goto error;

    for (i = 0; i < num_ofields; i++){
        if (is_fieldname_invalid(ofield_names[i]))
            goto error;
        cons->ofield_names[i] = strdup(ofield_names[i]);
    }

    cons->root = strdup(root);
    cons->min_timestamp = UINT32_MAX;
    cons->max_timestamp = 0;
    cons->max_timedelta = 0;
    cons->num_ofields = num_ofields;
    cons->events.item_size = sizeof(tdb_cons_event);
    cons->items.item_size = sizeof(tdb_item);

    /* Opportunistically try to create the output directory.
       We don't care if it fails, e.g. because it already exists */
    mkdir(root, 0755);
    tdb_path(cons->tempfile, "%s/tmp.items.%d", root, getpid());
    if (!(cons->items.fd = fopen(cons->tempfile, "wx")))
        goto error;

    if (cons->num_ofields > 0)
        if (!(cons->lexicons = calloc(cons->num_ofields,
                                      sizeof(struct judy_str_map))))
            goto error;

    for (i = 0; i < cons->num_ofields; i++){
        if (jsm_init(&cons->lexicons[i]))
            goto error;
        }

    return cons;
 error:
    tdb_cons_free(cons);
    return NULL;
}

void tdb_cons_free(tdb_cons *cons)
{
    int i;
    for (i = 0; i < cons->num_ofields; i++){
        free(cons->ofield_names[i]);
        jsm_free(&cons->lexicons[i]);
    }
    free(cons->lexicons);
    if (cons->tempfile)
        unlink(cons->tempfile);
    if (cons->events.data)
        free(cons->events.data);
    if (cons->items.data)
        free(cons->items.data);
    free(cons->trail_pointers);
    free(cons->ofield_names);
    free(cons->root);
    free(cons);
}

#if 0
/* TODO re-enable this if we decide to support overflow values. */

/*
this function guarantees that the string representation of
TDB_OVERFLOW_VAL is unique in this lexicon.
*/
static const uint8_t *find_overflow_value(tdb_cons *cons, tdb_field field)
{
    static const int LEN = sizeof(TDB_OVERFLOW_STR) - 1;
    int i, j;
    Word_t *ptr;

    for (i = 1; i * 2 + LEN < TDB_MAX_VALUE_SIZE; i++){
        for (j = 0; j < i; j++){
            cons->overflow_str[j] = TDB_OVERFLOW_LSEP;
            cons->overflow_str[i + LEN + j] = TDB_OVERFLOW_RSEP;
        }
        memcpy(&cons->overflow_str[i], TDB_OVERFLOW_STR, LEN);
        cons->overflow_str[i * 2 + LEN] = 0;
        JSLG(ptr, cons->lexicons[field], cons->overflow_str);
        if (!ptr)
            return cons->overflow_str;
    }
    DIE("Field %u overflows and could not generate a unique overflow value",
        field);
}
#endif

/*
Insert a new string in the lexicon and return the matching val.
*/
static tdb_val insert_to_lexicon(struct judy_str_map *jsm,
                                 const char *value,
                                 uint32_t value_length)
{
    tdb_val val;

    if (jsm_num_keys(jsm) < TDB_MAX_NUM_VALUES){
        if ((val = jsm_insert(jsm, value, value_length)))
            return val;
        else
            return 0;
    }else{
        if ((val = jsm_get(jsm, value, value_length)))
            return val;
        else
            return TDB_OVERFLOW_VALUE;
    }
}

/*
Append an event in this cons.
*/
int tdb_cons_add(tdb_cons *cons,
                 const uint8_t uuid[16],
                 const uint32_t timestamp,
                 const char **values,
                 const uint32_t *value_lengths)
{
    tdb_field i;
    tdb_cons_event *event;
    Word_t *uuid_ptr;

    for (i = 0; i < cons->num_ofields; i++)
        if (value_lengths[i] > TDB_MAX_VALUE_SIZE)
            return 2;

    uuid_ptr = lookup_uuid(cons, uuid);
    event = (tdb_cons_event*)arena_add_item(&cons->events);

    event->item_zero = cons->items.next;
    event->num_items = 0;
    event->timestamp = timestamp;
    event->prev_event_idx = *uuid_ptr;
    *uuid_ptr = cons->events.next;

    if (timestamp < cons->min_timestamp)
        cons->min_timestamp = timestamp;

    for (i = 0; i < cons->num_ofields; i++){
        tdb_field field = i + 1;
        tdb_item item = field;
        tdb_val val;

        if (value_lengths[i]){
            if ((val = insert_to_lexicon(&cons->lexicons[i],
                                         values[i],
                                         value_lengths[i])))
                item |= val << 8;
            else
                return 1;
        }
        /* TODO do we have to write NULL items? could we just
        assume they exist implicitly? */
        *((tdb_item*)arena_add_item(&cons->items)) = item;
        ++event->num_items;
    }

    return 0;
}

/*
Append a single event to this cons. The event is a usual srequence
of (remapped) items. Used by tdb_cons_append().
*/
static void append_event(const tdb *db,
                         tdb_cons *cons,
                         uint64_t trail_id,
                         const uint32_t *items,
                         Word_t *uuid_ptr)
{
    tdb_cons_event *event = (tdb_cons_event*)arena_add_item(&cons->events);

    event->item_zero = cons->items.next;
    event->num_items = 0;
    event->timestamp = items[0];
    event->prev_event_idx = *uuid_ptr;
    *uuid_ptr = cons->events.next;

    tdb_field field;
    for (field = 1; field < cons->num_ofields + 1; field++){
        *((tdb_item*)arena_add_item(&cons->items)) = items[field];
        ++event->num_items;
    }
}

/*
Append the lexicons of an existing TrailDB, db, to this cons. Used by
tdb_cons_append().
*/
static uint32_t **append_lexicons(tdb_cons *cons, const tdb *db)
{
    uint32_t **lexicon_maps;
    uint32_t i, field;

    if (!(lexicon_maps = calloc(cons->num_ofields, sizeof(uint32_t*))))
        return NULL;

    for (field = 0; field < cons->num_ofields; field++){
        struct tdb_lexicon lex;
        uint32_t *map;

        tdb_lexicon_read(db, field, &lex);

        if (!(map = lexicon_maps[field] = malloc(lex.size * 4)))
            goto err;

        for (i = 0; i < lex.size; i++){
            uint32_t value_length;
            const char *value = tdb_lexicon_get(&lex, i, &value_length);
            tdb_val val;

            if ((val = insert_to_lexicon(&cons->lexicons[field],
                                         value,
                                         value_length)))
                map[i] = val;
            else
                goto err;
        }
    }
    return lexicon_maps;
err:
    for (i = 0; i < cons->num_ofields; i++)
        free(lexicon_maps[i]);
    free(lexicon_maps);
    return NULL;
}

/*
This is a variation of tdb_cons_add(): Instead of accepting fields as
strings, it reads them as integer items from an existing TrailDB and
remaps them to match with this cons.
*/
int tdb_cons_append(tdb_cons *cons, const tdb *db)
{
    uint32_t **lexicon_maps;
    uint64_t trail_id;
    tdb_item *items;
    uint32_t i, e, n, items_len = 0;
    int ret;
    /* event_width includes the event delimiter, 0 byte */
    const uint32_t event_width = db->num_fields + 1;

    if (cons->num_ofields != db->num_fields - 1)
        return -1;

    if (db->min_timestamp < cons->min_timestamp)
        cons->min_timestamp = db->min_timestamp;

    lexicon_maps = append_lexicons(cons, db);

    for (trail_id = 0; trail_id < db->num_trails; trail_id++){
        const uint8_t *uuid = tdb_get_uuid(db, trail_id);
        Word_t *uuid_ptr = lookup_uuid(cons, uuid);
        tdb_field field;

        /* TODO this could use an iterator of the trail */
        if (tdb_get_trail(db, trail_id, &items, &items_len, &n, 0)){
            ret = -1;
            goto err;
        }
        for (e = 0; e < n; e += event_width){
            for (field = 1; field < cons->num_ofields + 1; field++){
                uint32_t idx = e + field;
                tdb_val val = tdb_item_val(items[idx]);
                if (val && val != TDB_OVERFLOW_VALUE)
                    items[idx] = field | lexicon_maps[field - 1][val];
            }
            append_event(db, cons, trail_id, &items[e], uuid_ptr);
        }
    }
err:
    for (i = 0; i < cons->num_ofields; i++)
        free(lexicon_maps[i]);
    free(lexicon_maps);
    return ret;
}

int tdb_cons_finalize(tdb_cons *cons, uint64_t flags)
{
    struct tdb_file items_mmapped = {};

    cons->num_events = cons->events.next;

    /* finalize event items */
    arena_flush(&cons->items);
    if (fclose(cons->items.fd)) {
        WARN("Closing %s failed", cons->tempfile);
        return -1;
    }

    if (cons->num_events && cons->num_ofields) {
        if (tdb_mmap(cons->tempfile, &items_mmapped, NULL)) {
            WARN("Mmapping %s failed", cons->tempfile);
            return -1;
        }
    }

    TDB_TIMER_DEF

    /* finalize lexicons */
    TDB_TIMER_START
    if (store_lexicons(cons))
        goto error;
    TDB_TIMER_END("encoder/store_lexicons")

    TDB_TIMER_START
    if (store_uuids(cons))
        goto error;
    TDB_TIMER_END("encoder/store_uuids")

    TDB_TIMER_START
    if (store_version(cons))
        goto error;
    TDB_TIMER_END("encoder/store_version")

    if (dump_trail_pointers(cons))
        goto error;

    TDB_TIMER_START
    tdb_encode(cons, (tdb_item*)items_mmapped.data);
    TDB_TIMER_END("encoder/encode")

    if (items_mmapped.data)
        munmap((void*)items_mmapped.data, items_mmapped.size);
    return 0;

 error:
    if (items_mmapped.data)
        munmap((void*)items_mmapped.data, items_mmapped.size);
    tdb_cons_free(cons);
    return -1;
}
