#define _DEFAULT_SOURCE /* ftruncate() */
#define _GNU_SOURCE /* mkostemp() */

#include <sys/mman.h>
#include <sys/stat.h>
#include <sys/types.h>
#include <unistd.h>
#include <stdlib.h>

#include "judy_str_map.h"
#include "tdb_internal.h"
#include "arena.h"
#include "util.h"

struct jm_fold_state{
    const char *path;
    FILE *out;
    uint64_t offset;
};

static void *lexicon_store_fun(uint64_t id,
                               const char *value,
                               uint64_t len,
                               void *state)
{
    struct jm_fold_state *s = (struct jm_fold_state*)state;

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

    struct jm_fold_state state;
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

    if (ftruncate(fileno(state.out), (off_t)size))
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

static void *store_uuids_fun(__uint128_t key,
                             Word_t *value __attribute__((unused)),
                             void *state)
{
    struct jm_fold_state *s = (struct jm_fold_state*)state;
    SAFE_WRITE(&key, 16, s->path, s->out);
    return s;
}

static int store_uuids(tdb_cons *cons)
{
    char path[TDB_MAX_PATH_SIZE];
    struct jm_fold_state state = {.path = path};
    uint64_t num_trails = j128m_num_keys(&cons->trails);

    /* this is why num_trails < TDB_MAX)NUM_TRAILS < 2^59:
       (2^59 - 1) * 16 < LONG_MAX (off_t) */
    if (num_trails > TDB_MAX_NUM_TRAILS) {
        WARN("Too many trails (%"PRIu64")", num_trails);
        return 1;
    }

    tdb_path(path, "%s/uuids", cons->root);
    SAFE_OPEN(state.out, path, "w");

    if (ftruncate(fileno(state.out), (off_t)(num_trails * 16))) {
        WARN("Could not init (%"PRIu64" trails): %s", num_trails, path);
        return -1;
    }

    j128m_fold(&cons->trails, store_uuids_fun, &state);

    SAFE_CLOSE(state.out, path);
    return 0;
}

static int is_fieldname_invalid(const char* field)
{
    uint64_t i;

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
                                     uint64_t num_ofields)
{
    Pvoid_t check = NULL;
    tdb_field i;
    Word_t tmp;

    for (i = 0; i < num_ofields; i++){
        Word_t *ptr;
        JSLI(ptr, check, (const uint8_t*)ofield_names[i]);
        if (*ptr){
            JSLFA(tmp, check);
            return 1;
        }
        *ptr = 1;
    }
    JSLFA(tmp, check);
    return 0;
}

tdb_cons *tdb_cons_new(const char *root,
                       const char **ofield_names,
                       uint64_t num_ofields)
{
    tdb_cons *cons;
    tdb_field i;
    int fd;

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

    j128m_init(&cons->trails);

    cons->root = strdup(root);

    cons->min_timestamp = UINT64_MAX;
    cons->num_ofields = num_ofields;
    cons->events.item_size = sizeof(tdb_cons_event);
    cons->items.item_size = sizeof(tdb_item);

    /* Opportunistically try to create the output directory.
       We don't care if it fails, e.g. because it already exists */
    mkdir(root, 0755);
    tdb_path(cons->tempfile, "%s/tmp.items.XXXXXX", root);
    if ((fd = mkostemp(cons->tempfile, 0)) == -1)
        goto error;

    if (!(cons->items.fd = fdopen(fd, "w")))
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
    uint64_t i;
    for (i = 0; i < cons->num_ofields; i++){
        if (cons->ofield_names)
            free(cons->ofield_names[i]);
        if (cons->lexicons)
            jsm_free(&cons->lexicons[i]);
    }
    free(cons->lexicons);
    if (cons->items.fd)
        fclose(cons->items.fd);
    if (cons->tempfile)
        unlink(cons->tempfile);
    if (cons->events.data)
        free(cons->events.data);
    if (cons->items.data)
        free(cons->items.data);

    j128m_free(&cons->trails);
    free(cons->ofield_names);
    free(cons->root);
    free(cons);
}

/*
Append an event in this cons.
TODO tests for append
*/
int tdb_cons_add(tdb_cons *cons,
                 const uint8_t uuid[16],
                 const uint64_t timestamp,
                 const char **values,
                 const uint64_t *value_lengths)
{
    tdb_field i;
    tdb_cons_event *event;
    Word_t *uuid_ptr;
    __uint128_t uuid_key;

    for (i = 0; i < cons->num_ofields; i++)
        if (value_lengths[i] > TDB_MAX_VALUE_SIZE)
            return 2;

    memcpy(&uuid_key, uuid, 16);
    uuid_ptr = j128m_insert(&cons->trails, uuid_key);

    event = (tdb_cons_event*)arena_add_item(&cons->events);

    event->item_zero = cons->items.next;
    event->num_items = 0;
    event->timestamp = timestamp;
    event->prev_event_idx = *uuid_ptr;
    *uuid_ptr = cons->events.next;

    if (timestamp < cons->min_timestamp)
        cons->min_timestamp = timestamp;

    for (i = 0; i < cons->num_ofields; i++){
        tdb_field field = (tdb_field)(i + 1);
        tdb_val val = 0;
        tdb_item item;
        /* TODO add a test for sparse trails */
        if (value_lengths[i]){
            if (!(val = (tdb_val)jsm_insert(&cons->lexicons[i],
                                            values[i],
                                            value_lengths[i])))
                return 1;

        }
        item = tdb_make_item(field, val);
        memcpy(arena_add_item(&cons->items), &item, sizeof(tdb_item));
        ++event->num_items;
    }
    return 0;
}

/*
Append a single event to this cons. The event is a usual srequence
of (remapped) items. Used by tdb_cons_append().
*/
static void append_event(tdb_cons *cons,
                         const tdb_item *items,
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
        if (items[field]){
            memcpy(arena_add_item(&cons->items),
                   &items[field],
                   sizeof(tdb_item));
            ++event->num_items;
        }
    }
}

/*
Append the lexicons of an existing TrailDB, db, to this cons. Used by
tdb_cons_append().
*/
static uint64_t **append_lexicons(tdb_cons *cons, const tdb *db)
{
    tdb_val **lexicon_maps;
    tdb_val i;
    tdb_field field;

    if (!(lexicon_maps = calloc(cons->num_ofields, sizeof(tdb_val*))))
        return NULL;

    for (field = 0; field < cons->num_ofields; field++){
        struct tdb_lexicon lex;
        uint64_t *map;

        tdb_lexicon_read(db, field + 1, &lex);

        if (!(map = lexicon_maps[field] = malloc(lex.size * sizeof(tdb_val))))
            goto err;

        for (i = 0; i < lex.size; i++){
            uint64_t value_length;
            const char *value = tdb_lexicon_get(&lex, i, &value_length);
            tdb_val val;
            if ((val = (tdb_val)jsm_insert(&cons->lexicons[field],
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
/* TODO append test */
int tdb_cons_append(tdb_cons *cons, const tdb *db)
{
    tdb_val **lexicon_maps = NULL;
    uint64_t trail_id;
    tdb_item *items;
    uint64_t i, e, n, items_len = 0;
    tdb_field field;
    int ret = 0;
    /* event_width includes the event delimiter, 0 byte */
    const uint64_t event_width = db->num_fields + 1;

    /* TODO: we could be much more permissive with what can be joined:
    we could support "full outer join" and replace all missing fields
    with NULLs automatically.
    */
    if (cons->num_ofields != db->num_fields - 1)
        /* TODO fix error codes */
        return -1;

    for (field = 0; field < cons->num_ofields; field++)
        if (strcmp(cons->ofield_names[field], tdb_get_field_name(db, field + 1)))
            /* TODO fix error codes */
            return -2;

    if (db->min_timestamp < cons->min_timestamp)
        cons->min_timestamp = db->min_timestamp;

    if (!(lexicon_maps = append_lexicons(cons, db)))
        goto err;

    for (trail_id = 0; trail_id < tdb_num_trails(db); trail_id++){
        __uint128_t uuid_key;
        Word_t *uuid_ptr;

        memcpy(&uuid_key, tdb_get_uuid(db, trail_id), 16);
        uuid_ptr = j128m_insert(&cons->trails, uuid_key);

        /* TODO this could use an iterator of the trail */
        if (tdb_get_trail(db, trail_id, &items, &items_len, &n, 0)){
            ret = -1;
            goto err;
        }
        for (e = 0; e < n; e += event_width){
            for (field = 1; field < cons->num_ofields + 1; field++){
                uint64_t idx = e + field;
                tdb_val val = tdb_item_val(items[idx]);
                if (val)
                    /* translate non-NULL vals */
                    val = lexicon_maps[field - 1][val - 1];
                items[idx] = tdb_make_item(field, val);
            }
            append_event(cons, &items[e], uuid_ptr);
        }
    }
err:
    if (lexicon_maps){
        for (i = 0; i < cons->num_ofields; i++)
            free(lexicon_maps[i]);
        free(lexicon_maps);
    }
    free(items);
    return ret;
}

int tdb_cons_finalize(tdb_cons *cons, uint64_t flags __attribute__((unused)))
{
    struct tdb_file items_mmapped = {};
    uint64_t num_events = cons->events.next;
    int ret = 0;

    /* finalize event items */
    arena_flush(&cons->items);
    if (fclose(cons->items.fd)) {
        cons->items.fd = NULL;
        WARN("Closing %s failed", cons->tempfile);
        return -1;
    }
    cons->items.fd = NULL;

    if (num_events && cons->num_ofields) {
        if (tdb_mmap(cons->tempfile, &items_mmapped, NULL)) {
            WARN("Mmapping %s failed", cons->tempfile);
            return -1;
        }
    }

    TDB_TIMER_DEF

    /* TODO fix error codes (ret) below */
    /* finalize lexicons */
    TDB_TIMER_START
    if (store_lexicons(cons)){
        ret = -1;
        goto error;
    }
    TDB_TIMER_END("encoder/store_lexicons")

    TDB_TIMER_START
    if (store_uuids(cons)){
        ret = -1;
        goto error;
    }
    TDB_TIMER_END("encoder/store_uuids")

    TDB_TIMER_START
    if (store_version(cons)){
        ret = -1;
        goto error;
    }
    TDB_TIMER_END("encoder/store_version")

    TDB_TIMER_START
    if ((ret = tdb_encode(cons, (tdb_item*)items_mmapped.data)))
        goto error;
    TDB_TIMER_END("encoder/encode")

error:
    if (items_mmapped.data)
        munmap((void*)items_mmapped.data, items_mmapped.size);
    return ret;
}
