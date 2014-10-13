
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <cmph.h>

#include "discodb.h"
#include "tdb_internal.h"
#include "util.h"

#define DECODE_BUF_INCREMENT 1000000
#define KEY_BUFFER_SIZE 1000000

#define MIN_NUMBER_OF_COOKIES 100

struct cookie_ctx{
    uint32_t id;
    tdb *db;
    char key[16];
};

void ci_dispose(void *data, char *key, cmph_uint32 l)
{ /* no-op */
}

void ci_rewind(void *data)
{
    struct cookie_ctx *ctx = (struct cookie_ctx*)data;
    ctx->id = 0;
}

int ci_read(void *data, char **p, cmph_uint32 *len)
{
    struct cookie_ctx *ctx = (struct cookie_ctx*)data;
    tdb_cookie cookie = tdb_get_cookie(ctx->db, ctx->id);

    memcpy(ctx->key, cookie, 16);
    *p = ctx->key;
    *len = 16;

    ++ctx->id;
    return *len;
}

void create_cookie_index(const char *path, tdb *db)
{
    void *data;
    cmph_config_t *config;
    cmph_t *cmph;
    FILE *out;
    uint32_t size;

    struct cookie_ctx ctx = {
        .id = 0,
        .db = db
    };

    cmph_io_adapter_t r = {
        .data = &ctx,
        .nkeys = tdb_num_cookies(db),
        .read = ci_read,
        .dispose = ci_dispose,
        .rewind = ci_rewind
    };

    if (!(out = fopen(path, "w")))
        DIE("Could not open output file at %s\n", path);

    if (!(config = cmph_config_new(&r)))
        DIE("cmph_config failed\n");

    cmph_config_set_algo(config, CMPH_CHM);

    if (getenv("DEBUG_CMPH"))
        cmph_config_set_verbosity(config, 5);

    if (!(cmph = cmph_new(config)))
        DIE("cmph_new failed\n");

    size = cmph_packed_size(cmph);
    if (!(data = malloc(size)))
        DIE("Could not malloc a hash of %u bytes\n", size);

    cmph_pack(cmph, data);
    SAFE_WRITE(data, size, path, out);
    SAFE_CLOSE(out, path);

    cmph_destroy(cmph);
    cmph_config_destroy(config);
    free(data);
}

static void make_key(struct ddb_entry *key, tdb *db, tdb_item item)
{
    static char keybuf[KEY_BUFFER_SIZE];
    const char *value = tdb_get_item_value(db, item);
    tdb_field field = tdb_item_field(item);

    key->data = keybuf;
    key->length = snprintf(keybuf, KEY_BUFFER_SIZE, "%u:%s", field, value);

    if (key->length >= KEY_BUFFER_SIZE)
        DIE("Key too long. Increase KEY_BUFFER_SIZE\n");
}

static void add_trails(tdb *db, struct ddb_cons *cons)
{
    uint32_t *buf = NULL;
    uint32_t buf_size = 0;
    uint32_t i, j;
    uint32_t num_cookies = tdb_num_cookies(db);

    for (i = 0; i < num_cookies; i++){
        const struct ddb_entry value = {.data = (const char*)&i, .length = 4};
        uint32_t n;
        Pvoid_t uniques = NULL;
        int tmp;
        Word_t field_value;

        /* decode trail - no need to edge decode since we are only interest
           in unique field values */
        while ((n = tdb_decode_trail(db, i, buf, buf_size, 1)) == buf_size){
            free(buf);
            buf_size += DECODE_BUF_INCREMENT;
            if (!(buf = malloc(buf_size * 4)))
                DIE("Could not allocate decode buffer of %u items", buf_size);
        }

        /* remove duplicates */
        for (j = 1; j < n; j++){
            /* exclude end-of-event markers */
            if (buf[j]){
                /* exclude null values */
                if (tdb_item_val(buf[j]))
                    J1S(tmp, uniques, buf[j]);
            }else
                /* exclude timestamps */
                ++j;
        }

        /* add unique field values for this cookie in the index */
        J1F(tmp, uniques, field_value);
        while (tmp){
            struct ddb_entry key;
            make_key(&key, db, field_value);
            if (ddb_cons_add(cons, &key, &value))
                DIE("Could not add key (cookie %u)\n", i);
            J1N(tmp, uniques, field_value);
        }
        J1FA(field_value, uniques);

        if (!(i & 65535))
            fprintf(stderr,
                    "%u/%u (%2.1f%%) trails indexed\n",
                    i,
                    num_cookies,
                    100. * i / num_cookies);
    }
    free(buf);
}

static void create_trail_index(const char *path, tdb *db)
{
    struct ddb_cons *cons;
    uint64_t length;
    char *data;
    FILE *out;

    if (!(out = fopen(path, "w")))
        DIE("Could not open output file at %s\n", path);

    if (!(cons = ddb_cons_new()))
        DIE("Could not allocate discodb\n");

    add_trails(db, cons);

    if (!(data = ddb_cons_finalize(cons, &length, DDB_OPT_UNIQUE_ITEMS)))
        DIE("Finalizing index failed\n");

    SAFE_WRITE(data, length, path, out);
    SAFE_CLOSE(out, path);

    ddb_cons_free(cons);
    free(data);
}

int main(int argc, char **argv)
{
    char path[TDB_MAX_PATH_SIZE];
    tdb *db;
    TDB_TIMER_DEF

    if (argc < 2)
        DIE("USAGE: %s db-root\n", argv[0]);

    if (!(db = tdb_open(argv[1])))
        DIE("Could not load traildb\n");

    if (db->error_code)
        DIE("%s\n", db->error);

    TDB_TIMER_START
    tdb_path(path, "%s/trails.index", argv[1]);
    create_trail_index(path, db);
    TDB_TIMER_END("trails.index");

    /*
    CMPH may fail for a very small number of keys. We don't need an
    index for a small number of cookies anyways, so let's not even try.
    */
    if (tdb_num_cookies(db) > MIN_NUMBER_OF_COOKIES){
        TDB_TIMER_START
        tdb_path(path, "%s/cookies.index", argv[1]);
        create_cookie_index(path, db);
        TDB_TIMER_END("cookies.index");
    }

    tdb_close(db);
    return 0;
}
