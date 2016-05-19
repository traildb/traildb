
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <traildb.h>

int main(int argc, char **argv)
{
    const char *fields[] = {"username", "action"};
    tdb_error err;
    tdb_cons* cons = tdb_cons_init();

    if ((err = tdb_cons_open(cons, "tiny", fields, 2))){
        printf("Opening TrailDB constructor failed: %s\n", tdb_error_str(err));
        exit(1);
    }

    static char username[6];
    static uint8_t uuid[16];
    const char *EVENTS[] = {"open", "save", "close"};
    uint32_t i, j;

    /* create three users */
    for (i = 0; i < 3; i++){

        memcpy(uuid, &i, 4);
        sprintf(username, "user%d", i);

        /* every user has three events */
        for (j = 0; j < 3; j++){

            const char *values[] = {username, EVENTS[j]};
            uint64_t lengths[] = {strlen(username), strlen(EVENTS[j])};
            /* generate a dummy timestamp */
            uint64_t timestamp = i * 10 + j;

            if ((err = tdb_cons_add(cons, uuid, timestamp, values, lengths))){
                printf("Adding an event failed: %s\n", tdb_error_str(err));
                exit(1);
            }
        }
    }

    /* finalize the TrailDB */
    if ((err = tdb_cons_finalize(cons))){
        printf("Closing TrailDB constructor failed: %s\n", tdb_error_str(err));
        exit(1);
    }

    /* open the newly created TrailDB and print out its contents */

    tdb* db = tdb_init();
    if ((err = tdb_open(db, "tiny"))){
        printf("Opening TrailDB failed: %s\n", tdb_error_str(err));
        exit(1);
    }

    tdb_cursor *cursor = tdb_cursor_new(db);

    /* loop over all trails */
    for (i = 0; i < tdb_num_trails(db); i++){

        const tdb_event *event;
        uint8_t hexuuid[32];

        tdb_uuid_hex(tdb_get_uuid(db, i), hexuuid);
        printf("%.32s ", hexuuid);

        tdb_get_trail(cursor, i);

        /* loop over all events of this trail */
        while ((event = tdb_cursor_next(cursor))){
            printf("[ timestamp=%llu", event->timestamp);
            for (j = 0; j < event->num_items; j++){
                uint64_t len;
                const char *val = tdb_get_item_value(db, event->items[j], &len);
                printf(" %s=%.*s", fields[j], len, val);
            }
            printf(" ] ");
        }

        printf("\n");
    }

    tdb_cursor_free(cursor);
    tdb_close(db);
    return 0;
}
