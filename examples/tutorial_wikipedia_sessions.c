
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <traildb.h>

#define SESSION_LIMIT (30 * 60) /* at least 30 minutes between edits */

int main(int argc, char **argv)
{
    if (argc < 2){
        printf("tutorial_wikipedia_sessions <wikipedia-history.tdb>\n");
        exit(1);
    }

    tdb* db = tdb_init();
    tdb_error err;

    if ((err = tdb_open(db, argv[1]))){
        printf("Opening TrailDB failed: %s\n", tdb_error_str(err));
        exit(1);
    }

    tdb_cursor *cursor = tdb_cursor_new(db);
    uint64_t i;
    for (i = 0; i < tdb_num_trails(db); i++){
        const tdb_event *event;
        tdb_get_trail(cursor, i);

        event = tdb_cursor_next(cursor);
        uint64_t prev_time = event->timestamp;
        uint64_t num_sessions = 1;
        uint64_t num_events = 1;

        while ((event = tdb_cursor_next(cursor))){
            if (event->timestamp - prev_time > SESSION_LIMIT)
                ++num_sessions;
            prev_time = event->timestamp;
            ++num_events;
        }

        printf("Trail[%llu] Number of Sessions: %llu Number of Events: %llu\n",
               i,
               num_sessions,
               num_events);
    }

    tdb_cursor_free(cursor);
    tdb_close(db);
    return 0;
}
