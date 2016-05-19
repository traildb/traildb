
This tutorial expects that you have TrailDB installed and working. If you haven't
installed TrailDB yet, see [Getting Started](getting_started) for instructions.

# Part I: Create a simple TrailDB

In this example, we will create a tiny TrailDB that includes events
from three users. You can find the full Python source code in the
[traildb-python](https://github.com/SemanticSugar/traildb-python/tree/ma
ster/examples/tutorial_simple_traildb.py)
repo and the C source in the [main traildb
repo](https://github.com/SemanticSugar/traildb/tree/master/examples/tuto
rial_simple_traildb.c).

Note that opening a new TrailDB constructor fails if there is an
existing TrailDB with the same name. If you run this example multiple
times, you should delete the `tiny` directory, which may contain partial
results, and `tiny.tdb` before running the example.

First, let's create a new constructor that we will use to populate the TrailDB.
The TrailDB will have two fields, `username` and `action`, which we will specify
when creating the constructor.

<div markdown data-multilang title="Create a new TrailDB constructor">
```Python
from traildb import TrailDBConstructor, TrailDB
from uuid import uuid4
from datetime import datetime

cons = TrailDBConstructor('tiny', ['username', 'action'])
```
```C
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
```
</div>

Now we can populate the TrailDB with events. We are going
to create three dummy users, each of which will have three
events. Note that the primary key identifying the user is a
[UUID](http://en.wikipedia.org/wiki/UUID). We can use the `uuid` module
in Python to generate UUIDs, or you can create your own identifiers like
the C code does.

<div markdown data-multilang title="Add events in the TrailDB">
```Python
for i in range(3):
    uuid = uuid4().hex
    username = 'user%d' % i
    for day, action in enumerate(['open', 'save', 'close']):
        cons.add(uuid, datetime(2016, i + 1, day + 1), (username, action))
```
```C
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
```
</div>

Once you are done adding events in the TrailDB, you have to finalize it.
Finalization takes of compacting the events and creating a valid TrailDB file.

<div markdown data-multilang title="Finalize the TrailDB">
```Python
cons.finalize()
```
```C
    if ((err = tdb_cons_finalize(cons))){
        printf("Closing TrailDB constructor failed: %s\n", tdb_error_str(err));
        exit(1);
    }
```
</div>

You can check the contents of the new TrailDB using the `tdb` tool by
running `tdb dump -i tiny`. We can easily print out its contents using
the API too:

<div markdown data-multilang title="Print out contents of the new TrailDB">
```Python
for uuid, trail in TrailDB('tiny').trails():
    print uuid, list(trail)
```
```C
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
```
</div>

That's it! You can easily extend this example for creating TrailDBs based on event
sources of your own.

# Part II: Analyze a large TrailDB

Exercise: Can you detect bots?
