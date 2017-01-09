
# Functions

[TOC]

# Construct a new TrailDB


### tdb_cons_init
Create a new TrailDB constructor handle.
```c
tdb_cons *tdb_cons_init(void)
```
Return NULL if memory allocation fails.

### tdb_cons_open
Open a new TrailDB.
```c
tdb_error tdb_cons_open(tdb_const *cons,
                        const char *root,
                        const char **ofield_names,
                        uint64_t num_ofields)
```
* `cons` constructor handle as returned from `tdb_cons_init`.
* `root` path to new TrailDB.
* `ofield_names` names of fields, each name terminated by a zero byte.
* `num_ofields` number of fields.

Return 0 on success, an error code otherwise.


### tdb_cons_close
Free a TrailDB constructor handle. Call this after [tdb_cons_finalize()](#tdb_cons_finalize).
```c
void tdb_cons_close(tdb_cons *cons)
```
* `cons` TrailDB constructor handle.


### tdb_cons_add
Add an event to TrailDB.
```c
tdb_error tdb_cons_add(tdb_cons *cons,
                       const uint8_t uuid[16],
                       const uint64_t timestamp,
                       const char **values,
                       const uint64_t *value_lengths)
```


* `cons` TrailDB constructor handle.
* `uuid` 16-byte UUID.
* `timestamp` integer timestamp. Usually Unix time.
* `values` values of each field, as an array of pointers to byte strings. The order of values
is the same as `ofield_names` in [tdb_cons_open()](#tdb_cons_open).
* `values_length` lengths of byte strings in `values`.

Return 0 on success, an error code otherwise.

### tdb_cons_append
Merge an existing TrailDB to this constructor. The fields must be equal
between the existing and the new TrailDB.
```c
tdb_error tdb_cons_append(tdb_cons *cons, const tdb *db)
```
* `cons` TrailDB constructor handle.
* `db` An existing TrailDB to be merged.

Return 0 on success, an error code otherwise.


### tdb_cons_set_opt
Set a constructor option.
```c
tdb_error tdb_cons_set_opt(tdb_cons *cons,
                           tdb_opt_key key,
                           tdb_opt_value value);
```
Currently the supported options are:

* key `TDB_OPT_CONS_OUTPUT_FORMAT`
    - value `TDB_OPT_CONS_OUTPUT_PACKAGE` create a one-file TrailDB (default).
    - value `TDB_OPT_CONS_OUTPUT_DIR` do not package TrailDB, keep a directory.

Return 0 on success, an error code otherwise.

### tdb_cons_get_opt
Get a constructor option.
```c
tdb_error tdb_cons_get_opt(tdb_cons *cons,
                           tdb_opt_key key,
                           tdb_opt_value *value);
```

See [tdb_cons_set_opt()](#tdb_cons_set_opt) for valid keys. Sets the `value`
to the current value of the key. Return 0 on success, an error code otherwise.


### tdb_cons_finalize
Finalize TrailDB construction. Finalization takes care of compacting the
events and creating a valid TrailDB file.
```c
tdb_error tdb_cons_finalize(tdb_cons *cons)
```

* `cons` TrailDB constructor handle.

Return 0 on success, an error code otherwise.


# Open a TrailDB and access metadata


### tdb_init
Create a new TrailDB handle.
```c
tdb *tdb_init(void)
```
Return NULL if memory allocation fails.


### tdb_open
Open a TrailDB for reading.
```c
tdb_error tdb_open(tdb *tdb, const char *root)
```

* `tdb` Traildb handle returned by [tdb_init()](#tdb_init).
* `root` path to TrailDB.

Return 0 on success, an error code otherwise.

### tdb_close
Close a TrailDB.
```c
void tdb_close(tdb *db)
```
* `db` TrailDB handle.

### tdb_dontneed
Inform the operating system that this TrailDB does not need to be kept in
memory.
```c
void tdb_dontneed(tdb *db)
```
* `db` TrailDB handle.

### tdb_willneed
Inform the operating system that this TrailDB will be accessed soon. Call
this after [tdb_dontneed()](#tdb_dontneed) once the TrailDB is needed again.
```c
void tdb_willneed(tdb *db)
```
* `db` TrailDB handle.

### tdb_num_trails
Get the number of trails.
```
uint64_t tdb_num_trails(const tdb *db)
```
* `db` TrailDB handle.


### tdb_num_events
Get the number of events.
```c
uint64_t tdb_num_events(const tdb *db)
```
* `db` TrailDB handle.


### tdb_num_fields
Get the number of fields.
```
uint64_t tdb_num_fields(const tdb *db)
```
* `db` TrailDB handle.


### tdb_min_timestamp
Get the oldest timestamp.
```
uint64_t tdb_min_timestamp(const tdb *db)
```
* `db` TrailDB handle.


### tdb_max_timestamp
Get the newest timestamp.
```c
uint64_t tdb_max_timestamp(const tdb *db)
```
* `db` TrailDB handle.


### tdb_version
Get the version.
```c
uint64_t tdb_version(const tdb *db)
```
* `db` TrailDB handle.


### tdb_error_str
Translate an error code to a string.
```
const char *tdb_error_str(tdb_error errcode)
```
Return a string description corresponding to the error code.
The string is owned by TrailDB so the caller does not need to free it.


### tdb_set_opt
Set a TrailDB option.
```c
tdb_error tdb_set_opt(tdb *db,
                      tdb_opt_key key,
                      tdb_opt_value value);
```
Currently the supported options are:

* key `TDB_OPT_ONLY_DIFF_ITEMS`
    - value: `0` - Cursors should return all items (default).
    - value: `1` - Cursors should return mostly distinct items.
* key `TDB_OPT_CURSOR_EVENT_BUFFER_SIZE`
    - value: `number of events` - Set the size of the cursor readahead buffer.
* key `TDB_OPT_EVENT_FILTER`
    - value: pointer to `const struct tdb_event_filter*` as returned
      by [tdb_event_filter_new()](#tdb_event_filter_new). This filter is
      applied to all new cursors created with the `db` handle, that is,
      [tdb_cursor_set_event_filter()](#tdb_cursor_set_event_filter) is
      called automatically. In effect, this defines a view (a subset of
      events) of `db`. The event filter must stay alive for the lifetime
      of the `db` handle.

Return 0 on success, an error code otherwise.

### tdb_get_opt
Get a TrailDB option.
```c
tdb_error tdb_get_opt(tdb *db,
                      tdb_opt_key key,
                      tdb_opt_value *value);
```

See [tdb_set_opt()](#tdb_set_opt) for valid keys. Sets the `value`
to the current value of the key. Return 0 on success, an error code otherwise.


# Working with items, fields and values

See [TrailDB Data Model](technical_overview/#data-model) for a
description of items, fields, and values.

For maximum performance, it is a good idea to use `tdb_item`s as
extensively as possible in your application when working with TrailDBs.
Convert items to strings only when really needed.


### tdb_item_field
Extract the field ID from an item.
```c
tdb_field tdb_item_field(tdb_item item)
```
* `item` an item.

Return a field ID.


### tdb_item_val
Extract the value ID from an item.
```c
tdb_val tdb_item_val(tdb_item item)
```
* `item` an item.

Return a value ID.


### tdb_make_item
Make an item given a field ID and a value ID.
```c
tdb_item tdb_make_item(tdb_field field, tdb_val val)
```
* `field` field ID.
* `val` value ID.

Return a new item.


### tdb_item_is32
Determine if an item can be safely cast to a 32-bit integer. You can
use this function to help to conserve memory by casting items to 32-bit
integers instead of default 64-bit items.
```c
int tdb_item_is32(tdb_item item)
```
* `item` an item

Return non-zero if you can cast this item to 32-bit integer without loss
of data.


### tdb_lexicon_size
Get the number of distinct values in the given field.
```c
uint64_t tdb_lexicon_size(const tdb *db, tdb_field field);
```
* `db` TrailDB handle.
* `field` field ID.

Returns the number of distinct values.


### tdb_get_field
Get the field ID given a field name.
```c
tdb_error tdb_get_field(tdb *db, const char *field_name, tdb_field *field)
```
* `db` TrailDB handle.
* `field_name` field name (zero-terminated string).
* `field` pointer to variable to store field ID in.

Return 0 on success, an error code otherwise (field not found).


### tdb_get_field_name
Get the field name given a field ID.
```c
const char *tdb_get_field_name(tdb *db, tdb_field field)
```
* `db` TrailDB handle.
* `field` field ID.

Return the field name or NULL if field ID is invalid. The string is
owned by TrailDB so the caller does not need to free it.


### tdb_get_item
Get the item corresponding to a value. Note that this is a relatively slow
operation that may need to scan through all values in the field.
```c
tdb_item tdb_get_item(tdb *db,
                      tdb_field field,
                      const char *value,
                      uint64_t value_length)
```
* `db` TrailDB handle.
* `field` field ID.
* `value` value byte string.
* `value_length` length of the value.

Return 0 if item was not found, a valid item otherwise.

### tdb_get_value
Get the value corresponding to a field ID and value ID pair.
```c
const char *tdb_get_value(tdb *db,
                          tdb_field field,
                          tdb_val val,
                          uint64_t *value_length)
```
* `db` TrailDB handle.
* `field` field ID.
* `val` value ID.
* `value_length` length of the returned byte string.

Return a byte string corresponding to the field-value pair or NULL if
value was not found. The string is owned by TrailDB so the caller does
not need to free it.


### tdb_get_item_value
Get the value corresponding to an item. This is a shorthand version of
[tdb_get_value()](#tdb_get_value).
```c
const char *tdb_get_item_value(tdb *db, tdb_item item, uint64_t *value_length)
```
* `db` TrailDB handle.
* `item` an item.
* `value_length` length of the returned byte string.

Return a byte string corresponding to the field-value pair or NULL if
value was not found. The string is owned by TrailDB so the caller does
not need to free it.


# Working with UUIDs

Each trail has a user-defined [16-byte UUID](http://en.wikipedia.org/wiki/UUID)
and a sequential 64-bit trail ID associated to it.

### tdb_get_uuid
Get the UUID given a trail ID. This is a fast O(1) operation.
```c
const uint8_t *tdb_get_uuid(const tdb *db, uint64_t trail_id)
```
* `db` TrailDB handle.
* `trail_id` trail ID (an integer between 0 and [tdb_num_trails()](#tdb_num_trails)).

Return a raw 16-byte UUID or NULL if trail ID is invalid.


### tdb_get_trail_id
Get the trail ID given a UUID. This is an O(log N) operation.
```c
tdb_error tdb_get_trail_id(const tdb *db,
                           const uint8_t uuid[16],
                           uint64_t *trail_id)
```
* `db` TrailDB handle.
* `uuid` a raw 16-byte UUID.
* `trail_id` output pointer to the trail ID.

Return 0 if UUID was found, an error code otherwise.


### tdb_uuid_raw
Translate a 32-byte hex-encoded UUID to a 16-byte UUID.
```c
tdb_error tdb_uuid_raw(const uint8_t hexuuid[32], uint8_t uuid[16])
```
* `hexuuid` source 32-byte hex-encoded UUID.
* `uuid` destination 16-byte UUID.

Return 0 on success, an error code if `hexuuid` is not a valid
hex-encoded string.


### tdb_uuid_hex
Translate a 16-byte UUID to a 32-byte hex-encoded UUID.
```
void tdb_uuid_hex(const uint8_t uuid[16], uint8_t hexuuid[32])
```
* `uuid` source 16-byte UUID.
* `hexuuid` destination 32-byte hex-encoded UUID.


# Query events with cursors


### tdb_cursor_new
Create a new cursor handle.
```c
tdb_cursor *tdb_cursor_new(const tdb *db)
```
* `db` TrailDB handle.

Return NULL if memory allocation fails.


### tdb_cursor_free
Free a cursor handle.
```c
void tdb_cursor_free(tdb_cursor *cursor)
```


### tdb_get_trail
Reset the cursor to the given trail ID.
```c
tdb_error tdb_get_trail(tdb_cursor *cursor, uint64_t trail_id)
```
* `cursor` cursor handle.
* `trail_id` trail ID (an integer between 0 and [tdb_num_trails()](#tdb_num_trails)).

Return 0 or an error code if trail ID is invalid.


### tdb_get_trail_length
Get the number of events remaining in this cursor.
```c
uint64_t tdb_get_trail_length(tdb_cursor *cursor);
```
* `cursor` cursor handle.

Return the number of events in this cursor. Note that this function consumes
the cursor. You need to reset it with [tdb_get_trail()](#tdb_get_trail) to
get more events.


### tdb_cursor_set_event_filter
Set an event filter for the cursor. See [filter events](#filter-events) for
more information about event filters.
```c
tdb_error tdb_cursor_set_event_filter(tdb_cursor *cursor,
                                      const struct tdb_event_filter *filter);
```
* `cursor` cursor handle.
* `filter` filter handle.

Return 0 on success or an error if this cursor does not support event
filtering (`TDB_OPT_ONLY_DIFF_ITEMS` is enabled).

Note that this function borrows `filter` so it needs to stay alive as long
as the cursor is being used. You can use the same `filter` in multiple cursors.


### tdb_cursor_unset_event_filter
Remove an event filter from the cursor.
```c
void tdb_cursor_unset_event_filter(tdb_cursor *cursor);
```
* `cursor` cursor handle.


### tdb_cursor_next
Consume the next event from the cursor.
```c
const tdb_event *tdb_cursor_next(tdb_cursor *cursor)
```
* `cursor` cursor handle.

Return an event struct or NULL if the cursor has no more events. The
event structure is defined as follows:

```c
typedef struct{
    uint64_t timestamp;
    uint64_t num_items;
    const tdb_item items[0];
} tdb_event;
```

`tdb_event` represents one event in the trail. Each event has a timestamp,
and a number of field-value pairs, encoded as items.


### tdb_cursor_peek
Return the next event from the cursor without consuming it.
```c
const tdb_event *tdb_cursor_peek(tdb_cursor *cursor)
```
* `cursor` cursor handle.

See [tdb_cursor_next](#tdb_cursor_next) for more details about `tdb_event`.


# Join trails with multi-cursors

A multi-cursor merges multiple trails represented by `tdb_cursor`
together to produce a single merged trail that has its events sorted in
the ascending timestamp order. The trails can originate from a single
TrailDB or multiple separate TrailDBs. In effect, a multi-cursor performs
efficient merge sort of the underlying trails on the fly.

You need to initialize all underlying `tdb_cursor`s to point at the
desired trails with [tdb_get_trail](#tdb_get_trail) as usual. Then,
call [tdb_multi_cursor_reset](#tdb_multi_cursor_reset) to reset the
multi-cursor state. After this, you can iterate over the joined
trail with [tdb_multi_cursor_next](#tdb_multi_cursor_next), event
by event, or get multiple joined events with a single call using
[tdb_multi_cursor_next_batch](#tdb_multi_cursor_next_batch). You can
repeat these steps for arbitrarily many trails using the same handles.

### tdb_multi_cursor_new
Create a new multi-cursor handle.
```c
tdb_multi_cursor *tdb_multi_cursor_new(tdb_cursor **cursors, uint64_t num_cursors)
```
* `cursors` a list of cursors to be merged.
* `num_cursors` number of cursors in `cursors`

Return NULL if memory allocation fails.


### tdb_multi_cursor_free
Free a multi-cursor handle.
```c
void tdb_multi_cursor_free(tdb_multi_cursor *mcursor)
```
* `mcursor` a multi-cursor handle


### tdb_multi_cursor_reset
Reset a multi-cursor handle to reflect the state of the underlying cursors. Call
this function every time after [tdb_get_trail](#tdb_get_trail).
```c
void tdb_multi_cursor_reset(tdb_multi_cursor *mcursor);
```
* `mcursor` a multi-cursor handle


### tdb_multi_cursor_next
Consume the next event, in the ascending timestamp order, from the underlying
cursors.
```c
const tdb_multi_event *tdb_multi_cursor_next(tdb_multi_cursor *mcursor)
```
* `mcursor` a multi-cursor handle

Return a multi event struct or NULL if the cursor has no more events. The
multi event structure is defined as follows:

```c
typedef struct{
    const tdb *db;
    const tdb_event *event;
    uint64_t cursor_idx;
} tdb_multi event;
```

`db` is a TrailDB handle to the TrailDB that contains this `event`.
See [tdb_cursor_next](#tdb_cursor_next) for more details about
`tdb_event`. Use the `db` handle to translate `event->items` to
values. The `cursor_idx` index points to the array of cursors given in
[tdb_multi_cursor_new](#tdb_multi_cursor_new).

### tdb_multi_cursor_next_batch
An optimized version of [tdb_multi_cursor_next](#tdb_multi_cursor_next).
Instead of returning a single event, this function returns an array of
events with a single function call.
```c
uint64_t tdb_multi_cursor_next_batch(tdb_multi_cursor *mcursor,
                                     tdb_multi_event *events,
                                     uint64_t max_events);
```
* `mcursor` a multi-cursor handle
* `events` a pre-allocated array of `tdb_multi_event` structs
* `max_events` size of the `events` array

Returns the number of events added to `events`, at most `max_events`.
If the value returned is 0, all events have been exhausted. See
[tdb_multi_cursor_next](#tdb_multi_cursor_next) for the definition of
`tdb_multi_event`.

Note that the pointers in the `events` array are valid only until the
next call to one of the multi-cursor functions. If you want to persist
the underlying events, you should copy them to another data structure.


### tdb_multi_cursor_peek
Return the next event, in the ascending timestamp order, from the underlying
cursors without consuming it.
```c
const tdb_multi_event *tdb_multi_cursor_peek(tdb_multi_cursor *mcursor);
```
* `mcursor` a multi-cursor handle

See [tdb_multi_cursor_next](#tdb_multi_cursor_next) for the definition
of `tdb_multi_event`.


# Filter events

An event filter is a boolean query over fields, expressed in [conjunctive normal
form](http://en.wikipedia.org/wiki/Conjunctive_normal_form).

Once [assigned to a cursor](#tdb_cursor_set_event_filter), only the
subset of events that match the query are returned. See [technical
overview](technical_overview/#return-a-subset-of-events-with-event-filte
rs) for more information.


### tdb_event_filter_new
Create a new event filter handle.
```c
struct tdb_event_filter *tdb_event_filter_new(void)
```
Return NULL if memory allocation fails.


### tdb_event_filter_free
Free an event filter handle.
```c
void tdb_event_filter_free(struct tdb_event_filter *filter)
```

### tdb_event_filter_add_term
Add a term (item) in the query. This item is attached to the
current clause with OR. You can make the item negative by setting
`is_negative` to non-zero.
```c
tdb_error tdb_event_filter_add_term(struct tdb_event_filter *filter,
                                    tdb_item term,
                                    int is_negative)
```
* `filter` filter handle.
* `term` an item to be included in the clause.
* `is_negative` is this item negative?

Return 0 on success, an error code otherwise (out of memory).

### tdb_event_filter_add_time_range
Add a time-range term to the query. This item is attached to the
current clause with OR. Finds events with timestamp `t` such that
`start_time <= t < end_time`.
```c
tdb_error tdb_event_filter_add_time_range(struct tdb_event_filter *filter,
                                          uint64_t start_time,
                                          uint64_t end_time)
```
* `filter` filter handle.
* `start_time` (inclusive) start of time range
* `end_time` (exclusive) end of time range

Return 0 on success, an error code otherwise (out of memory or invalid time range).


### tdb_event_filter_new_clause
Add a new clause in the query. The new clause is attached to the
query with AND.
```c
tdb_error tdb_event_filter_new_clause(struct tdb_event_filter *filter)
```
* `filter` filter handle.

Return 0 success, an error code otherwise (out of memory).


### tdb_event_filter_num_clauses
Get the number of clauses in this filter.
```c
uint64_t tdb_event_filter_num_clauses(const struct tdb_event_filter *filter);
```
* `filter` filter handle.

Return the number of clauses. Note that a new filter has one clause by
default, so the return value is always at least one.


### tdb_event_filter_get_item
Get an item added to this filter.
```c
tdb_error tdb_event_filter_get_item(const struct tdb_event_filter *filter,
                                    uint64_t clause_index,
                                    uint64_t item_index,
                                    tdb_item *item,
                                    int *is_negative)
```
* `filter` filter handle.
* `clause_index` clause index: `0 < clause_index < tdb_event_filter_num_clauses()`.
* `item_index` item index in the clause.
* `item` returned item.
* `is_negative` return 1 if the item negative, 0 otherwise, as set in [tdb_event_filter_add_term](#tdb_event_filter_add_term).

Returns 0 if an item was found at this location, `TDB_ERR_NO_SUCH_ITEM` otherwise. Note
that empty clauses always return `TDB_ERR_NO_SUCH_ITEM` although the clauses themselves
are valid.

Here is an example how to deconstruct a filter back to clauses and items:
```c
for (clause = 0; clause < tdb_event_filter_num_clauses(filter); clause++){
    uint64_t item, idx = 0;
    int is_negative;
    while (!tdb_event_filter_get_item(filter, clause, idx, &item, &is_negative)){
        /* do something with 'item' in 'clause' */
        ++idx;
    }
}
```

