# API Reference


-------------------------
TrailDB construction
-------------------------


### tdb_cons_init
```c
tdb_cons *tdb_cons_init(void)
```

Create a traildb constructor object.

### tdb_cons_open
```c
tdb_error tdb_cons_open(tdb_const *cons, const char *root, const char **ofield_names, uint64_t num_ofields)
```

Start creating a new TrailDB.

* `cons` constructor object as returned from `tdb_cons_init`.
* `root` path to new TrailDB.
* `ofield_names` names of fields, each name terminated by a zero byte.
* `num_ofields` number of fields.

Returns error code. Call `tdb_cons_finalize` after constructing TrailDB.

### tdb_cons_close
```c
void tdb_cons_close(tdb_cons *cons)
```

Free a TrailDB constructor object.

* `cons` TrailDB constructor object.

### tdb_cons_add

```c
int tdb_cons_add(tdb_cons *cons,
                 const uint8_t uuid[16], 
                 const uint64_t timestamp,
                 const char **values,
                 const uint64_t *value_lengths)
```

Add an event to a TrailDB.

* `cons` TrailDB constructor object.
* `uuid` 16-byte trail identifier.
* `timestamp` integer timestamp. Usually unix time, but TrailDB makes no assumptions about how this should be interpreted aside from ordering events within the trail by increasing timestamp.
* `values` values of each field, as an array of pointers to byte strings. Number of fields and their order are defined by the call to `tdb_cons_open`; that is, `values` should contain `num_ofields` items as specified in the `tdb_cons_open`.
* `values_length` lengths of byte strings in `values`.

Returns error code.

### tdb_cons_append
```c
int tdb_cons_append(tdb_cons *cons, const tdb *db)
```

Append one TrailDB to another.

* `cons` TrailDB constructor object for the TrailDB to append to.
* `db` TrailDB object for the TrailDB to append.

### tdb_cons_finalize
```c
int tdb_cons_finalize(tdb_cons *cons, uint64_t flags)
```

Finalize TrailDB construction. Call this function to complete TrailDB creation and write it to disk.

* `cons` TrailDB constructor object to finalize..
* `flags` reserved for future use. Pass 0 for flags.

------------------------------
Opening and Closing TrailDBs
------------------------------

TrailDB functions themselves are generally not thread safe. But multiple processes or threads can open the
same TrailDB and read concurrently.

### tdb_init
```c
tdb *tdb_init(void)
```

Create a traildb object.

### tdb_open
```c
tdb_error tdb_open(tdb *tdb, const char *root)
```

Open a TrailDB for reading.

* `tdb` traildb object created by `tdb_init`
* `root` path to TrailDB.


Returns an error code. Call `tdb_close` to free it after finishing working with TrailDB.

### tdb_close
```c
void tdb_close(tdb *db)
```

Close an open TrailDB.


--------------------------------------
Working with fields and values
--------------------------------------

### tdb_get_field
```c
tdb_error tdb_get_field(tdb *db, const char *field_name, tdb_field *field)
```

Get field id by name.

* `db` TrailDB object.
* `field_name` field name.
* `field` pointer to variable to store field id to

Returns error code.

### tdb_get_field_name

```c
const char *tdb_get_field_name(tdb *db, tdb_field field)
```

Get field name by id.

* `db` TrailDB object.
* `field` integer field id.

Returns a pointer to a null terminated field name, memory owned by the TrailDB object (caller doesn't have to free this pointer).

### tdb_get_item

```c
tdb_item tdb_get_item(tdb *db, tdb_field field, const char *value, uint64_t value_length)
```

Construct tdb_item given field id and value.

* `db` TrailDB object.
* `field` integer field id.
* `value` value as a byte string
* `value_length` value size in bytes

Returns encoded field/value pair as `tdb_item`.

### tdb_get_value
```c
const char *tdb_get_value(tdb *db, tdb_field field, tdb_val val, uint64_t *value_length)
```

Get item value given field id and value id.

* `db` TrailDB object.
* `field` integer field id.
* `val` integer value id.

Returns pointer to the value, memory owned by the TrailDB object (caller doesn't have to free this pointer). Value length is stored at the location pointed at by `value_length`.

### tdb_get_item_value
```c
const char *tdb_get_item_value(tdb *db, tdb_item item, uint64_t *value_length)
```

Get item value given encoded field/value pair.

* `db` TrailDB object.
* `item` encoded field/value pair.

Returns pointer to the value, memory owned by the TrailDB object (caller doesn't have to free this pointer). Value length is stored at the location pointed at by `value_length`.

### tdb_get_uuid
```c
const uint8_t *tdb_get_cookie(const tdb *db, uint64_t trail_id)
```

Get trail uuid value by id.

* `db` TrailDB object.
* `trail_id` trail id.

Returns a pointer to 16-byte trail uuid, memory owned by the TrailDB object (caller doesn't have to free this pointer).

### tdb_get_trail_id
```c
tdb_error tdb_get_cookie_id(const tdb *db, const uint8_t uuid[16], uint64_t *trail_id)
```

Get trail id by uuid.

* `db` TrailDB object.
* `cookie_id` trail uuid as a 16-byte array.

Returns error code.

---------------
Decoding Trails
---------------

### tdb_cursor_new
```c
tdb_cursor *tdb_cursor_new(const tdb *db)
```

Create a new cursor object.

* `db` TrailDB object

### tdb_cursor_free
```c
void tdb_cursor_free(tdb_cursor *cursor)
```

Free a cursor object.


### tdb_get_trail
```c
tdb_error tdb_get_trail(tdb_cursor *cursor, uint64_t trail_id)
```

Find trail in traildb by id and initialize cursor to iterate through it. Note that trail ids are sequential; that is, trails within one traildb have integer ids starting from `0`. Therefore, to iterate through all the trails you can simply `tdb_get_trail` in a loop, for id values from `0` up to the value returned by `tdb_num_trails`.

* `cursor` cursor object as returned by `tdb_cursor_new`
* `trail_id` trail id

### tdb_get_trail_length
```c
uint64_t tdb_get_trail_length(tdb_cursor *cursor);
```

Convenience function to get trail length. It simply goes through entire trail as if you'd call `tdb_cursor_next` 
repeatedly, and therefore has O(n) complexity. Note that `cursor` object passed to it will be moved to the end of the trail in the process.

* `cursor` cursor object pointing at a trail.

### tdb_cursor_next
```c
const tdb_event *tdb_cursor_next(tdb_cursor *cursor)
```

Move cursor to the next event in the trail pointed at by `cursor`. If there are no more events, returns `NULL`. Otherwise returns a pointer to `tdb_event` structure.

```
typedef struct __attribute__((packed)){
    uint64_t timestamp;
    uint64_t num_items;
    const tdb_item items[0];
} tdb_event;
```

`items` array contains field/value pairs for the event.

--------------
Error Handling
--------------

### tdb_error_str
```
const char *tdb_error_str(tdb_error errcode)
```

Get error code string by error code.

* `errcode` error code.

Returns pointer to a null terminated error string, memory owned by the TrailDB object (caller doesn't have to free this pointer).

--------------
Stats
--------------

### tdb_num_trails

```
uint64_t tdb_num_trails(const tdb *db)
```

Get number of traild in a TrailDB.

* `db` TrailDB object.


### tdb_num_events
```c
uint64_t tdb_num_events(const tdb *db)
```

Get number of events in a TrailDB.

* `db` TrailDB object.

### tdb_num_fields
```
uint64_t tdb_num_fields(const tdb *db)
```

Get number of fields in a TrailDB. This number includes timestamp.

* `db` TrailDB object.

### tdb_min_timestamp
```
uint64_t tdb_min_timestamp(const tdb *db)
```

Get smallest timestamp value among all events stored in a TrailDB.

* `db` TrailDB object.

### tdb_max_timestamp
```c
uint64_t tdb_max_timestamp(const tdb *db)
```

Get largest timestamp value among all events stored in a TrailDB.

* `db` TrailDB object.


-----------------
Utility Functions
-----------------

### tdb_uuid_raw
```
int tdb_uuid_raw(const uint8_t hexuuid[32], uint8_t uuid[16])
```

Converts uuid from a hexadecimal string to a binary representation.

### tdb_uuid_hex
```
int tdb_uuid_hex(const uint8_t uuid[16], uint8_t hexuuid[32])
```

Converts uuid from 16-byte binary representation to a hexadecimal string.


