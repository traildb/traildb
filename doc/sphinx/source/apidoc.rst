==============
API Reference
==============

--------------
TrailDB construction
--------------

.. c:function:: tdb_cons *tdb_cons_new(const char *root, const char *ofield_names, uint32_t num_ofields)

    Create a TrailDB constructor object.

    :param root: path to new TrailDB.
    :param ofield_names: names of fields, each name terminated by a zero byte.
    :param num_ofields: number of fields.

    :return: Pointer to a TrailDB constructor object. Call :c:func:`tdb_cons_free` after constructing TrailDB.

.. c:function:: void tdb_cons_free(tdb_cons *cons)

    Free a TrailDB constructor object.

    :param cons: TrailDB constructor object.

.. c:function:: int tdb_cons_add(tdb_cons *cons, const uint8_t cookie[16], const uint32_t timestamp, const char *values)

    Add an event to a TrailDB.

    :param cons: TrailDB constructor object.
    :param cookie: 16-byte trail identifier.
    :param timestamp: integer timestamp. Usually unix time, but TrailDB makes no assumptions about how this should be interpreted
                      aside from ordering events within the trail by increasing timestamp.
    :param values: values of each field, as a list of zero-terminated strings, in the same order as specified
                   in a previous call to :c:func:`tdb_cons_new`.

    :return: zero on success.

.. c:function:: int tdb_cons_append(tdb_cons *cons, const tdb *db)

    Append one TrailDB to another.

    :param cons: TrailDB constructor object for the TrailDB to append to.
    :param db: TrailDB object for the TrailDB to append.

.. c:function:: int tdb_cons_finalize(tdb_cons *cons, uint64_t flags)

    Finalize TrailDB construction. Call this function to complete TrailDB creation and write it to disk.

    :param cons: TrailDB constructor object to finalize..
    :param flags: reserved for future use. Pass 0 for flags.

------------------------------
Opening and Closing TrailDBs
------------------------------

TrailDB functions are generally not thread safe. But multiple processes or threads can open the
same TrailDB at the same time, and memory overhead shouldn't be very high.

.. c:function:: tdb *tdb_open(const char *root)

    Open a TrailDB for reading.

    :param root: path to TrailDB.

    :return: Pointer to a TrailDB object. Call :c:func:`tdb_close` to free it after finishing working with TrailDB.

.. c:function:: void tdb_close(tdb *db);

    Close an open TrailDB.

.. _fields-values-functions:

--------------------------------------
Working with fields and values
--------------------------------------

.. c:function:: int tdb_get_field(tdb *db, const char *field_name)

    Get field id by name.

    :param db: TrailDB object.
    :param field_name: field name.

    :return: Integer field id or 255 if field cannot be found.

.. c:function:: const char *tdb_get_field_name(tdb *db, tdb_field field)

    Get field name by id.

    :param db: TrailDB object.
    :param field: integer field id.

    :return: Pointer to a null terminated field name, memory owned by the TrailDB object (caller doesn't have to free this pointer).

.. c:function:: int tdb_field_has_overflow_vals(tdb *db, tdb_field field)

    Check if field hit the 2^24 value limit and some values were not stored.

    :param db: TrailDB object.
    :param field: integer field id.

    :return: boolean result.

.. c:function:: tdb_item tdb_get_item(tdb *db, tdb_field field, const char *value)

    Construct tdb_item given field id and value.

    :param db: TrailDB object.
    :param field: integer field id.
    :param value: value as null terminated string

    :return: Encoded field/value pair

.. c:function:: const char *tdb_get_value(tdb *db, tdb_field field, tdb_val val)

    Get item value given field id and value id.

    :param db: TrailDB object.
    :param field: integer field id.
    :param val: integer value id.

    :return: Pointer to a null terminated value, memory owned by the TrailDB object (caller doesn't have to free this pointer).

.. c:function:: const char *tdb_get_item_value(tdb *db, tdb_item item)

    Get item value given encoded field/value pair.

    :param db: TrailDB object.
    :param item: encoded field/value pair.

    :return: Pointer to a null terminated value, memory owned by the TrailDB object (caller doesn't have to free this pointer).

.. c:function:: const uint8_t *tdb_get_cookie(const tdb *db, uint64_t cookie_id)

    Get cookie value by id.

    :param db: TrailDB object.
    :param cookie_id: cookie id.

    :return: Pointer to 16-byte cookie value, memory owned by the TrailDB object (caller doesn't have to free this pointer).

.. c:function:: uint64_t tdb_get_cookie_id(const tdb *db, const uint8_t cookie[16])

    Get cookie id by value.

    :param db: TrailDB object.
    :param cookie_id: cookie value as a 16-byte array.

    :return: Cookie id.

---------------
Decoding Trails
---------------
.. c:function:: uint32_t tdb_decode_trail(const tdb *db, uint64_t cookie_id, uint32_t *dst, uint32_t dst_size, int edge_encoded)

    Decode trail. This will use global filter, if set by :c:func:`tdb_set_filter`. This function will decode as many events as
    ``dst`` bufer fits; if trail is larger than that, return value would be equal to ``dst_size``. Common pattern when decoding trails
    is to reuse buffer for multiple calls of :c:func:`tdb_decode_trail` and resize it lazily when necessary. See example.

    :param db: TrailDB object.
    :param cookie_id: cookie id for the trail to decode.
    :param dst: buffer to decode to.
    :param dst_size: buffer size in events.
    :param edge_encoded: edge encoding mode boolean flag

    :return: Number of events decoded. If this number is equal to ``dst_size``, buffer wasn't big enough.

.. c:function:: uint32_t tdb_decode_trail_filtered(const tdb *db, uint64_t cookie_id, uint32_t *dst, uint32_t dst_size, int edge_encoded, const uint32_t *filter, uint32_t filter_len)

    A variant of :c:func:`tdb_decode_trail` with filter specified explicitly instead of using global filter set by :c:func:`tdb_set_filter`.

    :param db: TrailDB object.
    :param cookie_id: cookie id for the trail to decode.
    :param dst: buffer to decode to.
    :param dst_size: buffer size in events.
    :param edge_encoded: edge encoding mode boolean flag
    :param filter: filter specification (see Filter format)
    :param filter_len: filter size in 32-bit words

    :return: Number of events decoded. If this number is equal to ``dst_size``, buffer wasn't big enough.
--------------
Error Handling
--------------

.. c:function:: const char *tdb_error(const tdb *db)

    Get latest error message.

    :param db: TrailDB object.

    :return: Pointer to a null terminated error string, memory owned by the TrailDB object (caller doesn't have to free this pointer).

--------------
Stats
--------------

.. c:function:: uint64_t tdb_num_cookies(const tdb *db)

    Get number of cookies in a TrailDB.

    :param db: TrailDB object.
    :return: Number of cookies.

.. c:function:: uint64_t tdb_num_events(const tdb *db)

    Get number of events in a TrailDB.

    :param db: TrailDB object.
    :return: Number of events.

.. c:function:: uint32_t tdb_num_fields(const tdb *db)

    Get number of fields in a TrailDB, including timestamp.

    :param db: TrailDB object.
    :return: Number of fields.

.. c:function:: uint32_t tdb_min_timestamp(const tdb *db)

    Get minimum timestamp value for a TrailDB.

    :param db: TrailDB object.
    :return: Minimum timestamp value.

.. c:function:: uint32_t tdb_max_timestamp(const tdb *db)

    Get maximum timestamp value for a TrailDB.

    :param db: TrailDB object.
    :return: Maximum timestamp value.

----------------
Filter Functions
----------------

.. c:function:: int tdb_set_filter(tdb *db, const uint32_t *filter, uint32_t filter_len)

    Set global decoding filter that is later used by :c:func:`tdb_decode_trail`

    :param filter: filter specification (see Filter format). Can be NULL to remove filter.
    :param filter_len: filter size in 32-bit words

.. c:function:: const uint32_t *tdb_get_filter(const tdb *db, uint32_t *filter_len)

    Get global decoding filter that is later used by :c:func:`tdb_decode_trail`

    :param filter_len: Variable to store filter size in 32-bit words
    :return: Pointer to filter. Memory owned by the TrailDB object (caller doesn't have to free this pointer). 
             May be NULL if filter is not set.

-----------------
Utility Functions
-----------------

.. c:function:: int tdb_cookie_raw(const uint8_t hexcookie[32], uint8_t cookie[16])

    Converts cookie from a hexadecimal string to a binary representation.

.. c:function:: int tdb_cookie_hex(const uint8_t cookie[16], uint8_t hexcookie[32])

    Converts cookie from 16-byte binary representation to a hexadecimal string.


----------------
Filter Format
----------------

Functions :c:func:`tdb_decode_trail` and :c:func:`tdb_decode_trail_filtered` can be configured to filter out events
from trail that do not match a specified filter expression while decoding.

Filters are `conjuctive normal form boolean expressions <https://en.wikipedia.org/wiki/Conjunctive_normal_form>`_, that is::

    ((Field1 = X) OR (Field2 != Y ))  AND ((Field3 = Z) | (Field1 = N) | ...) ...

Filters are stored as arrays of ``uint32_t``, exact format described below.

Each inner expression of the form ``Field OP Value`` is stored as three values

.. code-block:: c

    struct {
        uint32_t op;        /* 0 for equal, 1 for not equal */
        uint32_t item;      /* field/value pair as returned by tdb_get_item() */
    } expr_t;

Each ``OR`` clause is stored as

.. code-block:: c

    struct {
        uint32_t num_exprs;
        expr_t exprs[num_exprs];
    } clause_t;

And entire filter is stored as

.. code-block:: c

    struct {
        uint32_t num_clauses;
        clause_t clauses[num_clauses];
    } clause_t;


----------------------
Decoded Trail Format
----------------------

Each trail is decoded to an array of ``uint32_t``.

Each event in the trail is represented by a

.. code-block:: c

    uint32_t timestamp;                  /* event timestamp */
    tdb_item field_vals[num_fields-1];   /* event field/value pairs */
    uint32_t zero;                       /* zero item */

Here ``num_fields`` is the value returned by :c:func:`tdb_num_fields`. That value includes timestamp, therefore there
are ``num_fields-1`` string-valued fields for each event. There's a zero item after each event for convenience when
scanning through the trail.

Each field is a key/value pair of field and value ids (``tdb_field`` and ``tdb_val``), you can convert to/from string
field names and corresponding numeric ids using functions from :ref:`fields-values-functions`.