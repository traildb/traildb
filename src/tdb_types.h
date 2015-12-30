
#ifndef __TDB_TYPES_H__
#define __TDB_TYPES_H__

#include <stdint.h>

#include "tdb_limits.h"

/*
   Internally we deal with ids:
    (uint64_t) trail_id  -> (16 byte) uuid
    (uint32_t) field     -> (0-terminated str) field_name
    (uint44_t) val       -> (bytes) value

   The complete picture looks like:

    uuid      -> trail_id
    trail_id  -> [event, ...]
    event     := [timestamp, item, ...]
    item      := (field, val)
    field     -> field_name
    val       -> value

   There are two types of tdb_items, narrow (32-bit) and wide (64-bit):

    Narrow item (32 bit):

    [ field | wide-flag | val ]
      7       1           24

    Wide item (64 bit):

    [ field | wide-flag | ext-field | ext-flag | val ]
      7       1           7           1          48
*/

typedef uint32_t tdb_field;
typedef uint64_t tdb_val;
typedef uint64_t tdb_item;

#define tdb_item_field32(item) (item & 127)
#define tdb_item_val32(item)   ((item >> 8) & UINT32_MAX)
#define tdb_item_is32(item)    (!(item & 128))

/*
static tdb_field tdb_item_field(tdb_item item) __attribute__((unused));
static tdb_val tdb_item_val(tdb_item item) __attribute__((unused));
*/

static inline tdb_field tdb_item_field(tdb_item item)
{
    if (tdb_item_is32(item))
        return (tdb_field)tdb_item_field32(item);
    else
        return (tdb_field)((item & 127) | (((item >> 8) & 127) << 7));
}

static inline tdb_val tdb_item_val(tdb_item item)
{
    if (tdb_item_is32(item))
        return (tdb_val)tdb_item_val32(item);
    else
        return (tdb_val)(item >> 16);
}

/*
static tdb_item tdb_make_item(tdb_field field,
                              tdb_val val) __attribute__((unused));
*/

static inline tdb_item tdb_make_item(tdb_field field, tdb_val val)
{
    /*
      here we assume that val < 2^48 and field < 2^14
    */
    if (field > TDB_FIELD32_MAX || val > TDB_VAL32_MAX){
        const uint64_t field1 = field & 127;
        const uint64_t field2 = (field >> 7) << 8;
        return field1 | 128 | field2 | (val << 16);
    }else
        return field | (val << 8);
}

#endif /* __TDB_TYPES_H__ */

