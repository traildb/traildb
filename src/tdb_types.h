
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

    [ field | wide-flag | ext-field | ext-flag | val | reserved ]
      7       1           7           1          40    8

    'ext-flag' and 'reserved' are reserved for future needs. Note that
    timestamp items may use 47 bits, i.e. they have only 7 bits reserved
    space.
*/

typedef uint32_t tdb_field;
typedef uint64_t tdb_val;
typedef uint64_t tdb_item;

typedef struct _tdb_cons tdb_cons;
typedef struct _tdb tdb;

typedef struct __attribute__((packed)){
    uint64_t timestamp;
    uint64_t num_items;
    const tdb_item items[0];
} tdb_event;

typedef struct{
    struct tdb_decode_state *state;
    const char *next_event;
    uint64_t num_events_left;
} tdb_cursor;

#define tdb_item_field32(item) (item & 127)
#define tdb_item_val32(item)   ((item >> 8) & UINT32_MAX)
#define tdb_item_is32(item)    (!(item & 128))

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

typedef enum{

    /* reading */
    TDB_OPT_ONLY_DIFF_ITEMS = 100,
    TDB_OPT_EVENT_FILTER = 101,
    TDB_OPT_CURSOR_EVENT_BUFFER_SIZE = 102,

    /* writing */
    TDB_OPT_CONS_OUTPUT_FORMAT = 1001

} tdb_opt_key;

typedef union{
    const void *ptr;
    uint64_t value;
} tdb_opt_value;

static const tdb_opt_value TDB_TRUE __attribute__((unused)) = {.value = 1};
static const tdb_opt_value TDB_FALSE __attribute__((unused)) = {.value = 0};

#define opt_val(x) ((tdb_opt_value){.value = x})
#define TDB_OPT_CONS_OUTPUT_FORMAT_DIR 0
#define TDB_OPT_CONS_OUTPUT_FORMAT_PACKAGE 1

#endif /* __TDB_TYPES_H__ */

