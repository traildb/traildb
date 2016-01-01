
#ifndef __TDB_LIMITS_H__
#define __TDB_LIMITS_H__

#include <stdint.h>

/* these are kept in stack, so they shouldn't be overly large */
#define TDB_MAX_PATH_SIZE   2048
#define TDB_MAX_FIELDNAME_LENGTH 512
#define TDB_MAX_ERROR_SIZE  (TDB_MAX_PATH_SIZE + 512)

/* MAX_NUM_TRAILS * 16 must fit in off_t (long) type */
#define TDB_MAX_NUM_TRAILS  ((1LLU << 59) - 1)

/*
we need bit-level offsets to trails: At worst each item takes 64 bits,
so the theoretical max is 2^64 / 2^6 = 2^58. To make things a bit safer,
we set the max to 2^50.
*/
#define TDB_MAX_TRAIL_LENGTH ((1LLU << 50) - 1)

/* re: fields and values below, see tdb_types.h */

/* re: -2, one field is always the special 'time' field */
#define TDB_MAX_NUM_FIELDS ((1LLU << 14) - 2)

/* re: -2, one value is always the special NULL value */
#define TDB_MAX_NUM_VALUES ((1LLU << 40) - 2)

/*
timestamps have less future proofing than values, so TBD_MAX_TIMEDELTA can
be higher than TDB_MAX_NUM_VALUES, see tdb_types.h for details
*/
#define TDB_MAX_TIMEDELTA ((1LLU << 47) - 1)

/* 32-bit narrow items */
#define TDB_FIELD32_MAX 127
#define TDB_VAL32_MAX   ((1LLU << 24) - 1)

/* This is an arbitary value as long as it fits into stack comfortably */
#define TDB_MAX_VALUE_SIZE  (1LLU << 10)
/* TODO make TDB_MAX_LEXICON_SIZE limit 64-bit */
#define TDB_MAX_LEXICON_SIZE UINT32_MAX

/*
#define TDB_MAX_TIMEDELTA  ((1LLU << 24) - 2) // ~194 days
#define TDB_FAR_TIMEDELTA  ((1LLU << 24) - 1)
#define TDB_FAR_TIMESTAMP    UINT32_MAX
#define TDB_OVERFLOW_VALUE ((1LLU << 24) - 1)

*/

/* Support a character set that allows easy urlencoding.
   These characters are used in filenames, so better to be
   extra paranoid. */
#define TDB_FIELDNAME_CHARS "_-%"\
                            "abcdefghijklmnopqrstuvwxyz"\
                            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"\
                            "0123456789"

#endif /* TDB_LIMITS */
