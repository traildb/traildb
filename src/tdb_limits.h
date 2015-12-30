
#ifndef __TDB_LIMITS_H__
#define __TDB_LIMITS_H__

#define TDB_MAX_PATH_SIZE   2048
#define TDB_MAX_FIELDNAME_LENGTH 512
#define TDB_MAX_ERROR_SIZE  (TDB_MAX_PATH_SIZE + 512)

/* MAX_NUM_TRAILS * 16 must fit in off_t (long) type */
#define TDB_MAX_NUM_TRAILS  ((1LLU << 59) - 1)

/* num_events is uint32_t in tdb_decode_trail() and elsewhere */
#define TDB_MAX_TRAIL_LENGTH ((1LLU << 32) - 1)

#define TDB_MAX_NUM_FIELDS ((1LLU << 14) - 2)

#define TDB_MAX_NUM_VALUES ((1LLU << 48) - 2)

/* TODO remove TDB_OVERFLOW_VALUE */
#define TDB_OVERFLOW_VALUE ((1LLU << 24) - 1)

#define TDB_FIELD32_MAX 127
#define TDB_VAL32_MAX   ((1LLU << 24) - 1)

/* This is an arbitary value as long as it fits into stack comfortably */
#define TDB_MAX_VALUE_SIZE  (1LLU << 10)
#define TDB_MAX_LEXICON_SIZE UINT32_MAX
/*
#define TDB_MAX_TIMEDELTA  ((1LLU << 24) - 2) // ~194 days
#define TDB_FAR_TIMEDELTA  ((1LLU << 24) - 1)
#define TDB_FAR_TIMESTAMP    UINT32_MAX
*/

/* support a character set that allows easy urlencoding */
#define TDB_FIELDNAME_CHARS "_-%"\
                            "abcdefghijklmnopqrstuvwxyz"\
                            "ABCDEFGHIJKLMNOPQRSTUVWXYZ"\
                            "0123456789"

#endif /* TDB_LIMITS */
