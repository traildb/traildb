
#ifndef __TDB_IO_H__
#define __TDB_IO_H__

#include <stdio.h>
#include <stdlib.h>
#include <stdarg.h>

#include "tdb_error.h"

/* these are kept in stack, so they shouldn't be overly large */
#define TDB_MAX_PATH_SIZE   2048
#define TDB_MAX_FIELDNAME_LENGTH 512

#define TDB_OPEN(file, path, mode)\
    if (!(file = fopen(path, mode))){\
        ret = TDB_ERR_IO_OPEN;\
        goto done;\
    }

#define TDB_CLOSE_FINAL(file)\
    {\
        if (file && fclose(file))\
            return TDB_ERR_IO_CLOSE;\
        file = NULL;\
    }

#define TDB_CLOSE(file)\
    {\
        if (file && fclose(file)){\
            ret = TDB_ERR_IO_CLOSE;\
            goto done;\
        }\
        file = NULL;\
    }

#define TDB_FPRINTF(file, fmt, ...)\
    if (fprintf(file, fmt, ##__VA_ARGS__) < 1){\
        ret = TDB_ERR_IO_WRITE;\
        goto done;\
    }

#define TDB_READ(file, buf, size)\
    if (fread(buf, size, 1, file) != 1){\
        ret = TDB_ERR_IO_READ;\
        goto done;\
    }

#define TDB_WRITE(file, buf, size)\
    if (fwrite(buf, size, 1, file) != 1){\
        ret = TDB_ERR_IO_WRITE;\
        goto done;\
    }

#define TDB_TRUNCATE(file, size)\
    if (ftruncate(fileno(file), size)){\
        ret = TDB_ERR_IO_TRUNCATE;\
        goto done;\
    }

#define TDB_PATH(path, fmt, ...)\
    if (tdb_path(path, fmt, ##__VA_ARGS__)){\
        ret = TDB_ERR_PATH_TOO_LONG;\
        goto done;\
    }

#define TDB_SEEK(file, offset)\
    if (offset > LONG_MAX || fseek(file, (long)(offset), SEEK_SET) == -1){\
        ret = TDB_ERR_IO_WRITE;\
        goto done;\
    }

static int tdb_path(char path[TDB_MAX_PATH_SIZE],
                    char *fmt,
                    ...) __attribute__((unused));

static int tdb_path(char path[TDB_MAX_PATH_SIZE], char *fmt, ...)
{
    va_list aptr;

    va_start(aptr, fmt);
    if (vsnprintf(path, TDB_MAX_PATH_SIZE, fmt, aptr) >= TDB_MAX_PATH_SIZE)
        return TDB_ERR_PATH_TOO_LONG;
    va_end(aptr);
    return 0;
}

#endif /* __TDB_IO_H__ */
