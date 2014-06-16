
#ifndef __DIE_H__
#define __DIE_H__

#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <stdarg.h>

#include <Judy.h>

#define DIE_ON_ERROR(msg)\
    do { perror(msg); exit(EXIT_FAILURE); } while (0)

#define DIE(msg, ...)\
    do { fprintf(stderr, "FAIL: ");\
         fprintf(stderr, msg, ##__VA_ARGS__);\
         exit(EXIT_FAILURE); } while (0)

#define SAFE_WRITE(ptr, size, path, f)\
    if (fwrite(ptr, size, 1, f) != 1){\
        DIE("Writing to %s failed\n", path);\
    }

#define SAFE_SEEK(f, offset, path)\
    if (fseek(f, offset, SEEK_SET) == -1){\
        DIE("Seeking to %llu in %s failed\n", (unsigned long long)offset, path);\
    }

struct sortpair{
    Word_t key;
    Word_t value;
};

struct sortpair *sort_judyl(const Pvoid_t judy, Word_t *num_items);

#define MAX_PATH_SIZE 1024

void make_path(char path[MAX_PATH_SIZE], char *fmt, ...);

#endif /* __DIE_H__ */
