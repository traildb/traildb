
#ifndef __DIE_H__
#define __DIE_H__

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

#endif /* __DIE_H__ */
