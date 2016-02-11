#define _DEFAULT_SOURCE /* getline() */
#define _GNU_SOURCE

#include <fcntl.h>
#include <sys/mman.h>
#include <sys/stat.h>
#include <unistd.h>
#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include "tdb_package.h"


struct pkg_toc{
    char *fname;
    uint64_t offset;
    uint64_t size;
};

static uint64_t toc_count_lines(FILE *f)
{
    char *buf = NULL;
    size_t n = 0;
    uint64_t num_lines = 0;

    if (getline(&buf, &n, f) == -1)
        goto done;

    if (strcmp(buf, TDB_TAR_MAGIC))
        goto done;

    while (1){
        if (getline(&buf, &n, f) == -1){
            num_lines = 0;
            goto done;
        }
        if (buf[0] == '\n')
            break;
        ++num_lines;
    }
done:
    free(buf);
    return num_lines;
}

static tdb_error toc_parse(FILE *f, struct pkg_toc *toc, uint64_t num_lines)
{
    char *buf = NULL;
    size_t n = 0;
    uint64_t i;

    /* ignore magic line */
    if (getline(&buf, &n, f) == -1)
        return TDB_ERR_IO_READ;
    free(buf);

    for (i = 0; i < num_lines; i++){
        if (fscanf(f,
                   "%ms %"PRIu64" %"PRIu64,
                   &toc[i].fname,
                   &toc[i].offset,
                   &toc[i].size) != 3)
            return TDB_ERR_INVALID_PACKAGE;
    }
    return 0;
}

tdb_error open_package(tdb *db, const char *root)
{
    int ret = 0;
    uint64_t num_lines;

    TDB_OPEN(db->package_handle, root, "r");
    if (fseek(db->package_handle, TOC_FILE_OFFSET, SEEK_SET) == -1){
        ret = TDB_ERR_INVALID_PACKAGE;
        goto done;
    }
    if (!(num_lines = toc_count_lines(db->package_handle))){
        ret = TDB_ERR_INVALID_PACKAGE;
        goto done;
    }
    if (!(db->package_toc = calloc(num_lines + 1, sizeof(struct pkg_toc)))){
        ret = TDB_ERR_NOMEM;
        goto done;
    }
    if (fseek(db->package_handle, TOC_FILE_OFFSET, SEEK_SET) == -1){
        ret = TDB_ERR_INVALID_PACKAGE;
        goto done;
    }
    if ((ret = toc_parse(db->package_handle, db->package_toc, num_lines)))
        goto done;
done:
    return ret;
}

void free_package(tdb *db)
{
    if (db->package_toc){
        struct pkg_toc *toc = (struct pkg_toc*)db->package_toc;
        uint64_t i;
        for (i = 0; toc[i].fname; i++)
            free(toc[i].fname);
        free(db->package_toc);
    }
    if (db->package_handle)
        fclose(db->package_handle);
}

static int toc_get(const tdb *db,
                   const char *fname,
                   uint64_t *offset,
                   uint64_t *size)
{
    const struct pkg_toc *toc = (const struct pkg_toc*)db->package_toc;
    uint64_t i;
    /*
    NOTE we find the matching file using a linear scan below.
    This shouldn't be a problem UNLESS there are a very large number of fields
    and lexicon.* files, in which case the list can get long. It should be easy
    to replace this with a faster search like JudySL, if needed.
    */
    for (i = 0; toc[i].fname; i++)
        if (!strcmp(toc[i].fname, fname)){
            *offset = toc[i].offset;
            *size = toc[i].size;
            return 0;
        }
    return -1;
}

FILE *package_fopen(const char *fname,
                    const char *root __attribute__((unused)),
                    const tdb *db)
{
    uint64_t offset, size;
    if (toc_get(db, fname, &offset, &size))
        return NULL;
    if (fseek(db->package_handle, (off_t)offset, SEEK_SET) == -1)
        return NULL;
    return db->package_handle;
}

int package_fclose(FILE *f __attribute__((unused)))
{
    /* we don't want to close db->package_handle */
    return 0;
}

int package_mmap(const char *fname,
                 const char *root __attribute__((unused)),
                 struct tdb_file *dst,
                 const tdb *db)
{
    /*
    we need to page-align offsets for mmap() and adjust data pointers
    accordingly. dst->mmap_size and dst->ptr correspond to the page-aligned
    values, dst->size and dst->data to the values containing the actual data.
    */

    int fd = fileno(db->package_handle);
    uint64_t offset, shift;
    if (toc_get(db, fname, &offset, &dst->size))
        return -1;

    shift = offset & ((uint64_t)(getpagesize() - 1));
    dst->mmap_size = dst->size + shift;
    offset -= shift;

    dst->ptr = mmap(NULL,
                    dst->mmap_size,
                    PROT_READ,
                    MAP_SHARED,
                    fd,
                    (off_t)offset);

    if (dst->ptr == MAP_FAILED)
        return -1;

    dst->data = &dst->ptr[shift];
    return 0;
}
