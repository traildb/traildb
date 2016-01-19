#ifdef HAVE_ARCHIVE_H

#define _DEFAULT_SOURCE /* mkstemp() */
#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>

#include <stdint.h>
#include <string.h>
#include <stdio.h>
#include <stdlib.h>

#include <archive.h>
#include <archive_entry.h>

#include "tdb_package.h"

/*
NOTE! DO NOT change HEADER_FILES since we guarantee that these files
(and TOC_FILE) can be found at fixed offsets
*/
static const char *HEADER_FILES[] = {"version",
                                     /* TODO add "id" (id should have a magic prefix) */
                                     "info"};

static const char *DATA_FILES[] = {"fields",
                                   "trails.codebook",
                                   "trails.toc",
                                   "trails.data",
                                   "uuids"};

static const char TOC_FILE[] = "tar.toc";

#define TOC_FILE_OFFSET 2560 /* = (len(HEADER_FILES) * 2 + 1) * 512 */

static inline void debug_print(char __attribute__((unused)) *fmt, ...)
{
#ifdef TDB_PACKAGE_DEBUG
    va_list aptr;
    va_start(aptr, fmt);
    vfprintf(stderr, fmt, aptr);
    va_end(aptr);
#endif
}

static inline tdb_error write_toc_entry(FILE *toc_file,
                                        const char *fname,
                                        uint64_t offset,
                                        uint64_t size)
{
    int ret = 0;
    TDB_FPRINTF(toc_file, "%s %"PRIu64" %"PRIu64"\n", fname, offset, size);
done:
    return ret;
}

static tdb_error write_header(struct archive *tar,
                              struct archive_entry *entry,
                              const char *fname,
                              uint64_t size)
{
    if (size > INT64_MAX)
        return TDB_ERR_IO_PACKAGE;

    archive_entry_clear(entry);
    archive_entry_set_pathname(entry, fname);
    archive_entry_set_size(entry, (int64_t)size);
    archive_entry_set_filetype(entry, AE_IFREG);
    archive_entry_set_perm(entry, 0644);
    if (archive_write_header(tar, entry) != ARCHIVE_OK)
        return TDB_ERR_IO_PACKAGE;
    return 0;
}

static tdb_error write_file_entry(struct archive *tar,
                                  struct archive_entry *entry,
                                  const char *src,
                                  const char *root,
                                  FILE *toc_file)
{
    const uint64_t BUFFER_SIZE = 65536;
    char buffer[BUFFER_SIZE];
    char path[TDB_MAX_PATH_SIZE];
    struct stat stats;
    int fd = 0;
    int ret = 0;
    uint64_t num_left;

    TDB_PATH(path, "%s/%s", root, src);

    if ((fd = open(path, O_RDONLY)) == -1){
        debug_print("opening source file %s failed\n", path);
        ret = TDB_ERR_IO_PACKAGE;
        goto done;
    }
    if (fstat(fd, &stats)){
        debug_print("fstat on source file %s failed\n", path);
        ret = TDB_ERR_IO_PACKAGE;
        goto done;
    }

    if ((ret = write_header(tar, entry, src, (uint64_t)stats.st_size))){
        debug_print("write_header for %s failed\n", src);
        goto done;
    }

    if ((ret = write_toc_entry(toc_file,
                               src,
                               (uint64_t)archive_filter_bytes(tar, -1),
                               (uint64_t)stats.st_size))){
        debug_print("write_toc_entry for source file %s failed\n", src);
        goto done;
    }

    for (num_left = (uint64_t)stats.st_size; num_left > 0;){
        ssize_t r = read(fd, buffer, BUFFER_SIZE);
        if (r < 1){
            debug_print("reading source file %s failed\n", path);
            ret = TDB_ERR_IO_PACKAGE;
            goto done;
        }
        if (archive_write_data(tar, buffer, (size_t)r) != r){
            debug_print("writing file %s failed\n", src);
            ret = TDB_ERR_IO_PACKAGE;
            goto done;
        }
        num_left -= (uint64_t)r;
    }

    /*
    once the file has been successfully appended to the archive,
    we delete the source to save disk space
    */
    if (unlink(path)){
        ret = TDB_ERR_IO_PACKAGE;
        debug_print("unlinking %s failed\n", path);
        goto done;
    }
done:
    if (fd)
        close(fd);
    return ret;
}

static tdb_error write_entries(struct archive *tar,
                               struct archive_entry *entry,
                               const char **files,
                               uint64_t num_files,
                               const tdb_cons *cons,
                               FILE *toc_file)
{
    uint64_t i;
    int ret = 0;

    for (i = 0; i < num_files; i++)
        if ((ret = write_file_entry(tar,
                                    entry,
                                    files[i],
                                    cons->root,
                                    toc_file)))
            goto done;
done:
    return ret;
}

static tdb_error init_tar_toc(struct archive *tar,
                              struct archive_entry *entry,
                              const tdb_cons *cons,
                              FILE *toc_file,
                              uint64_t *toc_offset,
                              uint64_t *toc_max_size)
{
    /*
    We need to preallocate enough space for tar.toc file. Since we don't
    know the exact offsets yet, we allocate the absolute maximum the toc
    can take.
    */
    static const uint64_t VALUE_SIZE = 22; /* = len(' %d\n' % 2**64) */
    static const uint64_t LEXICON_PREFIX_LEN = 8; /* = len("lexicon.") */
    uint64_t i, size = strlen(TOC_FILE) + VALUE_SIZE + strlen(TDB_TAR_MAGIC);
    char *buffer = NULL;
    int ret = 0;

    for (i = 0; i < sizeof(HEADER_FILES) / sizeof(HEADER_FILES[0]); i++)
        size += strlen(HEADER_FILES[i]) + VALUE_SIZE;

    for (i = 0; i < sizeof(DATA_FILES) / sizeof(DATA_FILES[0]); i++)
        size += strlen(DATA_FILES[i]) + VALUE_SIZE;

    for (i = 0; i < cons->num_ofields; i++)
        size += strlen(cons->ofield_names[i]) + LEXICON_PREFIX_LEN + VALUE_SIZE;

    *toc_max_size = ++size; /* empty line in the end */

    if ((ret = write_header(tar, entry, TOC_FILE, size))){
        debug_print("write_header for TOC_FILE failed\n");
        goto done;
    }

    /*
    archives are unreadable if the TOC is not found exactly at the right
    offset. Assert that this requirement is not violated.
    */
    *toc_offset = (uint64_t)archive_filter_bytes(tar, -1);
    if (*toc_offset != TOC_FILE_OFFSET){
        debug_print("assert failed: invalid toc offset: %"PRIu64"\n",
                    *toc_offset);
        ret = TDB_ERR_IO_PACKAGE;
        goto done;
    }

    if ((ret = write_toc_entry(toc_file,
                               TOC_FILE,
                               *toc_offset,
                               size))){
        debug_print("write_toc_entry for TOC_FILE failed\n");
        goto done;
    }

    /* we just need an array of null bytes */
    if (!(buffer = calloc(1, size))){
        ret = TDB_ERR_NOMEM;
        goto done;
    }
    if (archive_write_data(tar, buffer, size) != (ssize_t)size){
        debug_print("reserving %"PRIu64" bytes for TOC_FILE failed\n", size);
        ret = TDB_ERR_IO_PACKAGE;
        goto done;
    }
done:
    free(buffer);
    return ret;
}

static tdb_error write_lexicons(struct archive *tar,
                                struct archive_entry *entry,
                                const tdb_cons *cons,
                                FILE *toc_file)
{
    char path[TDB_MAX_PATH_SIZE];
    uint64_t i;
    int ret = 0;

    for (i = 0; i < cons->num_ofields; i++){
        TDB_PATH(path, "lexicon.%s", cons->ofield_names[i]);
        if ((ret = write_file_entry(tar, entry, path, cons->root, toc_file)))
            goto done;
    }
done:
    return ret;
}

static tdb_error write_tar_toc(int fd,
                               FILE *toc_file,
                               uint64_t toc_offset,
                               uint64_t toc_max_size)
{
    /*
    we rewind back to the position of the TOC_FILE and actually
    fill in the contents of the file
    */
    const uint64_t BUFFER_SIZE = 65536;
    char buffer[BUFFER_SIZE];
    uint64_t n, i, toc_size;
    int ret = 0;
    long offset;
    /* find an empty line denoting EOF in toc_file */
    TDB_FPRINTF(toc_file, "\n");
    if ((offset = ftell(toc_file)) == -1){
        debug_print("ftell(toc_file) failed\n");
        ret = TDB_ERR_IO_PACKAGE;
        goto done;
    }
    toc_size = (uint64_t)offset;

    /* assert that our max_size estimate is not broken */
    if (toc_size > toc_max_size){
        debug_print("assert failed: toc_size %"PRIu64" > %"PRIu64"\n",
                    toc_size,
                    toc_max_size);
        ret = TDB_ERR_IO_PACKAGE;
        goto done;
    }
    rewind(toc_file);

    if (lseek(fd, (off_t)toc_offset, SEEK_SET) != (off_t)toc_offset){
        debug_print("lseek(fd) failed\n");
        ret = TDB_ERR_IO_PACKAGE;
        goto done;
    }

    for (n = 0; n < toc_size;){
        size_t r = fread(buffer, 1, BUFFER_SIZE, toc_file);
        if (r < 1){
            debug_print("fread() toc_file failed\n");
            ret = TDB_ERR_IO_READ;
            goto done;
        }
        for (i = 0; i < r;){
            ssize_t w = write(fd, &buffer[i], r - i);
            if (w < 1){
                debug_print("write(fd) failed\n");
                ret = TDB_ERR_IO_PACKAGE;
                goto done;
            }
            i += (uint64_t)w;
        }
        n += r;
    }

done:
    return ret;
}

tdb_error cons_package(const tdb_cons *cons)
{
    char dst_path[TDB_MAX_PATH_SIZE];
    char path[TDB_MAX_PATH_SIZE];
    struct archive *tar = NULL;
    int fd = 0;
    int ret = 0;
    FILE *toc_file;
    struct archive_entry *entry = archive_entry_new();
    uint64_t toc_offset = 0;
    uint64_t toc_max_size = 0;

    if (!entry)
        return TDB_ERR_NOMEM;

    /* 1) open archive */

    if (!(tar = archive_write_new())){
        debug_print("archive_write_new() failed\n");
        ret = TDB_ERR_NOMEM;
        goto done;
    }
    if (archive_write_set_format_gnutar(tar) != ARCHIVE_OK){
        debug_print("archive_write_set_format_gnutar() failed\n");
        ret = TDB_ERR_IO_PACKAGE;
        goto done;
    }
    TDB_PATH(dst_path, "%s.tdb.XXXXXX", cons->root);
    if ((fd = mkstemp(dst_path)) == -1){
        debug_print("mkstemp(%s) failed\n", dst_path);
        ret = TDB_ERR_IO_PACKAGE;
        goto done;
    }
    if (archive_write_open_fd(tar, fd) != ARCHIVE_OK){
        debug_print("archive_write_open_fd(%s) failed\n", dst_path);
        ret = TDB_ERR_IO_PACKAGE;
        goto done;
    }

    /* open TOC_FILE */
    TDB_PATH(path, "%s/%s", cons->root, TOC_FILE);
    TDB_OPEN(toc_file, path, "w+");
    TDB_FPRINTF(toc_file, TDB_TAR_MAGIC);

    /* 2) write header files */
    if ((ret = write_entries(tar,
                             entry,
                             HEADER_FILES,
                             sizeof(HEADER_FILES) / sizeof(HEADER_FILES[0]),
                             cons,
                             toc_file)))
        goto done;

    /* 3) write tar toc */
    if ((ret = init_tar_toc(tar,
                            entry,
                            cons,
                            toc_file,
                            &toc_offset,
                            &toc_max_size)))
        goto done;

    /* 4) write lexicons */
    if ((ret = write_lexicons(tar, entry, cons, toc_file)))
        goto done;

    /* 5) write data */
    if ((ret = write_entries(tar,
                             entry,
                             DATA_FILES,
                             sizeof(DATA_FILES) / sizeof(DATA_FILES[0]),
                             cons,
                             toc_file)))
        goto done;

    /* 6) finalize archive */
    if (archive_write_free(tar) != ARCHIVE_OK){
        ret = TDB_ERR_IO_PACKAGE;
        goto done;
    }

    /* 7) write toc */
    if ((ret = write_tar_toc(fd, toc_file, toc_offset, toc_max_size)))
        goto done;

    /* delete TOC_FILE */
    fclose(toc_file);
    unlink(path);

    /* fsync() is required to ensure integrity of the package */
    if (fsync(fd) || close(fd)){
        debug_print("fsync || close failed\n");
        ret = TDB_ERR_IO_CLOSE;
        goto done;
    }
    tar = NULL;

    /* 9) rename archive */
    TDB_PATH(path, "%s.tdb", cons->root);
    if (rename(dst_path, path)){
        debug_print("rename to %s -> %s failed\n", dst_path, path);
        ret = TDB_ERR_IO_CLOSE;
        goto done;
    }

    /*
    rmdir() failing (most often because the directory is not empty), is not
    considered a fatal error: This can happen e.g. if the directory contains
    remnants of a previously failed tdb_cons, which is harmless
    */
    if (rmdir(cons->root))
        debug_print("rmdir(%s) failed\n", cons->root);

done:
    archive_entry_free(entry);

    if (fd)
        close(fd);
    if (tar){
        if (archive_write_free(tar) != ARCHIVE_OK)
            ret = TDB_ERR_IO_PACKAGE;
    }
    return ret;
}

#endif /* HAVE_ARCHIVE_H */
