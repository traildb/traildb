
#ifndef __TDB_PACKAGE_H__
#define __TDB_PACKAGE_H__

#include <stdio.h>

#include "tdb_internal.h"
#include "tdb_error.h"

#define TDB_TAR_MAGIC "TAR TOC FOR TDB VER 1\n"
#define TOC_FILE_OFFSET 2560 /* = (len(HEADER_FILES) * 2 + 1) * 512 */

tdb_error cons_package(const tdb_cons *cons);

tdb_error open_package(tdb *db, const char *root);

void free_package(tdb *db);

FILE *package_fopen(const char *fname, const char *root, const tdb *db);

int package_fclose(FILE *f);

int package_mmap(const char *fname,
                 const char *root,
                 struct tdb_file *dst,
                 const tdb *db);

#endif /* __TDB_PACKAGE_H__ */
