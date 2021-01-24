lib_LTLIBRARIES = libtraildb.la
ACLOCAL_AMFLAGS = -I m4
libtraildb_la_CFLAGS = -std=c99 \
                       -DJUDYERROR=judyerror_macro_missing_fix_this \
                       -O3 \
                       -fvisibility=hidden \
                       -g \
                       -Wall \
                       -Wextra \
                       -Wconversion \
                       -Wcast-qual \
                       -Wformat-security \
                       -Wmissing-declarations \
                       -Wmissing-prototypes \
                       -Wnested-externs \
                       -Wpointer-arith \
                       -Wshadow \
                       -Wstrict-prototypes

AM_CPPFLAGS = -Isrc/xxhash -Isrc/dsfmt -Isrc/pqueue
AM_CFLAGS=-O3 -g -fvisibility=hidden

libtraildb_la_LIBADD = src/xxhash/xxhash.lo src/dsfmt/dSFMT.lo src/pqueue/pqueue.lo
libtraildb_la_SOURCES = \
  src/tdb.c \
  src/tdb_cons.c \
  src/tdb_uuid.c \
  src/tdb_decode.c \
  src/tdb_encode.c \
  src/tdb_encode_model.c \
  src/tdb_queue.c \
  src/tdb_huffman.c \
  src/tdb_cons_package.c \
  src/tdb_package.c \
  src/tdb_multi_cursor.c \
  src/arena.c \
  src/judy_str_map.c \
  src/judy_128_map.c

EXTRA_libtraildb_la_SOURCES = src/xxhash/xxhash.c src/dsfmt/dSFMT.c src/pqueue/pqueue.c

include_HEADERS = \
  src/traildb.h \
  src/tdb_error.h \
  src/tdb_types.h \
  src/tdb_limits.h

bin_PROGRAMS = util/traildb_bench tdbcli/tdb
util_traildb_bench_SOURCES = util/traildb_bench.c
util_traildb_bench_CFLAGS  = ${libtraildb_la_CFLAGS} -Isrc/
util_traildb_bench_LDADD   = libtraildb.la

tdbcli_tdb_CFLAGS = -Isrc/ \
                    -O3 \
                    -g \
                    -Wall
tdbcli_tdb_LDFLAGS = -pthread
tdbcli_tdb_SOURCES = tdbcli/main.c tdbcli/tdb_index.c tdbcli/op_dump.c \
		     tdbcli/op_make.c tdbcli/op_merge.c tdbcli/jsmn/jsmn.c \
		     tdbcli/thread_util.c tdbcli/filter.c src/xxhash/xxhash.c \
		     tdbcli/op_index.c
tdbcli_tdb_LDADD = libtraildb.la
