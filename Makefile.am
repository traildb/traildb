lib_LTLIBRARIES = libtraildb.la
ACLOCAL_AMFLAGS = -I m4
libtraildb_la_CFLAGS = -std=c99 \
                       -DJUDYERROR=judyerror_macro_missing_fix_this \
                       -O3 \
                       -g \
                       -Wall \
                       -Werror \
                       -Wextra \
                       -Wconversion \
                       -Wcast-qual \
                       -Wcast-align \
                       -Wformat-security \
                       -Wmissing-declarations \
                       -Wmissing-prototypes \
                       -Wnested-externs \
                       -Wpointer-arith \
                       -Wshadow \
                       -Wstrict-prototypes

AM_CPPFLAGS = -Isrc/xxhash -Isrc/dsfmt
AM_CFLAGS=-O3 -g

libtraildb_la_LIBADD = src/xxhash/xxhash.lo src/dsfmt/dSFMT.lo
libtraildb_la_SOURCES = \
  src/tdb.c \
  src/tdb_cons.c \
  src/tdb_uuid.c \
  src/tdb_decode.c \
  src/tdb_encode.c \
  src/tdb_encode_model.c \
  src/tdb_queue.c \
  src/tdb_huffman.c \
  src/arena.c \
  src/judy_str_map.c \
  src/judy_128_map.c

EXTRA_libtraildb_la_SOURCES = src/xxhash/xxhash.c src/dsfmt/dSFMT.c

include_HEADERS = \
  src/traildb.h \
  src/tdb_types.h \
  src/tdb_limits.h

bin_PROGRAMS = traildb_bench
traildb_bench_SOURCES = src/traildb_bench.c
traildb_bench_CFLAGS = ${libtraildb_la_CFLAGS} -Isrc/
traildb_bench_LDADD = libtraildb.la
