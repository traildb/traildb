lib_LTLIBRARIES = libtraildb.la
ACLOCAL_AMFLAGS = -I m4
libtraildb_la_CFLAGS = -std=c99 \
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

AM_CPPFLAGS = "-I$(top_srcdir)/xxhash -I$(top_srcdir)/dsfmt"
AM_CFLAGS=-O3 -g

MOSTLYCLEANFILES = $(top_srcdir)/src/xxhash/*.lo \
                   $(top_srcdir)/src/xxhash/*.o \
                   $(top_srcdir)/src/xxhash/.libs/* \
                   $(top_srcdir)/src/dsfmt/*.lo \
                   $(top_srcdir)/src/dsfmt/*.o \
                   $(top_srcdir)/src/dsfmt/.libs/*

libtraildb_la_LIBADD = $(top_srcdir)/src/xxhash/xxhash.lo $(top_srcdir)/src/dsfmt/dSFMT.lo
libtraildb_la_SOURCES = \
  src/tdb.c \
  src/tdb_cons.c \
  src/tdb_uuid.c \
  src/tdb_decode.c \
  src/tdb_encode.c \
  src/tdb_encode_model.c \
  src/tdb_queue.c \
  src/tdb_huffman.c \
  src/util.c \
  src/arena.c \
  src/judy_str_map.c \
  src/judy_128_map.c

EXTRA_libtraildb_la_SOURCES = src/xxhash/xxhash.c src/dsfmt/dSFMT.c

include_HEADERS = \
  src/traildb.h

