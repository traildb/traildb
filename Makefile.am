lib_LTLIBRARIES = libtraildb.la
ACLOCAL_AMFLAGS = -I m4
libtraildb_la_CFLAGS = -std=c99 -Wconversion -Wall -Wcast-qual -Wcast-align -Werror -Wextra -O3 -g
AM_CPPFLAGS = "-I$(top_srcdir)/xxhash -I$(top_srcdir)/dsfmt"
AM_CFLAGS=-O3 -g

libtraildb_la_SOURCES = \
  src/arena.c \
  src/huffman.c \
  src/tdb.c \
  src/tdb_cons.c \
  src/tdb_uuid.c \
  src/tdb_decode.c \
  src/tdb_encode.c \
  src/tdb_encode_model.c \
  src/tdb_queue.c \
  src/util.c \
  src/judy_str_map.c \
  src/judy_128_map.c \
  src/xxhash/xxhash.c \
  src/dsfmt/dSFMT.c

EXTRA_libtraildb_la_SOURCES = src/xxhash/xxhash.c src/dsfmt/dSFMT.c

include_HEADERS = \
  src/traildb.h

