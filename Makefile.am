lib_LTLIBRARIES = libtraildb.la
ACLOCAL_AMFLAGS = -I m4
AM_CPPFLAGS = "-I$(top_srcdir)/xxhash -I$(top_srcdir)/dsfmt"
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
  src/dsfmt/dSFMT.c \
  src/xxhash/xxhash.c
include_HEADERS = \
  src/traildb.h

