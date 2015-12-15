AC_INIT([traildb], [1.0], [we-will-need-an-actual-email-here-later@dev.null])
AM_INIT_AUTOMAKE([-Wall subdir-objects])
AM_PROG_AR
AM_PROG_LIBTOOL
AC_PROG_LIBTOOL
LT_INIT
AC_PROG_CC
AC_CONFIG_FILES([
  Makefile
])
AC_DEFINE(DSFMT_MEXP, 521)
AC_DEFINE(HAVE_SSE2, 1)

AC_SEARCH_LIBS([ddb_queue_new], [discodb], [], [
  AC_MSG_ERROR([unable to find discodb, with ddb_queue_new() function])
])
AC_CHECK_HEADERS([ddb_queue.h], [], [
  AC_MSG_ERROR([unable to find ddb_queue.h, from discodb])
])
AC_CHECK_LIB(Judy, JudyHSIns, [], [
  AC_MSG_ERROR([unable to libJudy])
], [])
AC_CHECK_HEADERS([Judy.h], [], [
  AC_MSG_ERROR([unable to find Judy.h, from Judy library])
])

AC_OUTPUT

