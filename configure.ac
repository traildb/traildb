AC_INIT([traildb], [1.0], [we-will-need-an-actual-email-here-later@dev.null])
: ${CFLAGS=""}
AC_CONFIG_MACRO_DIR([m4])
AM_INIT_AUTOMAKE([subdir-objects])
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

AC_CHECK_LIB(Judy, JudyHSIns, [], [
  AC_MSG_ERROR([unable to libJudy])
], [])
AC_CHECK_HEADERS([Judy.h], [], [
  AC_MSG_ERROR([unable to find Judy.h, from Judy library])
])

AC_CHECK_TYPE(__uint128_t, [], [
  AC_MSG_ERROR([__uint128_t not defined])
])

AC_CHECK_SIZEOF([off_t])

AC_CHECK_SIZEOF([size_t])

AC_OUTPUT

