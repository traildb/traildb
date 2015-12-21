#ifndef __TDB_PROFILE_H__
#define __TDB_PROFILE_H__

#include <time.h>
#include <stdio.h>

#ifdef TDB_PROFILE
#define TDB_TIMER_DEF clock_t __start;
#define TDB_TIMER_START __start = clock();
#define TDB_TIMER_END(msg) fprintf(stderr, "PROF: %s took %2.4fms\n", msg,\
        ((double) (clock() - __start)) / (CLOCKS_PER_SEC / 1000.0));
#else
#define TDB_TIMER_DEF
#define TDB_TIMER_START
#define TDB_TIMER_END(x)
#endif

#endif /* __TDB_PROFILE_H__ */
