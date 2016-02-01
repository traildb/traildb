#ifndef __TDB_PROFILE_H__
#define __TDB_PROFILE_H__

#include <sys/time.h>
#include <time.h>
#include <stdio.h>

#ifdef TDB_PROFILE

#define TDB_TIMER_DEF struct timeval __start; struct timeval __end;
#define TDB_TIMER_START gettimeofday(&__start, NULL);
#define TDB_TIMER_END(msg)\
        gettimeofday(&__end, NULL);\
        fprintf(stderr, "PROF: %s took %ldms\n", msg,\
            ((__end.tv_sec * 1000000L + __end.tv_usec) -\
             (__start.tv_sec * 1000000L + __start.tv_usec)) / 1000);
#else
#ifdef TDB_PROFILE_CPU

#define TDB_TIMER_DEF clock_t __start;
#define TDB_TIMER_START __start = clock();
#define TDB_TIMER_END(msg) fprintf(stderr, "PROF: %s took %2.4fms (CPU time)\n", msg,\
        ((double) (clock() - __start)) / (CLOCKS_PER_SEC / 1000.0));
#else
#define TDB_TIMER_DEF
#define TDB_TIMER_START
#define TDB_TIMER_END(x)
#endif
#endif

#endif /* __TDB_PROFILE_H__ */
