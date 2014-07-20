
#ifndef __DDB_PROFILE_H__
#define __DDB_PROFILE_H__

#include <time.h>
#include <stdio.h>

#ifdef DDB_PROFILE
#define DDB_TIMER_DEF struct timespec __start, __end;
#define DDB_TIMER_START clock_gettime(CLOCK_REALTIME, &__start);
#define DDB_TIMER_END(msg) clock_gettime(CLOCK_REALTIME, &__end);\
        fprintf(stderr, "PROF: %s took %lums\n", msg,\
            ((__end.tv_sec * 1000UL + __end.tv_nsec / 1000000UL) -\
             (__start.tv_sec * 1000UL + __start.tv_nsec / 1000000UL)));

#else
#define DDB_TIMER_DEF
#define DDB_TIMER_START
#define DDB_TIMER_END(x)
#endif

#endif /* __DDB_PROFILE_H__ */
