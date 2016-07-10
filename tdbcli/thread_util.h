
#ifndef TDBCLI_THREAD_UTIL
#define TDBCLI_THREAD_UTIL

#include <pthread.h>
#include <stdint.h>

struct thread_job{
    void *arg;
    pthread_t thread;
    int done;
    int started;
    int fresh;
};

typedef void *(*map_fun_t)(void*);
typedef void *(*reduce_fun_t)(struct thread_job *jobs,
                              uint32_t num_jobs,
                              void *reduce_ctx);

void execute_jobs(map_fun_t thread_fun,
                  struct thread_job *jobs,
                  uint32_t num_jobs,
                  uint32_t num_threads);

void *execute_jobs_with_reduce(map_fun_t map_fun,
                               reduce_fun_t reduce_fun,
                               struct thread_job *jobs,
                               void *reduce_ctx,
                               uint32_t num_jobs,
                               uint32_t num_threads);

#endif /* TDBCLI_THREAD_UTIL */
