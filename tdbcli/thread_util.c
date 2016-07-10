
#include <pthread.h>
#include <stdint.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>

#include "tdbcli.h"
#include "thread_util.h"

void execute_jobs(void *(*thread_fun)(void*),
                  struct thread_job *jobs,
                  uint32_t num_jobs,
                  uint32_t num_threads)
{
    uint32_t i, num_started = 0, num_done = 0;
    int err;

    num_threads = num_threads < num_jobs ? num_threads: num_jobs;

    for (i = 0; i < num_threads; i++){
        if ((err = pthread_create(&jobs[num_started].thread,
                                  NULL,
                                  thread_fun,
                                  jobs[num_started].arg)))
            DIE("Could not create a thread: %s\n", strerror(err));
        jobs[num_started].started = 1;
        jobs[num_started].done = 0;
        jobs[num_started].fresh = 0;
        ++num_started;
    }

    while (num_done < num_jobs){
        void *ret;
        int err, all_fresh = 1;

        for (i = 0; i < num_jobs; i++){
            if (jobs[i].started && !jobs[i].done && !jobs[i].fresh){
                all_fresh = 0;
                if ((err = pthread_join(jobs[i].thread, &ret)))
                    DIE("pthread_join failed: %s\n", strerror(err));
                else{
                    if (num_started < num_jobs){
                        if ((err = pthread_create(&jobs[num_started].thread,
                                                  NULL,
                                                  thread_fun,
                                                  jobs[num_started].arg)))
                            DIE("Could not create a thread: %s\n", strerror(err));
                        jobs[num_started].started = 1;
                        jobs[num_started].fresh = 1;
                        jobs[num_started].done = 0;
                        ++num_started;
                    }
                    jobs[i].done = 1;
                    ++num_done;
                }
            }
        }
        if (all_fresh)
            for (i = 0; i < num_jobs; i++)
                jobs[i].fresh = 0;
    }
}

void *execute_jobs_with_reduce(void *(*map_fun)(void*),
                               void *(*reduce_fun)(struct thread_job *jobs,
                                                   uint32_t num_jobs,
                                                   void *reduce_ctx),
                               struct thread_job *jobs,
                               void *reduce_ctx,
                               uint32_t num_jobs,
                               uint32_t num_threads)
{
    uint32_t i = 0;
    while (i < num_jobs){
        uint32_t batch_size;

        if (num_jobs - i > num_threads)
            batch_size = num_threads;
        else
            batch_size = num_jobs - i;

        execute_jobs(map_fun, &jobs[i], batch_size, num_threads);
        reduce_ctx = reduce_fun(&jobs[i], batch_size, reduce_ctx);
        i += batch_size;
    }
    return reduce_ctx;
}
