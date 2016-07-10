#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <sys/stat.h>
#include <sys/time.h>

#include <tdb_io.h>

#include "tdb_index.h"
#include "tdbcli.h"

int op_index(struct tdbcli_options *opt)
{
    struct timeval start_time, end_time;
    char in_path[TDB_MAX_PATH_SIZE];
    char out_path[TDB_MAX_PATH_SIZE];
    int ret;

    if (opt->output){
        TDB_PATH(out_path, "%s", opt->output);
    }else{
        struct stat stats;
        TDB_PATH(in_path, "%s", opt->input);
        if (stat(in_path, &stats) == -1){
            TDB_PATH(in_path, "%s.tdb", opt->input);
            if (stat(in_path, &stats) == -1)
                DIE("Could not stat input: %s", opt->input);
        }
        if (S_ISDIR(stats.st_mode)){
            TDB_PATH(out_path, "%s/index", in_path);
        }else{
            TDB_PATH(out_path, "%s.index", in_path);
        }
    }
    if (!access(out_path, W_OK))
        DIE("Output file %s already exists", out_path);

    gettimeofday(&start_time, NULL);
    if (tdb_index_create(opt->input, out_path))
        DIE("Creating index failed");
    gettimeofday(&end_time, NULL);

    printf("Index created successfully at %s in %u seconds.\n",
           out_path,
           end_time.tv_sec - start_time.tv_sec);
    return 0;
done:
    DIE("Path too long");
}
