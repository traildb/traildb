
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>
#include <sys/stat.h>
#include <sys/types.h>

#include "tdb_internal.h"
#include "merge.h"
#include "hex.h"
#include "util.h"

#define DEFAULT_PORT 7676

static void show(struct extractd_ctx *ctx)
{
    static char hex_cookie[33];
    tdb_cookie cookie;
    const uint32_t *events;
    uint32_t i, j, k, num_fields, num_events;
    int ret;

    while ((ret = extractd_next_trail(ctx->extd,
                                      &cookie,
                                      &events,
                                      &num_events,
                                      &num_fields,
                                      MAPPER_TIMEOUT))){
        if (ret == -1)
            DIE("Timeout");

        hex_encode(cookie, hex_cookie);
        printf("%s", hex_cookie);
        for (i = 0, k = 0; i < num_events; i++){
            if (i > 0)
                printf(" |");
            printf(" %u", events[k++]);
            for (j = 0; j < num_fields; j++)
                printf(" %s", extractd_get_token(ctx->extd, j, events[k++]));
        }
        printf("\n");
    }
}

static void print_usage_and_exit()
{
    printf("USAGE\n");
    /* list available ops */
    exit(1);
}

const char *safe_filename(const char *value)
{
    static char safe[TDB_MAX_PATH_SIZE];
    uint32_t i;

    for (i = 0; i < TDB_MAX_PATH_SIZE - 1 && value[i]; i++)
        if (isalnum(value[i]))
            safe[i] = value[i];
        else
            safe[i] = '_';
    safe[i] = 0;

    return safe;
}

static void output_fields(const struct extractd_ctx *ctx)
{
    static char path[TDB_MAX_PATH_SIZE];
    uint32_t i, j, k;
    FILE *out;

    for (i = 0, k = 0; i < extractd_get_num_fields(ctx->extd); i++){
        const char *field = extractd_get_field_name(ctx->extd, i);
        const char *safe = safe_filename(field);

        if (ctx->groupby_str && !strcmp(field, ctx->groupby_str))
            continue;

        if (ctx->dir)
            tdb_path(path, "%s/trail-field.%u.%s.csv", ctx->dir, k, safe);
        else
            tdb_path(path, "trail-field.%u.%s.csv", k, safe);

        if (!(out = fopen(path, "w")))
            DIE("Could not open output file at %s\n", path);

        for (j = 0; j < extractd_num_tokens(ctx->extd, i); j++){
            const char *token = extractd_get_token(ctx->extd, i, j);
            SAFE_FPRINTF(out, path, "%s\n", token);
        }

        ++k;
        fclose(out);
    }
}

int main(int argc, char **argv)
{
    static struct extractd_ctx ctx;
    int c, option_index = 0;

    static struct option long_options[] = {
        {"port", required_argument, 0, 'p'},
        {"show", no_argument, 0, 's'},
        {"groupby", required_argument, 0, 'g'},
        {"output-dir", required_argument, 0, 'o'},
        {0, 0, 0, 0}
    };

    do{
        c = getopt_long(argc,
                        argv,
                        "p:g:o:s",
                        long_options,
                        &option_index);
        switch (c){
            case -1:
                break;
            case 'p': /* --port */
                ctx.port = parse_uint64(optarg, "port");
                break;
            case 'g': /* --groupby */
                ctx.groupby_str = optarg;
                break;
            case 'o': /* --output-dir */
                ctx.dir = optarg;
                break;
            case 's': /* --show */
                ctx.show = 1;
                break;
            default:
                print_usage_and_exit();
        }
    }while (c != -1);

    if (optind < argc){
        uint64_t num_mappers = parse_uint64(argv[optind], "number of mappers");
        ctx.extd = extractd_init(num_mappers,
                                 ctx.port ? ctx.port: DEFAULT_PORT);
    }else{
        printf("%u %u\n", optind, argc);
        print_usage_and_exit();
    }

    if (ctx.show)
        show(&ctx);
    else{
        if (ctx.dir)
            mkdir(ctx.dir, 0755);
        grouper_process(&ctx);
        grouper_output(&ctx);
        output_fields(&ctx);
    }

    return 0;
}
