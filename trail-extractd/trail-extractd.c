
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <stdint.h>
#include <ctype.h>

#include <hex_encode.h>
#include <util.h>

#include "trail-extractd.h"

#define DEFAULT_PORT 7676

static void show(struct extractd_ctx *ctx)
{
    static char hex_cookie[33];
    const char *cookie;
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

        hex_encode((const uint8_t*)cookie, hex_cookie);
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
    static char safe[MAX_PATH_SIZE];
    uint32_t i;

    for (i = 0; i < MAX_PATH_SIZE - 1 && value[i]; i++)
        if (isalnum(value[i]))
            safe[i] = value[i];
        else
            safe[i] = '_';
    safe[i] = 0;

    return safe;
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
        grouper_process(&ctx);
        grouper_output(&ctx);
    }

    return 0;
}
