
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>

#include "util.h"

#include "trail-mix.h"
#include "ops.h"

/* Internal structures:
    - Set of chosen IDs
    - List of open DBs
    - Mapping of ID -> Attribute
    - Options
*/

/* Data format:

   ID:       [ Head byte ] [ 16-byte ID ]
   Int attr: [ Head byte ] [ 16-byte ID ] [ 8-byte val ]
   Lex attr: [ Head byte ] [ 8-byte size ] [ zero-delimited values ]
   Set attr: [ Head byte ] [ 16-byte ID ] [ 4-byte num items ] [ 4-byte item ] ...

   Head byte > 127, reserve < 127 bytes for ASCII format

   Ascii format:

   32-char hex-encoded ID
   32-char hex-encoded ID [space] int-attr

*/

/* set attributes:
   - aggregate sets as usual JudyL (cookie row ID) -> Judy1 (field value)
   - when it is time to output, merge:
        - If global dict:
            - Insert field value (int) -> field string -> global dict -> global ID

   INPUT ATTRIBUTES:

   - If open chunk:
        - First create for the opened db JudyHS -> Row ID
   - If not open chunk:
        - Create JudyHS on the fly, store cookies in an arena
   - Create TOC for local Lex
   - Create global dict JudySL (token) -> Global ID
   - Load JudyL -> Judy1 (set)
          JudyL -> val (int)
   - If no chunk:
        - Evaluate attributes directly
*/

static struct trail_ctx ctx;

static const struct trail_available_op *find_available_op(const char *name)
{
    int i;
    for (i = 0; available_ops[i].name; i++){
        if (!strcmp(available_ops[i].name, name))
            return &available_ops[i];
    }
    DIE("Unknown operation: %s\n", name);
}

static void print_op_usage_and_exit(const char *name)
{
    find_available_op(name)->help();
    exit(1);
}

static void print_usage_and_exit()
{
    printf("USAGE\n");
    /* list available ops */
    exit(1);
}

static void init_ops(int optidx, int argc, char **argv)
{
    if (optidx < argc){
        int i;

        if (!(ctx.ops = malloc((argc - optidx) * sizeof(struct trail_op))))
            DIE("Malloc failed in init_ops\n");

        for (i = 0; optidx < argc; i++, optidx++){
            char *arg = argv[optidx];
            char *name = strsep(&arg, "=");
            ctx.ops[i].op = find_available_op(name);
            /* attr-set must check that ctx.opt_match_events == 1 and (binary output or flatten) */
            ctx.ops[i].arg = ctx.ops[i].op->init(&ctx, arg, i, argc - optidx);
            ++i;

            MSG(&ctx, "Operation '%s' initialized\n", name);
        }
    }
}

static void initialize(int argc, char **argv)
{
    int c, option_index = 0;

    static struct option long_options[] = {
        {"match-events", no_argument, 0, 'e'},
        {"help", optional_argument, 0, 'h'},
        {"verbose", no_argument, 0, 'v'},
        {"output-trails", no_argument, 0, 't'},
        {"output-file", required_argument, 0, 'o'},
        {"output-binary", no_argument, 0, 'b'},
        /* long options */
        {"cardinalities", no_argument, 0, -2},
        {0, 0, 0, 0}
    };

    do{
        c = getopt_long(argc, argv, "eh::v", long_options, &option_index);
        switch (c){

            case -1:
                break;

            case -2: /* --cardinalities */
                ctx.opt_cardinalities = 1;
                break;

            case 'o': /* --output-file */
                ctx.output_file = optarg;
                break;

            case 'b': /* --output-binary */
                ctx.opt_binary = 1;
                break;

            case 'e': /* --match-events */
                ctx.opt_match_events = 1;
                break;

            case 't': /* --output-trails */
                ctx.opt_output_trails = 1;
                break;

            case 'v': /* --verbose */
                ctx.opt_verbose = 1;
                break;

            case 'h': /* --help */
                if (optarg)
                    print_op_usage_and_exit(optarg);
                else
                    print_usage_and_exit();
                break;

            default:
                print_usage_and_exit();
        }
    }while (c != -1);

    if (optind < argc){
        const char *input = argv[optind++];
        int read_stdin = 0;

        if (strcmp(input, "-"))
            /* open default db, skip stdin */
            op_init_open(&ctx, input, 0, 0);
        else
            read_stdin = 1;

        /* before parsing stdin, which can be an expensive operation,
           let's check that all ops are ok, and possibly open the db */
        init_ops(optind, argc, argv);

        if (read_stdin)
            input_parse_stdin(&ctx);
        else
            input_choose_all_rows(&ctx);

        /* MSG(num_matches) */
    }else
        print_usage_and_exit();
}

static void exec_ops()
{

}

int main(int argc, char **argv)
{
    initialize(argc, argv);
    exec_ops();

    if (ctx.opt_output_trails)
        output_trails(&ctx);
    else
        output_matches(&ctx);

    MSG(&ctx, "Done!\n");
    return 0;
}
