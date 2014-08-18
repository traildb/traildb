
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <string.h>
#include <time.h>
#include <sys/types.h>
#include <unistd.h>

#include <ddb_profile.h>
#include "util.h"

#include "trail-mix.h"
#include "ops.h"

#include <breadcrumbs_decoder.h>

/* COUNTER_BUF_SIZE should be less than PIPE_BUF, preferably less than
   512 bytes, to guarantee atomic appends and write to fifo.
*/
#define COUNTER_BUF_SIZE 511

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

#define TRAIL_BUF_INCREMENT 1000000

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
        int i, check_seen = 0;

        ctx.num_ops = argc - optidx;
        if (!(ctx.ops = malloc(ctx.num_ops * sizeof(struct trail_op))))
            DIE("Malloc failed in init_ops\n");

        for (i = 0; i < ctx.num_ops; i++, optidx++){
            char *arg = argv[optidx];
            char *name = strsep(&arg, "=");
            uint64_t flags = 0;

            ctx.ops[i].op = find_available_op(name);
            /* attr-set must check that ctx.opt_match_events == 1 and (binary output or flatten) */
            ctx.ops[i].arg = ctx.ops[i].op->init(&ctx,
                                                 arg,
                                                 i,
                                                 ctx.num_ops,
                                                 &flags);

            /* RULE: Attribute modifications must precede checks */
            if (flags & TRAIL_OP_CHECK_ATTR)
                check_seen = 1;
            if ((flags & TRAIL_OP_MOD_ATTR) && check_seen)
                DIE("Attribute checks must precede ops that modify "
                    "attributes (offending op: %s)\n", name);

            /* RULE: Attribute checks that support either pre- or post-ops
               must support both of them */
            if ((flags & TRAIL_OP_CHECK_ATTR) &&
                (flags & (TRAIL_OP_PRE_TRAIL | TRAIL_OP_POST_TRAIL)) &&
                (__builtin_popcount(flags & (TRAIL_OP_PRE_TRAIL |
                                             TRAIL_OP_POST_TRAIL)) == 2))
                DIE("Internal error (1): Invalid attribute check %s\n", name);

            /* RULE: Attribute mods must set exactly one scope */
            if ((flags & TRAIL_OP_MOD_ATTR) &&\
                __builtin_popcount(flags & (TRAIL_OP_DB |
                                            TRAIL_OP_PRE_TRAIL |
                                            TRAIL_OP_EVENT |
                                            TRAIL_OP_POST_TRAIL |
                                            TRAIL_OP_FINALIZE)) != 1)
                DIE("Internal error (2): Invalid attribute modifier %s\n", name);

            ctx.ops[i].flags = flags;
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
        {"input-file", required_argument, 0, 'i'},
        {"counters-file", required_argument, 0, 'C'},
        {"output-binary", no_argument, 0, 'b'},
        {"output-count", no_argument, 0, 'c'},
        {"random-seed", required_argument, 0, 'S'},
        /* long options */
        {"cardinalities", no_argument, 0, -2},
        {"no-index", no_argument, 0, -3},
        {"counters-prefix", required_argument, 0, -4},
        {0, 0, 0, 0}
    };

    do{
        c = getopt_long(argc,
                        argv,
                        "C:i:o:cbetvh::S:",
                        long_options,
                        &option_index);
        switch (c){

            case -1:
                break;

            case -2: /* --cardinalities */
                ctx.opt_cardinalities = 1;
                break;

            case -3: /* --no-index */
                ctx.opt_no_index = 1;
                break;

            case -4: /* --no-index */
                ctx.counters_prefix = optarg;
                break;

            case 'o': /* --output-file */
                ctx.output_file = optarg;
                break;

            case 'i': /* --input-file */
                ctx.input_file = optarg;
                break;

            case 'C': /* --counters-file */
                if (!(ctx.counters_file = fopen(optarg, "a")))
                    DIE("Could not open counters file at %s\n", optarg);
                break;

            case 'c': /* --output-count */
                ctx.opt_output_count = 1;
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

            case 'S': /* --random-seed */
                ctx.random_seed = parse_uint64(optarg, "random-seed");
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

    if (!ctx.random_seed)
        ctx.random_seed = (uint32_t)time(NULL) + (uint32_t)getpid();

    if (optind < argc){
        const char *input = argv[optind++];

        if (strcmp(input, "-")){
            uint64_t dummy;
            /* open default db, skip stdin */
            op_init_open(&ctx, input, 0, 0, &dummy);
        }else
            ctx.read_stdin = 1;

        /* before parsing stdin, which can be an expensive operation,
           let's check that all ops are ok, and possibly open the db */
        init_ops(optind, argc, argv);

#ifdef ENABLE_COOKIE_INDEX
        if (ctx.db)
            input_load_cookie_index(&ctx);
#endif

        if (ctx.read_stdin)
            input_parse_stdin(&ctx);
        else
            input_choose_all_rows(&ctx);

        if (ctx.opt_output_trails && !ctx.db)
            DIE("Cannot --output-trails without a DB\n");

        /* MSG(num_matches) */
    }else
        print_usage_and_exit();
}

static uint32_t *filter_ops(uint64_t include,
                            uint64_t exclude,
                            uint32_t *num_ops)
{
    int i;
    uint32_t *indices;

    if (!(indices = malloc(ctx.num_ops * 4)))
        DIE("Malloc failed in filter_ops\n");

    for (*num_ops = 0, i = 0; i < ctx.num_ops; i++){
        if ((ctx.ops[i].flags & include) && !(ctx.ops[i].flags & exclude))
            indices[(*num_ops)++] = i;
    }

    if (ctx.opt_verbose){
        const char *label;
        switch (include){
            case TRAIL_OP_DB:
                label = "db";
                break;
            case TRAIL_OP_PRE_TRAIL:
                label = "pre-trail";
                break;
            case TRAIL_OP_POST_TRAIL:
                label = "post-trail";
                break;
            case TRAIL_OP_EVENT:
                label = "event";
                break;
            case TRAIL_OP_FINALIZE:
                label = "finalize";
                break;
            default:
                label = "unknown";
        }
        fprintf(stderr,
                "Initialized %u %s operations:",
                *num_ops,
                label);
        for (i = 0; i < *num_ops; i++)
            fprintf(stderr, " %s", ctx.ops[indices[i]].op->name);
        fprintf(stderr, "\n");
    }

    return indices;
}

/*
    --match-events mode:
    All operations must match any single event in the trail.
    No early stopping. All events are evaluated.
*/
static inline int exec_match_events(const uint32_t *trail,
                                    uint32_t len,
                                    uint64_t row_id,
                                    const uint32_t *eveops,
                                    uint32_t num_eveops)
{
    uint32_t event_start, i, j;
    int skip = 1;

    for (i = 0; i < len;){

        /* find next event */
        event_start = i++;
        while (trail[i])
            ++i;

        for (j = 0; j < num_eveops; j++){
            int idx = eveops[j];
            if (ctx.ops[idx].op->exec(&ctx,
                                      TRAIL_OP_EVENT,
                                      row_id,
                                      &trail[event_start],
                                      event_start - i,
                                      ctx.ops[idx].arg))
                break;
        }
        /* if all ops match an event, the whole trail matches */
        if (j == num_eveops)
            skip = 0;

        ++i;
    }
    return skip;
}

/*
    Default matching:
    All operations must match some event in the trail, not necessarily
    the same one. We can early-stop as soon as all ops have found a
    matching event.
*/
static inline int exec_match_trail(const uint32_t *trail,
                                   uint32_t len,
                                   uint64_t row_id,
                                   const uint32_t *eveops,
                                   uint32_t num_eveops)
{
    uint64_t match_ops = 0;
    uint32_t event_start, i, j, eveops_left = num_eveops;

    for (i = 0; i < len;){

        /* find next event */
        event_start = i++;
        while (trail[i])
            ++i;

        for (j = 0; j < num_eveops; j++){
            int idx = eveops[j];

            /* skip over ops that have already matched */
            if (match_ops & (1 << j))
                continue;

            if (!ctx.ops[idx].op->exec(&ctx,
                                       TRAIL_OP_EVENT,
                                       row_id,
                                       &trail[event_start],
                                       event_start - i,
                                       ctx.ops[idx].arg)){
                if (!--eveops_left)
                    /* all ops match some event, trail match */
                    return 0;

                /* exclude this op */
                match_ops |= (1 << j);
            }
        }
        ++i;
    }
    return 1;
}

static void exec_ops()
{
    uint64_t row_id = 0;
    uint32_t i;
    int tmp, cont;

    uint32_t *trail = NULL;
    uint32_t trail_size = 0;

    uint32_t num_postops, num_eveops, num_preops, num_dbops, num_finops;
    uint32_t *eveops = filter_ops(TRAIL_OP_EVENT, 0, &num_eveops);
    uint32_t *dbops = filter_ops(TRAIL_OP_DB, 0, &num_dbops);
    uint32_t *finops = filter_ops(TRAIL_OP_FINALIZE, 0, &num_finops);
    uint32_t *postops;
    uint32_t *preops;

    int attr_check_post = 0;

    /* this is a convenient limit that makes trail-level matching more
       efficient to implement, see exec_match_trail() above */
    if (num_eveops > 64)
        DIE("Maximum number of event-level operations is 64\n");

    /* disable attribute pre-trail checks if some ops modify attributes */
    for (i = 0; i < ctx.num_ops; i++)
        if (ctx.ops[i].flags & TRAIL_OP_MOD_ATTR)
            attr_check_post = 1;

    /* enable either pre- or post-check of attributes, not both */
    preops = filter_ops(TRAIL_OP_PRE_TRAIL,
                        attr_check_post ? TRAIL_OP_CHECK_ATTR: 0,
                        &num_preops);
    postops = filter_ops(TRAIL_OP_POST_TRAIL,
                         attr_check_post ? 0: TRAIL_OP_CHECK_ATTR,
                         &num_postops);

    /* DATABASE-LEVEL OPERATIONS */
    for (i = 0; i < num_dbops; i++){
        /* these ops should modify ctx directly, so we can ignore the
           return value */
        int idx = dbops[i];
        ctx.ops[idx].op->exec(&ctx, TRAIL_OP_DB, 0, NULL, 0, ctx.ops[idx].arg);
    }

    J1F(cont, ctx.matched_rows, row_id);
    while (cont){
        uint32_t len = 0;

        /* PRE-TRAIL OPERATIONS */
        /* Reject a trail before decoding it, e.g. based on its attribute */
        for (i = 0; i < num_preops; i++){
            int idx = preops[i];
            if (ctx.ops[idx].op->exec(&ctx,
                                      TRAIL_OP_PRE_TRAIL,
                                      row_id,
                                      NULL,
                                      0,
                                      ctx.ops[idx].arg))
                goto reject;
        }

        /* EVENT OPERATIONS */
        /* Reject a trail based on its events */
        if (ctx.db){

            if (num_eveops || num_postops){
                while ((len = bd_trail_decode(ctx.db,
                                              row_id,
                                              trail,
                                              trail_size,
                                              0)) == trail_size){
                    free(trail);
                    trail_size += TRAIL_BUF_INCREMENT;
                    if (!(trail = malloc(trail_size * 4)))
                        DIE("Could not allocate trail buffer of %u items\n",
                             trail_size);
                }
            }

            if (num_eveops){
                int skip;
                if (ctx.opt_match_events)
                    skip = exec_match_events(trail,
                                             len,
                                             row_id,
                                             eveops,
                                             num_eveops);
                else
                    skip = exec_match_trail(trail,
                                            len,
                                            row_id,
                                            eveops,
                                            num_eveops);
                if (skip)
                    goto reject;
            }
        }

        /* POST-TRAIL OPERATIONS */
        /* Reject a decoded trail as a whole */
        for (i = 0; i < num_postops; i++){
            int idx = postops[i];
            if (ctx.ops[idx].op->exec(&ctx,
                                      TRAIL_OP_POST_TRAIL,
                                      row_id,
                                      trail,
                                      len,
                                      ctx.ops[idx].arg))
                goto reject;
        }

        goto accept;
reject:
        /* Reject this row_id: Remove it from matched_rows */
        J1U(tmp, ctx.matched_rows, row_id);
accept:
        J1N(cont, ctx.matched_rows, row_id);
    }

    /* FINALIZE-LEVEL OPERATIONS */
    for (i = 0; i < num_finops; i++){
        /* these ops should modify ctx directly, so we can ignore the
           return value */
        int idx = finops[i];
        ctx.ops[idx].op->exec(&ctx,
                              TRAIL_OP_FINALIZE,
                              0,
                              NULL,
                              0,
                              ctx.ops[idx].arg);
    }

    free(trail);
    free(dbops);
    free(preops);
    free(eveops);
    free(postops);
}

void write_counter(const char *name, long long int val)
{
    static char buf[COUNTER_BUF_SIZE];

    if (ctx.counters_file){
        const char *prefix = ctx.counters_prefix ? ctx.counters_prefix: "";
        int n = snprintf(buf,
                         COUNTER_BUF_SIZE,
                         ctx.counters_prefix ? "%s.%s %lld\n": "%s%s %lld\n",
                         prefix,
                         name,
                         val);
        if (n >= COUNTER_BUF_SIZE)
            DIE("Counter buffer too small (value %s.%s)\n", prefix, name);
        else if (fwrite(buf, n, 1, ctx.counters_file) != 1)
            DIE("Writing a counter %s.%s failed\n", prefix, name);
        fflush(ctx.counters_file);
    }
}

int main(int argc, char **argv)
{
    long long unsigned int count;
    DDB_TIMER_DEF

    DDB_TIMER_START
    initialize(argc, argv);
    if (ctx.opt_verbose){
        DDB_TIMER_END("initialization");
    }

    J1C(count, ctx.matched_rows, 0, -1);
    write_counter("main.input_trails", count);
    MSG(&ctx, "Number of input trails: %llu\n", count);

    DDB_TIMER_START
    exec_ops();
    if (ctx.opt_verbose){
        DDB_TIMER_END("operations");
    }

    J1C(count, ctx.matched_rows, 0, -1);
    write_counter("main.output_trails", count);
    MSG(&ctx, "Number of output trails: %llu\n", count);

    DDB_TIMER_START
    if (ctx.opt_output_trails)
        output_trails(&ctx);
    else
        output_matches(&ctx);
    if (ctx.opt_verbose){
        DDB_TIMER_END("output");
    }

    MSG(&ctx, "Done!\n");
    return 0;
}
