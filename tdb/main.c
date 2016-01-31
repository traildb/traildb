#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <errno.h>

#include <traildb.h>

#include "tdbcli.h"

/*
fields:
  - CSV: uuid:0,time:1
  - JSON: uuid:user,time:tstamp
*/

static struct tdbcli_options options;

static const char *OPS[] = {"make", "dump"};
#define OP_MAKE 0
#define OP_DUMP 1

long int safely_to_int(const char *str, const char *field)
{
    char *end = NULL;
    errno = 0;
    long int x = strtol(str, &end, 10);
    if (errno || *end)
        DIE("Invalid %s: %s", field, str);
    return x;
}

static void print_usage_and_exit()
{
    printf("Usage\n");
    exit(1);
}

#if 0
static void parse_fields(const char *fields_arg, int op)
{
    static const char *DEFAULT_FIELDNAMES[] = {"uuid", "time"};
    static const uint32_t DEFAULT_FIELD_MAP[] = {1, 2};

    /* four ways to define fields:

    0) no options
       * uuid and time are expected to be found at columns 1 and 2

    1) --fields uuid,time,field1,field2
       * Read / output first K fields
       * Extract the defined fields from / to JSON.

    2) --fields 2:uuid,3:time,20:field1,30:field2
       * Explicit column IDs for CSV.
       * Incompatible with JSON.
       * Incompatible with dump.

    3) --csv-header
       * Read/output fields from/to a csv header.
       * Incompatible with --fields.
       * Incompatible with JSON.
    */

    if (fields_arg){
    if (options.csv_has_header)
        /* mode 3) */
        DIE("Can't specify both --fields and --csv-header");


    /*
    if op == make,
        opt->fieldnames = tdb field names
        return input_field -> tdb_field mapping
    if op == dump
        opt->fieldnames = output field names
        return tdb_field -> output_field mapping
    */
    }else if (op == OP_DUMP){
    }else if (op == OP_MAKE){
        
    }
}
#endif

static void initialize(int argc, char **argv, int op)
{
    static const char DEFAULT_MAKE_INPUT[] = "-";
    static const char DEFAULT_MAKE_OUTPUT[] = "a";
    static const char DEFAULT_DUMP_INPUT[] = "a";
    static const char DEFAULT_DUMP_OUTPUT[] = "-";
    static const char DEFAULT_DELIMITER[] = " ";

    static struct option long_options[] = {
        {"csv", no_argument, 0, 'c'},
        {"json", no_argument, 0, 'j'},
        {"input", required_argument, 0, 'i'},
        {"output", required_argument, 0, 'o'},
        {"delimiter", required_argument, 0, 'd'},
        {"fields", required_argument, 0, 'f'},
        {"tdb-format", required_argument, 0, 't'},
        {"urlencode", no_argument, 0, 'u'},
        {"csv-header", no_argument, 0, -2},
        {0, 0, 0, 0}
    };

    int c, option_index = 1;

    /* defaults */

    if (op == OP_MAKE){
        options.input = DEFAULT_MAKE_INPUT;
        options.output = DEFAULT_MAKE_OUTPUT;
    }else if (op == OP_DUMP){
        options.input = DEFAULT_DUMP_INPUT;
        options.output = DEFAULT_DUMP_OUTPUT;
    }

    options.format = FORMAT_CSV;
    options.delimiter = DEFAULT_DELIMITER;

    do{
        c = getopt_long(argc,
                        argv,
                        "cji:o:f:d:t:",
                        long_options,
                        &option_index);

        switch (c){
            case -1:
                break;
            case 'c':
                options.format = FORMAT_CSV;
                break;
            case 'j':
                options.format = FORMAT_JSON;
                break;
            case 'i':
                options.input = optarg;
                break;
            case 'o':
                options.output = optarg;
                break;
            case 'd':
                if (strlen(optarg) != 1)
                    DIE("Delimiter must be one character, not '%s'", optarg);
                options.delimiter = optarg;
                break;
            case 'f':
                options.fields_arg = optarg;
                break;
            case 't':
                options.output_format_is_set = 1;
                if (!strcmp(optarg, "pkg"))
                    options.output_format = TDB_OPT_CONS_OUTPUT_FORMAT_PACKAGE;
                else if (!strcmp(optarg, "dir"))
                    options.output_format = TDB_OPT_CONS_OUTPUT_FORMAT_DIR;
                else{
                    DIE("Unknown output format: '%s'.\n"
                        "Expected 'pkg' or 'dir'.\n", optarg);
                }
                break;
            case -2:
                options.csv_has_header = 1;
                break;
            default:
                print_usage_and_exit();
        }
    }while (c != -1);
}

int main(int argc, char **argv)
{
    int i, op = -1;

    if (argc < 2)
        print_usage_and_exit();

    for (i = 0; i < sizeof(OPS) / sizeof(OPS[0]); i++)
        if (!strcmp(argv[1], OPS[i])){
            op = i;
            break;
        }

    if (op == -1)
        print_usage_and_exit();

    initialize(argc, argv, op);

    switch (op){
        case OP_MAKE:
            return op_make(&options);
        case OP_DUMP:
            return op_dump(&options);
        default:
            print_usage_and_exit();
    }

    return 0;
}


