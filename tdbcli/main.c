#include <string.h>
#include <stdio.h>
#include <stdlib.h>
#include <getopt.h>
#include <errno.h>

#include <traildb.h>

#include "tdbcli.h"

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
    printf(
"\ntdb - a command line interface for manipulating TrailDBs\n"
"\n"
"USAGE:\n"
"tdb <command> [options]\n"
"\n"
"Command can be either 'make' to create a TrailDB or\n"
"'dump' to dump an existing TrailDB to an output file.\n"
"\n"
"OPTIONS:\n"
"-c --csv          read input as CSV or output CSV (default)\n"
"-d --delimiter    CSV delimiter (default: ' ')\n"
"-j --json         read input as JSON or output JSON\n"
"                   the format is one JSON-encoded event (object) per line\n"
"-i --input        read input from the given file:\n"
"                   for 'make' this is the source of input events\n"
"                    (default: stdin)\n"
"                   for 'dump' this is the TrailDB to be dumped\n"
"                    (default: a.tdb)\n"
"-o --output       write output to the given file:\n"
"                   for 'make' this is the TrailDB to be created\n"
"                    (default: a.tdb)\n"
"                  for 'dump' this is the output file for events\n"
"                    (default: stdout)\n"
"-f --fields       field specification - see below for details\n"
"-F --filter       filter specification -- see below for details\n"
"--csv-header      read fields from the CSV header - see below for details\n"
"--json-no-empty   don't output empty values to JSON output\n"
"--skip-bad-input  don't quit on malformed input lines, skip them\n"
"--tdb-format      TrailDB output format:\n"
"                   'pkg' for the default one-file format,\n"
"                   'dir' for a directory\n"
"-v --verbose      print diagnostic output to stderr\n"
"\n"
"FIELD SPECIFICATION:\n"
"The --fields option determines how fields from the input are mapped to\n"
"the output. Multiple ways of defining the mapping are supported. The\n"
"exact behavior varies between 'make' and 'dump':\n"
"\n"
"make:\n"
"1) if no --fields is specified, two fields are expected, 'uuid' and 'time'\n"
"     - CSV file should have the first column 'uuid', second 'time'\n"
"     - JSON objects must have keys 'uuid' and 'time'\n"
"2) --fields uuid,time,field2,field3,...\n"
"     - CSV file should have at least the specified columns\n"
"       (note that 'uuid' and 'time' can be set in any column)\n"
"     - JSON objects must have keys 'uuid' and 'time'. If other specified\n"
"       keys are found, they are extrated to TrailDB.\n"
"3) --fields 2:uuid,5:time,30:field3,102:field4\n"
"     - maps the specified CSV column IDs to TrailDB fields\n"
"4) --csv-header\n"
"     - like 2) but reads the field names from the first row of the input\n"
"\n"
"Note that in all the cases above 'uuid' and 'time' must be specified.\n"
"\n"
"dump:\n"
"1) if no --fields in specified, all fields are output.\n"
"2) --fields uuid,time,field2,field3,...\n"
"    - outputs only the specified fields from TrailDB\n"
"\n"
"FILTER SPECIFICATION:\n"
"The --filter option specifies an event filter for dumping a subset of\n"
"events from an existing TrailDB. The filter is a boolean query, expressed\n"
"in Conjunctive Normal Form. Remember to surround the query in quotes.\n"
"\n"
"Syntax:\n"
" - Disjunctions (OR) are separated by whitespace.\n"
" - Conjunctions (AND) are separated by the '&' character.\n"
" - Terms are one of the following:\n"
"    - field_name=value (equals)\n"
"    - field_name!=value (not equals)\n"
"    - field_name=@filename (read value from a file. Useful for reading\n"
"      binary values or values including delimiter characters)\n"
"    - field= (empty value)\n"
"\n"
"Example:\n"
"--filter='author=Asimov & name=Foundation name=@book_name & price!='\n"
"(author is Asimov AND name is Foundation OR a name read from the file\n"
"'book_name' AND price is not empty)\n"
"\n"
);
exit(1);
}

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
        {"filter", required_argument, 0, 'F'},
        {"verbose", no_argument, 0, 'v'},
        {"tdb-format", required_argument, 0, -2},
        {"csv-header", no_argument, 0, -3},
        {"json-no-empty", no_argument, 0, -4},
        {"skip-bad-input", no_argument, 0, -5},
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
                        "cvji:o:f:F:d:t:",
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
            case 'F':
                options.filter_arg = optarg;
                break;
            case 'v':
                options.verbose = 1;
                break;
            case -2:
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
            case -3:
                options.csv_has_header = 1;
                break;
            case -4:
                options.json_no_empty = 1;
                break;
            case -5:
                options.skip_bad_input = 1;
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


