#define _DEFAULT_SOURCE
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include "traildb.h"


#define REPORT_ERROR(fmt, ...)				\
	do {						\
		fprintf(stderr, (fmt), ##__VA_ARGS__);	\
	} while(0)


/**
 * calls tdb_get_trail over the full tdb
 */
static void get_all_and_decode(const tdb* db)
{
	const uint64_t num_trails = tdb_num_trails(db);
	uint64_t items_decoded = 0;
	tdb_item* items = NULL;
	uint64_t items_len = 0;

	for(uint64_t trail_id = 0; trail_id < num_trails; ++trail_id) {
		uint64_t num_items;
		const int err = tdb_get_trail(db, trail_id, &items, &items_len, &num_items, 0);
		assert(err == 0);

		for(uint64_t i = 1; i < num_items; ++i) {
			if(tdb_item_val(items[i]) != 0) {
				uint64_t dummy;
				tdb_get_item_value(db, items[i], &dummy);
				++items_decoded;
			}
		}
	}

	free(items);
	printf("# items decoded: %llu\n", (unsigned long long)items_decoded);
}

static int cmd_get_all_and_decode(char** dbs, int argc)
{
	for(int i = 2; i < argc; ++i) {
		const char* path = dbs[i];
		tdb* db = tdb_init();
		int err = tdb_open(db, path);
		if(err) {
			printf("Error code %i while opening TDB at %s\n", err, path);
			return 1;
		}
		get_all_and_decode(db);
		tdb_close(db);
	}

	return 0;
}

static const char** duplicate_fieldids(const tdb* db)
{
	const uint64_t num_fields = tdb_num_fields(db);
	assert(num_fields <= TDB_MAX_NUM_FIELDS);

	const char** fieldids = malloc(num_fields * sizeof(char*));

	for(tdb_field i = 1; i < num_fields; ++i) {
		char* field_id = strdup(tdb_get_field_name(db, i));
		assert(field_id);
		fieldids[i-1] = field_id;
	}

	return fieldids;
}

static int cmd_append_all(const char* output_path, const char* input)
{
	tdb* db = tdb_init(); assert(db);
	int err = tdb_open(db, input);
	if(err) {
		REPORT_ERROR("Failed to open TDB. error=%i\n", err);
		return 1;
	}

	const uint64_t num_fields = tdb_num_fields(db) - 1;
	const char**   field_ids  = duplicate_fieldids(db);
	tdb_cons*      cons       = tdb_cons_init();
	assert(cons);

	err = tdb_cons_open(cons, output_path, field_ids, num_fields);
	if(err) {
		REPORT_ERROR("Failed to create TDB cons. error=%i\n", err);
		goto free_fieldids;
	}

	err = tdb_cons_append(cons, db);
	if(err) {
		REPORT_ERROR("Failed to append DB. error=%i\n", err);
		goto close_cons;
	}

	err = tdb_cons_finalize(cons);
	if(err) {
		REPORT_ERROR("Failed to finalize output DB. error=%i\n", err);
		goto close_cons;
	}

	printf("Successfully converted / rewritten DB.\n");


close_cons:
	tdb_cons_close(cons);

free_fieldids:
	/* to make the compiler not complain about casting const'ness away,
	   let's pull out this small, dirty trick */
	for(uint64_t i = 0; i < num_fields; ++i) {
		void* make_compiler_happy;
		memcpy(&make_compiler_happy, field_ids + i, sizeof(void*));
		free(make_compiler_happy);
	}
	free(field_ids);

	tdb_close(db);

	return err ? 1 : 0;
}

static void print_help(void)
{
	printf(
"Usage: traildb_bench <decode-all|append-all> [<mode-specific options>*]\n"
"\n"
"Synopsis:\n"
"  decode-all <database directory>*\n"
"  :: iterates over the complete DB, decoding\n"
"     every value encountered\n"
"  append-all <output path> <input path>\n"
"  :: copies data from one DB into a new or\n"
"     existing database. Has multiple use-cases,\n"
"     such as: converting DB formats, merging DBs, etc..\n"
"     Assumes DBs have identical fieldsets.\n");
}

#define IS_CMD(cmd, nargs) (strcmp(command, (cmd)) == 0 && argc >= (nargs))

int main(int argc, char** argv)
{
	const char* command;
   
	if(argc < 2) {
		print_help();
		return 1;
	}

	command = argv[1];
	if(IS_CMD("decode-all", 2)) {
		return cmd_get_all_and_decode(argv + 2, argc - 2);
	}
	else if(IS_CMD("append-all", 4)) {
		return cmd_append_all(argv[2], argv[3]);
	}
	else {
		print_help();
		return 1;
	}

	return 0;
}
