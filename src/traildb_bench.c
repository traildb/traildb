#define _DEFAULT_SOURCE
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include "traildb.h"


//#define DEBUG 1

#define LLU(val) ((unsigned long long)(val))

#define REPORT_ERROR(fmt, ...)				\
	do {						\
		fprintf(stderr, (fmt), ##__VA_ARGS__);	\
	} while(0)

#define REPORT_WARNING(fmt, ...)			\
	do {						\
		fprintf(stderr, ("WARNING: " fmt), ##__VA_ARGS__);	\
	} while(0)

/**
 * super dirty hack to make the compiler happy
 *
 * also: https://www.youtube.com/watch?v=1dV_6EtfvkM
 */
static inline const char** const_quirk(char** argv) {
	void* p;
	memcpy(&p, &argv, sizeof(argv));
	return p;
}

/// dump "raw" in groups of 4 bytes to stdout
static void dump_hex(const char* raw, uint64_t length)
{
/*	for(unsigned int i = 0; i < length; i += 4) {
		printf("%02x%02x%02x%02x ",
		       raw[i+0], raw[i+1], raw[i+2], raw[i+3]);
	}

	for(uint64_t i = length - (length % 4); i < length; ++i) {
		printf("%02x", raw[i]);
	}
*/
	for(uint64_t i = 0; i < length; ++i) {
		printf("%02x ", raw[i]);
	}
}

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
	printf("# items decoded: %llu\n", LLU(items_decoded));
}

static int cmd_get_all_and_decode(char** dbs, int argc)
{
	for(int i = 0; i < argc; ++i) {
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

static int compare_int(const void* lhs, const void* rhs)
{
	return *(const int*)lhs - *(const int*)rhs;
}

static int resolve_fieldids(tdb_field** field_ids, const tdb* db,
			    const char** field_names, int names_length)
{
	tdb_field* out = malloc((unsigned)names_length * sizeof(tdb_field));
	assert(field_ids != NULL);

	for(int i = 0; i < names_length; ++i)
	{
		const int err = tdb_get_field(db, field_names[i], &out[i]);
		if(err) {
			REPORT_ERROR("Could not find field name %s\n",
				     field_names[i]);
			goto err;
		}
	}

	qsort(out, (unsigned)names_length, sizeof(tdb_field), compare_int);

	/* super-naively skip dups ... */
	int skipped = 0;
	for(int i = 0; i + 1 < names_length - skipped; ++i)
	{
		if(out[i] == out[i+1]) {
			++skipped;
			memmove(&out[i], &out[i+1],
				(unsigned)(names_length - skipped - i));
		}
	}
	assert(names_length >= skipped); /// debugging

	if(skipped) {
		REPORT_WARNING("Skipping %u duplicate field names\n", skipped);
	}

#ifdef DEBUG
	for(int i = 0; i < names_length - skipped; ++i) {
		REPORT_WARNING("field_id[%i] = %i\n",
			       i, out[i]);
	}
#endif

	*field_ids = out;
	return names_length - skipped;

err:
        free(out);
	return -1;
}

/**
 * copies a subset of data from one DB into another one. The subset is
 * given by field names
 */
static int cmd_recode(const char* output_path, const char* input,
		      const char** field_names, int names_length)
{
	tdb* db = tdb_init(); assert(db);
	int err = tdb_open(db, input);
	if(err) {
		REPORT_ERROR("Failed to open TDB. error=%i\n", err);
		return 1;
	}

	tdb_cons* cons = tdb_cons_init();
	assert(cons);

	err = tdb_cons_open(cons, output_path, field_names, (unsigned)names_length);
	if(err) {
		REPORT_ERROR("Failed to create TDB cons. error=%i\n", err);
		goto free_tdb;
	}

	const uint64_t num_fields = tdb_num_fields(db);
	uint64_t       records_decoded = 0;
	tdb_item*      items = NULL;
	uint64_t       items_len = 0;
	tdb_field*     field_ids;
	const char**   values;
	uint64_t*      value_lengths;

	err = resolve_fieldids(&field_ids, db, field_names, names_length);
	if(err < 0)
		return 1;

	names_length = err;

	values        = calloc(num_fields, sizeof(char*));
	value_lengths = calloc(num_fields, sizeof(uint64_t));
	assert(values); assert(value_lengths);

	/*
	 * for each trail
	 *   for each "record" in the timeline
	 *     extract/decode the relevant fields that are in field_ids
	 *     insert the record
	 */
	const uint64_t num_trails = tdb_num_trails(db);
	for(uint64_t trail_id = 0; trail_id < num_trails && trail_id < 1000; ++trail_id) {
		uint64_t num_items;

		err = tdb_get_trail(db, trail_id, &items, &items_len, &num_items, 0);
		if(err) {
			REPORT_ERROR("Failed to get trail (trail_id=%llu). error=%i\n",
				     LLU(trail_id), err);
			goto out;
		}
		assert((num_items % (num_fields + 1)) == 0);

#ifdef DEBUG
		{
#define HEX4 "%02x%02x%02x%02x"
			const uint8_t* uuid = tdb_get_uuid(db, trail_id);
			printf("cookie " HEX4 HEX4 HEX4 HEX4 "\n",
			       uuid[0],  uuid[1],  uuid[2],  uuid[3],
			       uuid[4],  uuid[5],  uuid[6],  uuid[7],
			       uuid[8],  uuid[9],  uuid[10], uuid[11],
			       uuid[12], uuid[13], uuid[14], uuid[15]);
#undef HEX4
		}
#endif		

		for(uint64_t record_id = 0; record_id < num_items; record_id += num_fields + 1) {
			const tdb_item* const record = items + record_id;

			/// assert record is indeed pointing to the
			/// beginning of a new record :)
			assert(record[num_fields] == 0);

			/* extract step */
			for(int field = 0; field < names_length; ++field) {
				assert(record_id + field_ids[field] < num_items);
				values[field] = tdb_get_item_value(
					db, record[field_ids[field]],
					&value_lengths[field]);
				if(values[field]) {
#ifdef DEBUG
					printf("values[%llu][%i] = (%u)%s\n",
					       LLU(record_id), field, (unsigned)value_lengths[field], values[field]);
#endif
				}
			}

			err = tdb_cons_add(cons, tdb_get_uuid(db, trail_id),
					   record[0], values, value_lengths);
			if(err) {
				REPORT_ERROR("Failed to append record (trail_id=%llu, record_id=%llu). error=%i\n",
					     LLU(trail_id), LLU(record_id), err);
				goto out;
			}
//			goto breaker;
		}
		records_decoded += num_items / (num_fields + 1);
	}
//breaker:
	err = tdb_cons_finalize(cons, 0);
	if(err) {
		REPORT_ERROR("Failed to finalize output DB. error=%i\n", err);
		goto out;
	}

	printf("Successfully recoded DB. (#records written: %llu)\n",
	       LLU(records_decoded));


out:
	if(items != NULL)
		free(items);
	free(value_lengths);
	free(values);
	free(field_ids);
	tdb_cons_close(cons);

free_tdb:
	tdb_close(db);

	return err;
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

static void dump_trail(const tdb* db, const uint8_t* uuid,
		       tdb_item* items, uint64_t items_length)
{
#define HEX4 "%02x%02x%02x%02x"
	printf("cookie " HEX4 HEX4 HEX4 HEX4 "\n",
	       uuid[0],  uuid[1],  uuid[2],  uuid[3],
	       uuid[4],  uuid[5],  uuid[6],  uuid[7],
	       uuid[8],  uuid[9],  uuid[10], uuid[11],
	       uuid[12], uuid[13], uuid[14], uuid[15]);
#undef HEX4

	int print_timestamp = 1;
	for(unsigned int i = 0; i < items_length; ++i) {
		if(print_timestamp) {
			/* every record starts with the timestamp */
			printf("ts=%llu:\n", LLU(items[i]));
			print_timestamp = 0;
		}
		else if(items[i] == 0) {
			/* int(0) marks a new record */
			putchar('\n');
			print_timestamp = 1;
		}
		else {
			uint64_t value_length = 0;
			const char* value = tdb_get_item_value(db, items[i], &value_length);
			printf(" %s=",
			       tdb_get_field_name(db, tdb_item_field(items[i])));
			dump_hex(value, value_length);
			putchar('\n');
		}
	}
}

static int cmd_dump(const char* db_path)
{
	tdb* db = tdb_init(); assert(db);
	int err = tdb_open(db, db_path);
	if(err) {
		REPORT_ERROR("Failed to open TDB at directory %s. error=%i\n",
			     db_path, err);
		return 1;
	}

	tdb_item* items = NULL;
	uint64_t items_len = 0;
	const uint64_t num_trails = tdb_num_trails(db);

	for(uint64_t trail_id = 0; trail_id < num_trails; ++trail_id) {
		uint64_t items_decoded = 0;

		err = tdb_get_trail(db, trail_id, &items, &items_len, &items_decoded, 0);
		if(err) {
			REPORT_ERROR("Failed to decode trail %llu. error=%i\n",
				     LLU(trail_id), err);

			goto out;
		}

		dump_trail(db, tdb_get_uuid(db, trail_id), items, items_decoded);
		putchar('\n');
	}

out:
	if(items != NULL)
		free(items);
	tdb_close(db);
	return err;
}

static int cmd_info(const char* db_path)
{
	tdb* db = tdb_init(); assert(db);
	int err = tdb_open(db, db_path);
	if(err) {
		REPORT_ERROR("Failed to open TDB at directory %s. error=%i\n",
			     db_path, err);
		return 1;
	}

	printf("DB at %s:\n"
	       " version: %llu\n"
	       " #trails: %llu\n"
	       " #events: %llu\n"
	       " #fields: %llu\n"
	       "\n"
	       " min timestamp: %llu\n"
	       " max timestamp: %llu\n",
	       db_path,
	       LLU(tdb_version(db)),
	       LLU(tdb_num_trails(db)),
	       LLU(tdb_num_events(db)),
	       LLU(tdb_num_fields(db)),
	       LLU(tdb_min_timestamp(db)),
	       LLU(tdb_max_timestamp(db)));

       printf("\nColumns: \n");
       printf(" field[00] = %s (implicit)\n", tdb_get_field_name(db, 0 ));
       for(unsigned fid = 1; fid < tdb_num_fields(db); ++fid) {
	       printf(" field[%02u] = %s\n", fid, tdb_get_field_name(db, fid));
       }

       tdb_close(db);
       return err;
}

static void print_help(void)
{
	printf(
"Usage: traildb_bench <command> [<mode-specific options>*]\n"
"\n"
"Available commands:\n"
"  decode-all <database directory>*\n"
"  :: iterates over the complete DB, decoding\n"
"     every value encountered\n"
"  append-all <output path> <input path>\n"
"  :: copies data from one DB into a new or\n"
"     existing database. Has multiple use-cases,\n"
"     such as: converting DB formats, merging DBs, etc..\n"
"     Assumes DBs have identical fieldsets.\n"
"  recode <output path> <input path> <column name>+\n"
"  :: copies the given (sub)set of columns from the DB at\n"
"      /input path/ into /output path/\n"
"  info <path>\n"
"  :: displays information on a TDB\n"
"  dump <path>\n"
"  :: dumps contents of a traildb in a most primitive way.\n"	       
		);
}

#define IS_CMD(cmd, nargs) (strcmp(command, (cmd)) == 0 && argc >= (nargs + 2))

int main(int argc, char** argv)
{
	const char* command;
   
	if(argc < 2) {
		print_help();
		return 1;
	}

	command = argv[1];
	if(IS_CMD("decode-all", 1)) {
		return cmd_get_all_and_decode(argv + 2, argc - 2);
	}
	else if(IS_CMD("append-all", 2)) {
		return cmd_append_all(argv[2], argv[3]);
	}
	else if(IS_CMD("recode", 3)) {
		return cmd_recode(
			argv[2], argv[3], const_quirk(argv + 4), argc - 4);
	}
	else if(IS_CMD("info", 1)) {
		return cmd_info(argv[2]);
	}
	else if(IS_CMD("dump", 1)) {
		return cmd_dump(argv[2]);
	}
	else {
		print_help();
		return 1;
	}

	return 0;
}
