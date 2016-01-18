#define _DEFAULT_SOURCE
#include <string.h>
#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <inttypes.h>
#include "traildb.h"
#include "tdb_profile.h"


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

#define TIMED(msg, err, stmt)			\
do {						\
	TDB_TIMER_DEF;				\
	TDB_TIMER_START;			\
	(err) = (stmt);				\
	TDB_TIMER_END("traildb_bench/" msg);	\
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

static void dump_hex(const char* raw, uint64_t length)
{
	for(unsigned int i = 0; i+4 <= length; i += 4) {
		printf("%02x%02x%02x%02x ",
		       raw[i+0], raw[i+1], raw[i+2], raw[i+3]);
	}

	for(uint64_t i = length - (length % 4); i < length; ++i) {
		printf("%02x", raw[i]);
	}
}

/**
 * calls tdb_get_trail over the full tdb
 */
static int do_get_all_and_decode(const tdb* db, const char* path)
{
	tdb_error err;
	tdb_cursor* const c = tdb_cursor_new(db); assert(c);
	uint64_t items_decoded = 0;

	const uint64_t num_trails = tdb_num_trails(db);
	for(uint64_t trail_id = 0; trail_id < num_trails; ++trail_id) {
		err = tdb_get_trail(c, trail_id);
		if(err) {
			REPORT_ERROR("%s: failed to extract trail %llu. error=%i\n",
				     path, LLU(trail_id), err);
			goto err;
		}

		const uint64_t trail_len = tdb_get_trail_length(c);
		for(uint64_t i = 1; i < trail_len; ++i) {
			const tdb_event* const e = tdb_cursor_next(c);
			assert(e);
			for(uint64_t j = 0; j < e->num_items; ++j) {
				uint64_t dummy;
				tdb_get_item_value(db, e->items[i], &dummy);
				++items_decoded;
			}
		}
	}

	printf("# items decoded: %llu\n", LLU(items_decoded));

err:
	tdb_cursor_free(c);
	return err;
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
		TIMED("get_all", err, do_get_all_and_decode(db, path));
		tdb_close(db);
	}

	return 0;
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

	*field_ids = out;
	return 0;

err:
        free(out);
	return -1;
}

static int do_recode(tdb_cons* const cons, tdb* const db,
		     tdb_field* const field_ids, const int num_fieldids)
{
	int err;
	const uint64_t num_fields = tdb_num_fields(db);
	const uint64_t num_trails = tdb_num_trails(db);

	const char** const  values        = calloc(num_fields, sizeof(char*));
	uint64_t*    const  value_lengths = calloc(num_fields, sizeof(uint64_t));
	tdb_cursor*  const  c             = tdb_cursor_new(db);
	assert(values); assert(value_lengths); assert(c);

	/*
	 * for each trail
	 *   for each "record" in the timeline
	 *     extract/decode the relevant fields that are in field_ids
	 *     insert the record
	 */
	for(uint64_t trail_id = 0; trail_id < num_trails; ++trail_id) {
		err = tdb_get_trail(c, trail_id);
		if(err) {
			REPORT_ERROR("Failed to get trail (trail_id=%llu). error=%i\n",
				     LLU(trail_id), err);
			goto free_mem;
		}

		const uint64_t ev_len = tdb_get_trail_length(c);
		for(uint64_t ev_id = 0; ev_id < ev_len; ++ev_id) {
			const tdb_event* const e = tdb_cursor_next(c);
			assert(e);

			/* extract step */
			for(int field = 0; field < num_fieldids; ++field) {
				assert(0 < field_ids[field]);
				assert(field_ids[field] <= e->num_items);

				/** field ids start by '1' as column 0
				    historically is the timestamp
				    column. With tdb_event, the actual
				    fields are 0-indexed */
				values[field] = tdb_get_item_value(
					db, e->items[field_ids[field] - 1],
					&value_lengths[field]);
			}

			err = tdb_cons_add(cons, tdb_get_uuid(db, trail_id),
					   e->timestamp, values, value_lengths);
			if(err) {
				REPORT_ERROR("Failed to append record (trail_id=%llu, ev_id=%llu). error=%i\n",
					     LLU(trail_id), LLU(ev_id), err);
				goto free_mem;
			}
		}

		/* assert, all events from this trail were indeed processed */
		assert(tdb_cursor_next(c) == NULL);
	}

free_mem:
	tdb_cursor_free(c);
	free(value_lengths);
	free(values);
	return err;
}

/**
 * copies a subset of data from one DB into another one. The subset is
 * given by field names
 */
static int cmd_recode(const char* output_path, const char* input,
		      const char** field_names, int names_length)
{
	assert(names_length > 0);

	tdb* const db = tdb_init(); assert(db);
	int err = tdb_open(db, input);
	if(err) {
		REPORT_ERROR("Failed to open TDB. error=%i\n", err);
		return 1;
	}

	tdb_field* field_ids;
	err = resolve_fieldids(&field_ids, db, field_names, names_length);
	if(err < 0) {
		goto free_tdb;
	}

	tdb_cons* const cons = tdb_cons_init();
	assert(cons);

	err = tdb_cons_open(cons, output_path, field_names, (unsigned)names_length);
	if(err) {
		REPORT_ERROR("Failed to create TDB cons. error=%i\n", err);
		goto free_ids;
	}

	TIMED("recode", err, do_recode(cons, db, field_ids, names_length));
	if(err)
		goto close_cons;

	err = tdb_cons_finalize(cons);
	if(err) {
		REPORT_ERROR("Failed to finalize output DB. error=%i\n", err);
	}

close_cons:
	tdb_cons_close(cons);
free_ids:
	free(field_ids);
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

	TIMED("tdb_cons_append()", err, tdb_cons_append(cons, db));
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

static void dump_trail(const tdb* db, const uint8_t* uuid, tdb_cursor* c)
{
#define HEX4 "%02x%02x%02x%02x"
	printf("cookie " HEX4 HEX4 HEX4 HEX4 "\n",
	       uuid[0],  uuid[1],  uuid[2],  uuid[3],
	       uuid[4],  uuid[5],  uuid[6],  uuid[7],
	       uuid[8],  uuid[9],  uuid[10], uuid[11],
	       uuid[12], uuid[13], uuid[14], uuid[15]);
#undef HEX4

	const tdb_event* e;
	while((e = tdb_cursor_next(c))) {
		printf("ts=%" PRIu64 ":\n", e->timestamp);
		for(uint64_t i = 0; i < e->num_items; ++i) {
			const char* name = tdb_get_field_name(
				db, tdb_item_field(e->items[i]));
			uint64_t     v_len = 0;
			const char*  v     = tdb_get_item_value(
				db, e->items[i], &v_len);

			printf(" %s=", name);
			dump_hex(v, v_len);
			putchar('\n');
		}
		putchar('\n');
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

	tdb_cursor* const c = tdb_cursor_new(db);
	const uint64_t num_trails = tdb_num_trails(db);

	for(uint64_t trail_id = 0; trail_id < num_trails; ++trail_id) {
		err = tdb_get_trail(c, trail_id);
		if(err) {
			REPORT_ERROR("Failed to decode trail %llu. error=%i\n",
				     LLU(trail_id), err);

			goto out;
		}

		dump_trail(db, tdb_get_uuid(db, trail_id), c);
		putchar('\n');
	}

out:
	tdb_cursor_free(c);
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
