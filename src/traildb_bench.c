#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include <string.h>
#include "traildb.h"



static inline void _dump_item(const tdb* db, const tdb_item item_id)
{
   uint64_t value_length;
   const char* value = tdb_get_item_value(db, item_id, &value_length);

   printf("length=%u field=%i val=%i  value=0x",
	  (unsigned)value_length,
	  (unsigned)tdb_item_field(item_id),
	  (unsigned)tdb_item_val(item_id));
   if(value == NULL) {
      printf("(null)");
   } else {
      for(uint64_t hexidx = 0; value && hexidx < value_length; ++hexidx)
      {
	 printf("%ix", (int)value[hexidx]);
      }
   }
   putchar('\n');
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

      for(uint64_t i = 1; i < num_items; ++i)
      {
	 if(tdb_item_val(items[i]) != 0) {
	    uint64_t dummy;
	    tdb_get_item_value(db, items[i], &dummy);
	    ++items_decoded;
	 }

//	 printf("field[%u]: ", (unsigned)i);
//	 _dump_item(db, items[i]);
      }
   }

   free(items);

   printf("# items decoded: %llu\n", (unsigned long long)items_decoded);
}

static void print_help(const char* progname) {
   printf(
      "Usage: %s <command> [<mode-specific options>*]\n"
      "Available commands:\n"
      "    decode-all <database directory>*\n",
      progname);
}


int main(int argc, char** argv)
{
   const char* command;
   
   if(argc < 2) {
      print_help(argv[0]);
      return 1;
   }

   command = argv[1];
   if(strcmp(command, "decode-all") == 0)
   {
      for(int i = 2; i < argc; ++i) {
	 const char* path = argv[i];
         tdb* db = tdb_init();
	 int err = tdb_open(db, path);
	 if(err) {
	    printf("Error code %i while opening TDB at %s\n", err, path);
	    return 1;
	 }
	 get_all_and_decode(db);
	 tdb_close(db);
      }
   } else {
      print_help(argv[0]);
      return 1;
   }
   
   return 0;
}
