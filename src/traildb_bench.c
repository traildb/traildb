#include <stdio.h>
#include <assert.h>
#include <stdlib.h>
#include "traildb.h"



/**
 * calls tdb_get_trail over the full tdb
 */
static void cmd_get_full(const tdb* tdb)
{
   const uint64_t num_trails = tdb_num_trails(tdb);

   for(uint64_t trail_id = 0; trail_id < num_trails; ++trail_id) {
      tdb_item* items;
      uint64_t items_len;
      uint64_t num_items;
      const int err = tdb_get_trail(tdb, trail_id, &items, &items_len, &num_items, 0);

      assert(err == 0);
      free(items);
   }
}

int main(int argc, char** argv)
{
   if(argc < 2) {
      printf("Usage: %s <db file>\n",
	     argv[0]);
      return 1;
   }
   else {
      tdb* tdb = tdb_init();
      tdb_open(tdb, argv[0]);
#if 0
      tdb_willneed(tdb);
#endif
      cmd_get_full(tdb);
      tdb_close(tdb);
   }

   
}
