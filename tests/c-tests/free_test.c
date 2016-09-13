#include <traildb.h>

int main(int argc, char **argv)
{
    //test all our free functions with NULL and make sure we don't segfault
    tdb_event_filter_free(NULL);
    tdb_cons_close(NULL);
    tdb_cursor_free(NULL);
    tdb_multi_cursor_free(NULL);
    return 0;
}

