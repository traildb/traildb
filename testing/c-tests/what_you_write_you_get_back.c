#include <traildb.h>
#include <stdio.h>
#include <stdlib.h>
#include <assert.h>
#include <string.h>

int main(int argc, char** argv)
{
    tdb_cons* c = tdb_cons_new(argv[1], "victim\0murder_weapon\0motive\0", 3);
    assert(c && "Expected tdb_cons_new() to succeed.");

    tdb_cons_add(c, (uint8_t*) "aaaaaaaaaaaaaaaa", 1000, "dave\0knife\0money\0");
    tdb_cons_add(c, (uint8_t*) "aaaaaaaaaaaaaaaa", 1002, "cat\0pistol\0annoyance\0");
    tdb_cons_add(c, (uint8_t*) "aaaaaaaaaaaaaaaa", 1004, "plant\0water\0sadism\0");
    tdb_cons_add(c, (uint8_t*) "aaaaaaaaaaaaaaaa", 1011, "neo900\0funding\0none\0");

    tdb_cons_add(c, (uint8_t*) "baaaaaaaaaaaaaaa", 2000, "dave2\0knife2\0money2\0");
    tdb_cons_add(c, (uint8_t*) "baaaaaaaaaaaaaaa", 2002, "cat2\0pistol2\0annoyance2\0");
    tdb_cons_add(c, (uint8_t*) "baaaaaaaaaaaaaaa", 2004, "plant2\0water2\0sadism2\0");
    tdb_cons_add(c, (uint8_t*) "baaaaaaaaaaaaaaa", 2011, "neo9002\0funding2\0none2\0");

    tdb_cons_add(c, (uint8_t*) "baaaaaaaaaaaaaac", 3400, "dave3\0knife3\0money3\0");
    tdb_cons_add(c, (uint8_t*) "baaaaaaaaaaaaaac", 3402, "cat3\0pistol3\0annoyance3\0");
    tdb_cons_add(c, (uint8_t*) "baaaaaaaaaaaaaac", 3404, "plant3\0water3\0sadism3\0");
    tdb_cons_add(c, (uint8_t*) "baaaaaaaaaaaaaac", 3411, "neo900\0funding2\0none3\0");

    assert( tdb_cons_finalize(c, 0) == 0 );
    tdb_cons_free(c);

    tdb* t = tdb_open(argv[1]);
    if ( !t ) { fprintf(stderr, "tdb_open() failed.\n"); return -1; }

    assert(tdb_get_field(t, "victim") == 1);
    assert(tdb_get_field(t, "murder_weapon") == 2);
    assert(tdb_get_field(t, "motive") == 3);

    assert(!strcmp(tdb_get_field_name(t, 1), "victim"));
    assert(!strcmp(tdb_get_field_name(t, 2), "murder_weapon"));
    assert(!strcmp(tdb_get_field_name(t, 3), "motive"));

    assert(tdb_num_cookies(t) == 3);
    assert(tdb_num_fields(t) == 4);
    assert(tdb_num_events(t) == 12);
    assert(tdb_min_timestamp(t) == 1000);
    assert(tdb_max_timestamp(t) == 3411);
    assert(tdb_field_has_overflow_vals(t, 0) == 0);
    assert(tdb_field_has_overflow_vals(t, 1) == 0);
    assert(tdb_field_has_overflow_vals(t, 2) == 0);
    assert(tdb_field_has_overflow_vals(t, 3) == 0);

    uint64_t i1 = tdb_get_cookie_id(t, (uint8_t*) "aaaaaaaaaaaaaaaa");
    uint64_t i2 = tdb_get_cookie_id(t, (uint8_t*) "baaaaaaaaaaaaaaa");
    uint64_t i3 = tdb_get_cookie_id(t, (uint8_t*) "baaaaaaaaaaaaaac");

    assert(i1 != i2);
    assert(i3 != i2);
    assert(i3 != i1);

    uint32_t buffer[1000];

    for ( int x = 0; x < 100; ++x ) {
    assert(tdb_decode_trail(t, i1, buffer, 1000, 0) == (5*4));
    assert(buffer[0] == 1000);
    assert(!strcmp(tdb_get_item_value(t, buffer[1]), "dave"));
    assert(!strcmp(tdb_get_item_value(t, buffer[2]), "knife"));
    assert(!strcmp(tdb_get_item_value(t, buffer[3]), "money"));
    assert(buffer[4] == 0);
    assert(buffer[5] == 1002);
    assert(!strcmp(tdb_get_item_value(t, buffer[6]), "cat"));
    assert(!strcmp(tdb_get_item_value(t, buffer[7]), "pistol"));
    assert(!strcmp(tdb_get_item_value(t, buffer[8]), "annoyance"));
    assert(buffer[9] == 0);
    assert(buffer[10] == 1004);
    assert(!strcmp(tdb_get_item_value(t, buffer[11]), "plant"));
    assert(!strcmp(tdb_get_item_value(t, buffer[12]), "water"));
    assert(!strcmp(tdb_get_item_value(t, buffer[13]), "sadism"));
    assert(buffer[14] == 0);
    assert(buffer[15] == 1011);
    assert(!strcmp(tdb_get_item_value(t, buffer[16]), "neo900"));
    assert(!strcmp(tdb_get_item_value(t, buffer[17]), "funding"));
    assert(!strcmp(tdb_get_item_value(t, buffer[18]), "none"));
    assert(buffer[19] == 0);

    assert(tdb_decode_trail(t, i2, buffer, 1000, 0) == (5*4));
    assert(buffer[0] == 2000);
    assert(!strcmp(tdb_get_item_value(t, buffer[1]), "dave2"));
    assert(!strcmp(tdb_get_item_value(t, buffer[2]), "knife2"));
    assert(!strcmp(tdb_get_item_value(t, buffer[3]), "money2"));
    assert(buffer[4] == 0);
    assert(buffer[5] == 2002);
    assert(!strcmp(tdb_get_item_value(t, buffer[6]), "cat2"));
    assert(!strcmp(tdb_get_item_value(t, buffer[7]), "pistol2"));
    assert(!strcmp(tdb_get_item_value(t, buffer[8]), "annoyance2"));
    assert(buffer[9] == 0);
    assert(buffer[10] == 2004);
    assert(!strcmp(tdb_get_item_value(t, buffer[11]), "plant2"));
    assert(!strcmp(tdb_get_item_value(t, buffer[12]), "water2"));
    assert(!strcmp(tdb_get_item_value(t, buffer[13]), "sadism2"));
    assert(buffer[14] == 0);
    assert(buffer[15] == 2011);
    assert(!strcmp(tdb_get_item_value(t, buffer[16]), "neo9002"));
    assert(!strcmp(tdb_get_item_value(t, buffer[17]), "funding2"));
    assert(!strcmp(tdb_get_item_value(t, buffer[18]), "none2"));
    assert(buffer[19] == 0);

    assert(tdb_decode_trail(t, i3, buffer, 1000, 0) == (5*4));
    assert(buffer[0] == 3400);
    assert(!strcmp(tdb_get_item_value(t, buffer[1]), "dave3"));
    assert(!strcmp(tdb_get_item_value(t, buffer[2]), "knife3"));
    assert(!strcmp(tdb_get_item_value(t, buffer[3]), "money3"));
    assert(buffer[4] == 0);
    assert(buffer[5] == 3402);
    assert(!strcmp(tdb_get_item_value(t, buffer[6]), "cat3"));
    assert(!strcmp(tdb_get_item_value(t, buffer[7]), "pistol3"));
    assert(!strcmp(tdb_get_item_value(t, buffer[8]), "annoyance3"));
    assert(buffer[9] == 0);
    assert(buffer[10] == 3404);
    assert(!strcmp(tdb_get_item_value(t, buffer[11]), "plant3"));
    assert(!strcmp(tdb_get_item_value(t, buffer[12]), "water3"));
    assert(!strcmp(tdb_get_item_value(t, buffer[13]), "sadism3"));
    assert(buffer[14] == 0);
    assert(buffer[15] == 3411);
    assert(!strcmp(tdb_get_item_value(t, buffer[16]), "neo900"));
    assert(!strcmp(tdb_get_item_value(t, buffer[17]), "funding2"));
    assert(!strcmp(tdb_get_item_value(t, buffer[18]), "none3"));
    assert(buffer[19] == 0);
    }

    tdb_close(t);
    return 0;
}

