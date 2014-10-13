extern(C):

    void* tdb_open(const char*);
    void tdb_close(void*);

    uint tdb_get_val(void*, uint, const char*);
    char* tdb_get_item_value(void*, uint);
    ubyte* tdb_get_cookie(void*, uint);
    long tdb_get_cookie_id(void*, ref ubyte[16]);
    int tdb_has_cookie_index(void*);

    uint tdb_num_cookies(void*);
    uint tdb_num_fields(void*);

    uint tdb_decode_trail(void*, uint, uint*, uint, int);

