extern(C):

    void* bd_open(const char*);
    void bd_close(void*);
    uint bd_num_cookies(void*);
    uint bd_num_fields(void*);
    ubyte* bd_lookup_cookie(void*, uint);
    long bd_rlookup_cookie(void*, ref ubyte[16]);
    char* bd_lookup_value(void*, uint);

    uint bd_trail_decode(void*, uint, uint*, uint, int);

    uint bd_lookup_token(void*, const char*, uint);

    int bd_has_cookie_index(void*);
