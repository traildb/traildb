
#ifndef __DDB_STRUCTS_H__
#define __DDB_STRUCTS_H__

struct ddb_entry{
    const char *data;
    uint32_t length;
};

struct ddb_query_term{
    struct ddb_entry key;
    int nnot;
};

struct ddb_query_clause{
    struct ddb_query_term *terms;
    uint32_t num_terms;
};

#endif /* __DDB_STRUCTS_H__ */
