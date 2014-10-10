#define _GNU_SOURCE

#include <sys/types.h>
#include <sys/stat.h>
#include <fcntl.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <stdint.h>

#include <Judy.h>

#include "discodb.h"
#include "mix.h"

#define MAX_EVENT_QUERY_LEN 1000000

struct qctx{
    struct ddb_query_clause *clauses;
    uint32_t num_clauses;
    uint32_t event_query_len;
    uint32_t event_query[0];
};

static uint32_t num_chars(const char *str, char chr)
{
    uint32_t n = 0;

    while (*str){
        if (*str == chr)
            ++n;
        ++str;
    }
    return n;
}

/*
   parse_query() looks hairy but it does not do anything special besides
   parsing the query string and looking up values.

   The main thing to note is that it builds two representations of the
   same query simultaneously: One for DiscoDB (which may be redundant),
   and one for event-level operations (event_query).

   The DiscoDB version is a straightforward list of structs. The event
   version is optimized for fast evaluation against events. It has the
   following format:

   [ num_terms | term1 | term2 | ... | num_terms | ... ]
    \--------------------------------/\---------------/
                  |                        |
                  Clause 1                 Clause 2

   That is, it contains clauses and terms as a flat array.
*/

static struct ddb_query_clause *parse_query(char *query,
                                            uint32_t *num_clauses,
                                            const struct trail_ctx *ctx,
                                            uint32_t *event_query,
                                            uint32_t *event_query_len)
{
    char *saveptr;
    char *clauseptr;
    struct ddb_query_clause *clauses;
    uint32_t max_num_clauses = num_chars(query, '&') + 1;
    const struct breadcrumbs *db = ctx->db;

    *event_query_len = *num_clauses = 0;

    if (!(clauses = malloc(max_num_clauses * sizeof(struct ddb_query_clause))))
        DIE("Could not allocate %u clauses\n", max_num_clauses);

    clauseptr = strtok_r(query, "&", &saveptr);
    while (clauseptr){
        char *saveptr1;
        uint32_t max_num_terms = num_chars(clauseptr, ' ') + 1;
        char *termptr = strtok_r(clauseptr, " ", &saveptr1);
        struct ddb_query_clause *clause = &clauses[*num_clauses];
        uint32_t clause_start = *event_query_len;
        uint32_t num_terms = 0;

        if (!(clause->terms =
              malloc(max_num_terms * sizeof(struct ddb_query_term))))
            DIE("Could not allocate %u clauses\n", max_num_terms);

        while (termptr){
            char *term;
            char *field_name = strsep(&termptr, ":");
            int field_idx = bd_lookup_field_index(db, field_name);

            if (field_idx == -1)
                DIE("Unrecognized field in q: %s\n", field_name);

            if (asprintf(&term, "%d:%s", field_idx + 1, termptr) == -1)
                DIE("Malloc failed in q/parse_query\n");

            clause->terms[num_terms].key.data = term;
            clause->terms[num_terms].key.length = strlen(term);
            clause->terms[num_terms++].nnot = 0;

            if (1 + *event_query_len == MAX_EVENT_QUERY_LEN)
                DIE("Too many terms in the query (found %u)\n",
                    *event_query_len);

            event_query[++*event_query_len] = bd_lookup_token(db,
                                                              termptr,
                                                              field_idx);
            if (!event_query[*event_query_len])
                MSG(ctx, "Unknown term '%s'\n", term);

            termptr = strtok_r(NULL, " ", &saveptr1);
        }
        clauseptr = strtok_r(NULL, "&", &saveptr);
        if (num_terms){
            clause->num_terms = num_terms;
            event_query[clause_start] = num_terms;
            ++*num_clauses;
            ++*event_query_len;
        }
    }
    return clauses;
}

static struct ddb *open_index(const char *root)
{
    char path[MAX_PATH_SIZE];
    struct ddb *db;
    int fd;

    make_path(path, "%s/trails.index", root);

    if ((fd = open(path, O_RDONLY)) == -1)
        return NULL;

    if (!(db = ddb_new()))
        DIE("Could not initialize index\n");

    if (ddb_load(db, fd)){
        const char *err;
        ddb_error(db, &err);
        DIE("Could not load index at %s: %s\n", path, err);
    }

    close(fd);
    return db;
}

static int match_index(struct trail_ctx *ctx, const struct qctx *q)
{
    struct ddb_cursor *cur;
    const struct ddb_entry *e;
    int tst, err;
    Word_t tmp;

    Pvoid_t new_matches = NULL;
    if (!(cur = ddb_query(ctx->db_index, q->clauses, q->num_clauses)))
        DIE("Query init failed (out of memory?)\n");

    while ((e = ddb_next(cur, &err))){
        Word_t row_id = *(uint32_t*)e->data;

        if (err)
            DIE("Query execution failed (out of memory?)\n");

        /* Intersect result set from the index with the currently
           matched rows */
        J1T(tst, ctx->matched_rows, row_id);
        if (tst)
            J1S(tst, new_matches, row_id);
    }

    ddb_free_cursor(cur);
    J1FA(tmp, ctx->matched_rows);
    /* Intersection replaces the old set of matched_rows */
    ctx->matched_rows = new_matches;

    return 0;
}

void op_help_q()
{

}

void *op_init_q(struct trail_ctx *ctx,
                const char *arg,
                int op_index,
                int num_ops,
                uint64_t *flags)
{
    char *query;
    struct qctx *q;

    if (!ctx->db)
        DIE("q requires a DB\n");
    if (!arg)
        DIE("q requires a string argument. See --help=q\n");

    if (!(query = strdup(arg)))
        DIE("Malloc failed in op_init_q\n");

    if (!(q = malloc(sizeof(struct qctx) + MAX_EVENT_QUERY_LEN * 4)))
        DIE("Malloc failed in op_init_q\n");

    q->clauses = parse_query(query,
                             &q->num_clauses,
                             ctx,
                             q->event_query,
                             &q->event_query_len);

    *flags = 0;


    if (!ctx->db_index && !ctx->opt_no_index)
        if (!(ctx->db_index = open_index(ctx->db_path)))
            MSG(ctx, "Query index not found at %s/index\n", ctx->db_path);

    if (ctx->db_index){
        *flags |= TRAIL_OP_DB;
        MSG(ctx, "Query index is enabled\n");

        /* We can skip trail decoding and evaluate the query only using the
           index under the following conditions:

             1. Index is enabled.
             2. Match-events mode not enabled (default).
             3. There is only one clause (i.e. no ANDs) in the query.

           If all the three conditions are true, index returns exactly the
           same result as what would be returned by checking events.
        */
        if (!(q->num_clauses == 1 && !ctx->opt_match_events)){
            *flags |= TRAIL_OP_EVENT;
            MSG(ctx, "Query (%dth op) is index-only\n", op_index);
        }
    }else
        *flags |= TRAIL_OP_EVENT;

    if (!(*flags & TRAIL_OP_DB))
        MSG(ctx, "Query index is disabled\n");

    free(query);
    return q;
}

int op_exec_q(struct trail_ctx *ctx,
              int mode,
              uint64_t row_id,
              const uint32_t *fields,
              uint32_t num_fields,
              const void *arg)
{
    const struct qctx *q = (const struct qctx*)arg;
    uint32_t j, i = 0;

    /* Check if event matches the given query. See
       the description of the event_query structure
       above.

       The key observation here is that 'fields' (events)
       contains exactly one value for each field, so we
       can look up values by field index efficiently.
    */
    while (i < q->event_query_len){
        uint32_t clause_len = q->event_query[i++];
        /* Evaluate one clause */
        for (j = 0; j < clause_len; j++){
            uint32_t term = q->event_query[i + j];
            if (term && fields[term & 255] == term)
                /* One of the terms match, clause ok */
                goto next_clause;
        }
        /* None of the terms in the clause match, query fails */
        return 1;
next_clause:
        i += clause_len;
    }
    /* All clauses ok, trail matches */
    return 0;
}
