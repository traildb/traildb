
#include <Judy.h>

#include "util.h"
#include "ddb_queue.h"
#include "ddb_profile.h"

#include "huffman.h"

#define MIN(a,b) ((a)>(b)?(b):(a))
#define MAX_CANDIDATES 16777216

struct hnode{
    uint32_t code;
    uint32_t num_bits;
    uint32_t symbol;
    uint64_t weight;
    struct hnode *left;
    struct hnode *right;
};

struct sortpair{
    uint32_t freq;
    uint32_t symbol;
};

static void allocate_codewords(struct hnode *node, uint32_t code, int depth)
{
    if (node == NULL)
        return;
    if (depth < 16 && (node->right || node->left)){
        allocate_codewords(node->left, code, depth + 1);
        allocate_codewords(node->right, code | (1 << depth), depth + 1);
    }else{
        node->code = code;
        node->num_bits = depth;
    }
}

static struct hnode *pop_min_weight(struct hnode *symbols,
        int *num_symbols, struct ddb_queue *nodes)
{
    const struct hnode *n = (const struct hnode*)ddb_queue_peek(nodes);
    if (!*num_symbols || (n && n->weight < symbols[*num_symbols - 1].weight))
        return ddb_queue_pop(nodes);
    else if (*num_symbols)
        return &symbols[--*num_symbols];
    return NULL;
}

static int huffman_code(struct hnode *symbols, int num)
{
    struct ddb_queue *nodes = NULL;
    struct hnode *newnodes = NULL;
    int new_i = 0;

    if (!num)
        return 0;
    if (!(nodes = ddb_queue_new(num * 2)))
        return -1;
    if (!(newnodes = malloc(num * sizeof(struct hnode)))){
        ddb_queue_free(nodes);
        return -1;
    }

    while (num || ddb_queue_length(nodes) > 1){
        struct hnode *new = &newnodes[new_i++];
        new->left = pop_min_weight(symbols, &num, nodes);
        new->right = pop_min_weight(symbols, &num, nodes);
        new->weight = (new->left ? new->left->weight: 0) +
                      (new->right ? new->right->weight: 0);
        ddb_queue_push(nodes, new);
    }
    allocate_codewords(ddb_queue_pop(nodes), 0, 0);
    free(newnodes);
    ddb_queue_free(nodes);
    return 0;
}

int compare(const void *p1, const void *p2)
{
    const struct sortpair *x = (const struct sortpair*)p1;
    const struct sortpair *y = (const struct sortpair*)p2;

    if (x->freq > y->freq)
        return -1;
    else if (x->freq < y->freq)
        return 1;
    return 0;
}

static uint32_t sort_symbols(const Pvoid_t freqs,
                             uint64_t *totalfreq,
                             struct hnode *book)
{
    uint32_t i;
    struct sortpair *pairs;
    Word_t symbol;
    Word_t num_symbols;
    Word_t *freq;

    JLC(num_symbols, freqs, 0, -1);

    if (!(pairs = calloc(num_symbols, sizeof(struct sortpair))))
        DIE("Couldn't allocate sortpairs (%u pairs)\n",
            (uint32_t)num_symbols);

    symbol = i = *totalfreq = 0;
    JLF(freq, freqs, symbol);
    while (freq){
        pairs[i].symbol = symbol;
        pairs[i++].freq = *freq;
        *totalfreq += *freq;
        JLN(freq, freqs, symbol);
    }
    qsort(pairs, num_symbols, sizeof(struct sortpair), compare);

    num_symbols = MIN(HUFF_CODEBOOK_SIZE, num_symbols);
    for (i = 0; i < num_symbols; i++){
        book[i].symbol = pairs[i].symbol;
        book[i].weight = pairs[i].freq;
    }

    free(pairs);
    return (uint32_t)num_symbols;
}

static void print_codeword(const struct hnode *node)
{
    int j;
    for (j = 0; j < node->num_bits; j++)
        fprintf(stderr, "%d", (node->code & (1 << j) ? 1: 0));
}

static void output_stats(const struct hnode *book,
    uint32_t num_symbols, uint64_t tot)
{
    fprintf(stderr, "#codewords: %u\n", num_symbols);
    uint64_t cum = 0;
    uint32_t i;
    fprintf(stderr, "index) field value freq prob cum\n");
    for (i = 0; i < num_symbols; i++){
        uint32_t f = book[i].weight;
        cum += f;
        fprintf(stderr,
                "%u) %u %u %u %2.3f %2.3f | ",
                i,
                book[i].symbol & 255,
                book[i].symbol >> 8,
                f,
                100. * f / tot,
                100. * cum / tot);
        print_codeword(&book[i]);
        fprintf(stderr, "\n");
    }
}

static Pvoid_t make_codemap(struct hnode *nodes, uint32_t num_symbols)
{
    Pvoid_t codemap = NULL;

    uint32_t i = num_symbols;
    while (i--){
        Word_t *ptr;

        if (nodes[i].num_bits){
            JLI(ptr, codemap, nodes[i].symbol);
            *ptr = nodes[i].code | (nodes[i].num_bits << 16);
        }
    }
    return codemap;
}

Pvoid_t huff_create_codemap(const Pvoid_t key_freqs)
{
    struct hnode *nodes;
    Pvoid_t codemap;
    uint64_t total_freq;
    uint32_t num_symbols;
    DDB_TIMER_DEF

    if (!(nodes = calloc(HUFF_CODEBOOK_SIZE, sizeof(struct hnode))))
        DIE("Could not allocate huffman codebook\n");

    DDB_TIMER_START
    num_symbols = sort_symbols(key_freqs, &total_freq, nodes);
    DDB_TIMER_END("huffman/sort_symbols")

    DDB_TIMER_START
    huffman_code(nodes, num_symbols);
    DDB_TIMER_END("huffman/huffman_code")

    if (getenv("DEBUG_HUFFMAN"))
        output_stats(nodes, num_symbols, total_freq);

    DDB_TIMER_START
    codemap = make_codemap(nodes, num_symbols);
    DDB_TIMER_END("huffman/make_codemap")

    free(nodes);
    return codemap;
}
