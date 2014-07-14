
#include <Judy.h>

#include "util.h"
#include "ddb_queue.h"
#include "ddb_profile.h"

#include "huffman.h"

#define MIN(a,b) ((a)>(b)?(b):(a))

struct hnode{
    uint32_t code;
    uint32_t num_bits;
    uint64_t symbol;
    uint64_t weight;
    struct hnode *left;
    struct hnode *right;
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

    /* construct the huffman tree bottom up */
    while (num || ddb_queue_length(nodes) > 1){
        struct hnode *new = &newnodes[new_i++];
        new->left = pop_min_weight(symbols, &num, nodes);
        new->right = pop_min_weight(symbols, &num, nodes);
        new->weight = (new->left ? new->left->weight: 0) +
                      (new->right ? new->right->weight: 0);
        ddb_queue_push(nodes, new);
    }
    /* allocate codewords top down (depth-first) */
    allocate_codewords(ddb_queue_pop(nodes), 0, 0);
    free(newnodes);
    ddb_queue_free(nodes);
    return 0;
}


static uint32_t sort_symbols(const Pvoid_t freqs,
                             uint64_t *totalfreq,
                             struct hnode *book)
{
    Word_t i;
    Word_t num_symbols;
    struct sortpair *pairs = sort_judyl(freqs, &num_symbols);

    *totalfreq = 0;
    for (i = 0; i < num_symbols; i++)
        *totalfreq += pairs[i].value;

    num_symbols = MIN(HUFF_CODEBOOK_SIZE, num_symbols);
    for (i = 0; i < num_symbols; i++){
        book[i].symbol = pairs[i].key;
        book[i].weight = pairs[i].value;
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
                         uint32_t num_symbols,
                         uint64_t tot)
{
    fprintf(stderr, "#codewords: %u\n", num_symbols);
    uint64_t cum = 0;
    uint32_t i;
    fprintf(stderr, "index) gramtype [field value] freq prob cum\n");

    for (i = 0; i < num_symbols; i++){

        long long unsigned int sym = book[i].symbol;
        long long unsigned int sym2 = sym >> 32;
        uint32_t f = book[i].weight;
        cum += f;

        fprintf(stderr, "%u) ", i);
        if (sym2 & 255){
            fprintf(stderr,
                    "bi [%llu %llu | %llu %llu] ",
                    sym & 255,
                    (sym >> 8) & ((1 << 24) - 1),
                    sym2 & 255,
                    sym2 >> 8);
        }else
            fprintf(stderr, "uni [%llu %llu] ", sym & 255, sym >> 8);

        fprintf(stderr, "%u %2.3f %2.3f | ",
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

static inline void encode_gram(const Pvoid_t codemap,
                               uint64_t gram,
                               char *buf,
                               uint64_t *offs)
{
    Word_t *ptr;

    JLG(ptr, codemap, gram);
    if (ptr){
        /* codeword: prefix code by an up bit */
        uint32_t code = 1 | (HUFF_CODE(*ptr) << 1);
        uint32_t bits = HUFF_BITS(*ptr);
        write_bits(buf, *offs, code);
        *offs += bits + 1;
    }else if ((gram >> 32) & 255){
        /* non-huffman bigrams are encoded as two unigrams */
        encode_gram(codemap, gram & UINT32_MAX, buf, offs);
        encode_gram(codemap, gram >> 32, buf, offs);
    }else{
        write_bits(buf, *offs + 1, gram & UINT32_MAX);
        *offs += 33;
    }
}

/* NB! This function assumes that buf is initially 2^32 / 8 = 512MB,
   initialized with zeros */
void huff_encode_grams(const Pvoid_t codemap,
                       uint32_t timestamp,
                       const uint64_t *grams,
                       uint32_t num_grams,
                       char *buf,
                       uint64_t *offs)
{
    uint32_t i = 0;
    uint64_t worstcase_bits = *offs + (num_grams + 1) * 2 * 33 + 64;
    if (worstcase_bits >= UINT32_MAX)
        DIE("Cookie trail too long: %llu bits\n",
            (unsigned long long)worstcase_bits);

    encode_gram(codemap, timestamp, buf, offs);
    for (i = 0; i < num_grams; i++)
        encode_gram(codemap, grams[i], buf, offs);
}

struct huff_codebook *huff_create_codebook(const Pvoid_t codemap,
                                           uint32_t *size)
{
    struct huff_codebook *book;
    Word_t symbol = 0;
    Word_t *ptr;

    *size = HUFF_CODEBOOK_SIZE * sizeof(struct huff_codebook);
    if (!(book = calloc(1, *size)))
        DIE("Could not allocate codebook in huff_create_codebook\n");

    JLF(ptr, codemap, symbol);
    while (ptr){
        uint32_t code = HUFF_CODE(*ptr);
        int n = HUFF_BITS(*ptr);
        int j = 1 << (16 - n);
        while (j--){
            int k = code | (j << n);
            book[k].symbol = symbol;
            book[k].bits = n;
        }
        JLN(ptr, codemap, symbol);
    }

    return book;
}

