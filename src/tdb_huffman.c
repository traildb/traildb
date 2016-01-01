
#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>

#include "tdb_queue.h"
#include "tdb_profile.h"
#include "tdb_huffman.h"

#include "util.h"
#include "judy_128_map.h"

#define MIN(a,b) ((a)>(b)?(b):(a))

struct hnode{
    __uint128_t symbol;
    uint32_t code;
    uint32_t num_bits;
    uint64_t weight;
    struct hnode *left;
    struct hnode *right;
};

static void allocate_codewords(struct hnode *node,
                               uint32_t code,
                               uint32_t depth)
{
    if (node == NULL)
        return;
    if (depth < 16 && (node->right || node->left)){
        allocate_codewords(node->left, code, depth + 1);
        allocate_codewords(node->right, code | (1U << depth), depth + 1);
    }else{
        node->code = code;
        node->num_bits = depth;
    }
}

static struct hnode *pop_min_weight(struct hnode *symbols,
                                    uint32_t *num_symbols,
                                    struct tdb_queue *nodes)
{
    const struct hnode *n = (const struct hnode*)tdb_queue_peek(nodes);
    if (!*num_symbols || (n && n->weight < symbols[*num_symbols - 1].weight))
        return tdb_queue_pop(nodes);
    else if (*num_symbols)
        return &symbols[--*num_symbols];
    return NULL;
}

static int huffman_code(struct hnode *symbols, uint32_t num)
{
    struct tdb_queue *nodes = NULL;
    struct hnode *newnodes = NULL;
    uint32_t new_i = 0;

    if (!num)
        return 0;
    if (!(nodes = tdb_queue_new(num * 2)))
        return -1;
    if (!(newnodes = malloc(num * sizeof(struct hnode)))){
        tdb_queue_free(nodes);
        return -1;
    }

    /* construct the huffman tree bottom up */
    while (num || tdb_queue_length(nodes) > 1){
        struct hnode *new = &newnodes[new_i++];
        new->left = pop_min_weight(symbols, &num, nodes);
        new->right = pop_min_weight(symbols, &num, nodes);
        new->weight = (new->left ? new->left->weight: 0) +
                      (new->right ? new->right->weight: 0);
        tdb_queue_push(nodes, new);
    }
    /* allocate codewords top down (depth-first) */
    allocate_codewords(tdb_queue_pop(nodes), 0, 0);
    free(newnodes);
    tdb_queue_free(nodes);
    return 0;
}


static uint32_t sort_symbols(const struct judy_128_map *freqs,
                             uint64_t *totalfreq,
                             struct hnode *book)
{
    Word_t i;
    uint64_t num_symbols;
    struct sortpair *pairs = sort_j128m(freqs, &num_symbols);

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

#ifdef TDB_DEBUG_HUFFMAN
static void print_codeword(const struct hnode *node)
{
    uint32_t j;
    for (j = 0; j < node->num_bits; j++)
        fprintf(stderr, "%u", (node->code & (1U << j) ? 1: 0));
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
        uint64_t f = book[i].weight;
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

        fprintf(stderr, "%lu %2.3f %2.3f | ",
                f,
                100. * (double)f / (double)tot,
                100. * (double)cum / (double)tot);
        print_codeword(&book[i]);
        fprintf(stderr, "\n");
    }
}
#endif

static void make_codemap(struct hnode *nodes,
                         uint32_t num_symbols,
                         struct judy_128_map *codemap)
{
    uint32_t i = num_symbols;
    while (i--){
        if (nodes[i].num_bits){
            Word_t *ptr = j128m_insert(codemap, nodes[i].symbol);
            *ptr = nodes[i].code | (nodes[i].num_bits << 16);
        }
    }
}

struct field_stats *huff_field_stats(const uint64_t *field_cardinalities,
                                     uint64_t num_fields,
                                     uint64_t max_timestamp_delta)
{
    uint64_t i;
    struct field_stats *fstats;

    if (!(fstats = malloc(sizeof(struct field_stats) + num_fields * 4)))
        return NULL;

    fstats->field_id_bits = bits_needed(num_fields);
    fstats->field_bits[0] = bits_needed(max_timestamp_delta);
    for (i = 0; i < num_fields - 1; i++){
        fstats->field_bits[i + 1] = bits_needed(field_cardinalities[i]);
    }
    return fstats;
}

void huff_create_codemap(const struct judy_128_map *gram_freqs,
                         struct judy_128_map *codemap)
{
    struct hnode *nodes;
    uint64_t total_freq;
    uint32_t num_symbols;
    TDB_TIMER_DEF

    if (!(nodes = calloc(HUFF_CODEBOOK_SIZE, sizeof(struct hnode))))
        DIE("Could not allocate huffman codebook");

    TDB_TIMER_START
    num_symbols = sort_symbols(gram_freqs, &total_freq, nodes);
    TDB_TIMER_END("huffman/sort_symbols")

    TDB_TIMER_START
    huffman_code(nodes, num_symbols);
    TDB_TIMER_END("huffman/huffman_code")

#ifdef TDB_DEBUG_HUFFMAN
    if (getenv("TDB_DEBUG_HUFFMAN"))
        output_stats(nodes, num_symbols, total_freq);
#endif

    TDB_TIMER_START
    make_codemap(nodes, num_symbols, codemap);
    TDB_TIMER_END("huffman/make_codemap")

    free(nodes);
}

static inline void encode_gram(const struct judy_128_map *codemap,
                               __uint128_t gram,
                               char *buf,
                               uint64_t *offs,
                               const struct field_stats *fstats)
{
    const tdb_field field = tdb_item_field(HUFF_BIGRAM_TO_ITEM(gram));
    const tdb_val value = tdb_item_val(HUFF_BIGRAM_TO_ITEM(gram));
    const uint32_t literal_bits = 1 + fstats->field_id_bits +
                                  fstats->field_bits[field];

    uint64_t huff_code, huff_bits;
    Word_t *ptr = j128m_get(codemap, gram);

    if (ptr){
        /* codeword: prefix code by an up bit */
        huff_code = 1 | (HUFF_CODE(*ptr) << 1);
        huff_bits = HUFF_BITS(*ptr) + 1;
    }

    if (ptr && (HUFF_IS_BIGRAM(gram) || huff_bits < literal_bits)){
        /* write huffman-coded codeword */
        write_bits(buf, *offs, huff_code);
        *offs += huff_bits;
    }else if (HUFF_IS_BIGRAM(gram)){
        /* non-huffman bigrams are encoded as two unigrams */
        encode_gram(codemap, HUFF_BIGRAM_TO_ITEM(gram), buf, offs, fstats);
        encode_gram(codemap, HUFF_BIGRAM_OTHER_ITEM(gram), buf, offs, fstats);
    }else{
        /* write literal:
           [0 (1 bit) | field (field_bits) | value (field_bits[field])]

           huff_encoded_size_max_bits() must match with the above definition
           in tdb_huffman.h
        */
        write_bits(buf, *offs + 1, field);
        *offs += fstats->field_id_bits + 1;
        write_bits64(buf, *offs, value);
        *offs += fstats->field_bits[field];
    }
}

void huff_encode_grams(const struct judy_128_map *codemap,
                       const __uint128_t *grams,
                       uint64_t num_grams,
                       char *buf,
                       uint64_t *offs,
                       const struct field_stats *fstats)
{
    uint64_t i = 0;
    for (i = 0; i < num_grams; i++)
        encode_gram(codemap, grams[i], buf, offs, fstats);
}

static void *create_codebook_fun(__uint128_t symbol, Word_t *value, void *state)
{
    struct huff_codebook *book = (struct huff_codebook*)state;
    uint32_t code = HUFF_CODE(*value);
    uint32_t n = HUFF_BITS(*value);
    uint32_t j = 1U << (16 - n);

    while (j--){
        uint32_t k = code | (j << n);
        book[k].symbol = symbol;
        book[k].bits = n;
    }

    return state;
}

struct huff_codebook *huff_create_codebook(const struct judy_128_map *codemap,
                                           uint32_t *size)
{
    struct huff_codebook *book;

    *size = HUFF_CODEBOOK_SIZE * sizeof(struct huff_codebook);
    if (!(book = calloc(1, *size)))
        DIE("Could not allocate codebook in huff_create_codebook");

    j128m_fold(codemap, create_codebook_fun, book);

    return book;
}

