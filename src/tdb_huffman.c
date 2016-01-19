#define _DEFAULT_SOURCE

#include <stdlib.h>
#include <stdio.h>
#include <stdint.h>
#include <string.h>
#include <sys/mman.h>
#include <sys/stat.h>

#include "tdb_queue.h"
#include "tdb_profile.h"
#include "tdb_huffman.h"
#include "tdb_error.h"

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

struct sortpair{
    __uint128_t key;
    Word_t value;
};

static uint8_t bits_needed(uint64_t max)
{
    uint64_t x = max;
    uint8_t bits = x ? 0: 1;
    while (x){
        x >>= 1;
        ++bits;
    }
    return bits;
}

static int compare(const void *p1, const void *p2)
{
    const struct sortpair *x = (const struct sortpair*)p1;
    const struct sortpair *y = (const struct sortpair*)p2;

    if (x->value > y->value)
        return -1;
    else if (x->value < y->value)
        return 1;
    return 0;
}

static void *sort_j128m_fun(__uint128_t key, Word_t *value, void *state)
{
    struct sortpair *pair = (struct sortpair*)state;

    pair->key = key;
    pair->value = *value;

    return ++pair;
}

static struct sortpair *sort_j128m(const struct judy_128_map *j128m,
                                   uint64_t *num_items)
{
    struct sortpair *pairs;

    *num_items = j128m_num_keys(j128m);

    if (!(pairs = calloc(*num_items, sizeof(struct sortpair))))
        return NULL;

    if (*num_items == 0)
        return pairs;

    j128m_fold(j128m, sort_j128m_fun, pairs);

    qsort(pairs, *num_items, sizeof(struct sortpair), compare);
    return pairs;
}

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
        return TDB_ERR_NOMEM;
    if (!(newnodes = malloc(num * sizeof(struct hnode)))){
        tdb_queue_free(nodes);
        return TDB_ERR_NOMEM;
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


static int sort_symbols(const struct judy_128_map *freqs,
                        uint64_t *totalfreq,
                        uint32_t *num_symbols,
                        struct hnode *book)
{
    uint64_t i;
    struct sortpair *pairs;
    uint64_t num;

    if (!(pairs = sort_j128m(freqs, &num)))
        return TDB_ERR_NOMEM;

    *totalfreq = 0;
    for (i = 0; i < num; i++)
        *totalfreq += pairs[i].value;

    *num_symbols = (uint32_t)(MIN(HUFF_CODEBOOK_SIZE, num));
    for (i = 0; i < *num_symbols; i++){
        book[i].symbol = pairs[i].key;
        book[i].weight = pairs[i].value;
    }

    free(pairs);
    return 0;
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

static int make_codemap(struct hnode *nodes,
                        uint32_t num_symbols,
                        struct judy_128_map *codemap)
{
    uint32_t i = num_symbols;
    while (i--){
        if (nodes[i].num_bits){
            /* TODO TDB_ERR_NOMEM handling */
            Word_t *ptr = j128m_insert(codemap, nodes[i].symbol);
            *ptr = nodes[i].code | (nodes[i].num_bits << 16);
        }
    }

    return 0;
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
    for (i = 0; i < num_fields - 1; i++)
        fstats->field_bits[i + 1] = bits_needed(field_cardinalities[i]);
    return fstats;
}

int huff_create_codemap(const struct judy_128_map *gram_freqs,
                        struct judy_128_map *codemap)
{
    struct hnode *nodes;
    uint64_t total_freq;
    uint32_t num_symbols;
    int ret = 0;
    TDB_TIMER_DEF

    if (!(nodes = calloc(HUFF_CODEBOOK_SIZE, sizeof(struct hnode)))){
        ret = TDB_ERR_NOMEM;
        goto done;
    }

    TDB_TIMER_START
    if ((ret = sort_symbols(gram_freqs, &total_freq, &num_symbols, nodes)))
        goto done;

    TDB_TIMER_END("huffman/sort_symbols")

    TDB_TIMER_START
    if ((ret = huffman_code(nodes, num_symbols)))
        goto done;
    TDB_TIMER_END("huffman/huffman_code")

#ifdef TDB_DEBUG_HUFFMAN
    if (getenv("TDB_DEBUG_HUFFMAN"))
        output_stats(nodes, num_symbols, total_freq);
#endif

    TDB_TIMER_START
    if ((ret = make_codemap(nodes, num_symbols, codemap)))
        goto done;
    TDB_TIMER_END("huffman/make_codemap")

done:
    free(nodes);
    return ret;
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
        huff_code = 1U | (((uint32_t)HUFF_CODE(*ptr)) << 1U);
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
        return NULL;

    j128m_fold(codemap, create_codebook_fun, book);

    return book;
}

/*
this function converts old 64-bit symbols in v0 to new 128-bit
symbols in v1
*/
int huff_convert_v0_codebook(struct tdb_file *codebook)
{
    const struct huff_codebook_v0{
        uint64_t symbol;
        uint32_t bits;
    } __attribute__((packed)) *old =
        (const struct huff_codebook_v0*)codebook->data;

    uint64_t i;
    uint64_t size = HUFF_CODEBOOK_SIZE * sizeof(struct huff_codebook);
    struct huff_codebook *new;

    /*
    we want to allocate memory with mmap() and not malloc() so that tdb_file
    can be munmap()'ed as usual
    */
    void *p = mmap(NULL,
                   size,
                   PROT_READ | PROT_WRITE,
                   MAP_PRIVATE | MAP_ANONYMOUS,
                   -1,
                   0);

    if (p == MAP_FAILED)
        return TDB_ERR_NOMEM;

    new = (struct huff_codebook*)p;

    for (i = 0; i < HUFF_CODEBOOK_SIZE; i++){
        /* extract the second part of the bigram */
        __uint128_t gram = old[i].symbol >> 32;
        gram <<= 64;
        /* extract the first part of the bigram */
        gram |= (old[i].symbol & UINT32_MAX);
        new[i].symbol = gram;
        new[i].bits = old[i].bits;
    }

    munmap(codebook->ptr, codebook->mmap_size);
    codebook->data = codebook->ptr = p;
    codebook->size = codebook->mmap_size = size;

    return 0;
}

