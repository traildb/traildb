
#include <string.h>
#include <stdint.h>
#include <stdlib.h>

#include <Judy.h>

#include "util.h"
#include "trail_encode.h"

#define SAMPLE_SIZE (0.1 * RAND_MAX)
#define RANDOM_SEED 238713
#define UNIGRAM_SUPPORT 0.00001

#define FIELD_IDX_1(x) (x & 255)
#define FIELD_IDX_2(x) ((x >> 32) & 255)

typedef void (*logline_op)(const uint32_t *encoded,
                           uint32_t n,
                           const struct cookie_logline *line);

static uint32_t get_sample_size()
{
    if (getenv("BD_SAMPLE_SIZE")){
        char *endptr;
        double d = strtod(getenv("BD_SAMPLE_SIZE"), &endptr);
        if (*endptr || d < 0.01 || d > 1.0)
            DIE("Invalid BD_SAMPLE_SIZE\n");
        else
            return d * RAND_MAX;
    }else
        return SAMPLE_SIZE;

}

static void logline_fold(logline_op op,
                         const struct cookie_logline *grouped,
                         uint64_t num_loglines,
                         const uint32_t *values,
                             uint32_t num_fields)
    {
        uint32_t *prev_values = NULL;
        uint32_t *encoded = NULL;
        uint32_t encoded_size = 0;
        uint64_t i = 0;
        unsigned int rand_state = RANDOM_SEED;
        const uint32_t sample_size = get_sample_size();

        if (!(prev_values = malloc(num_fields * 4)))
            DIE("Could not allocated %u fields in edge_encode_values\n",
                num_fields);

    /* this function scans through *all* unencoded data, takes a sample
       of cookies, edge-encodes loglines for a cookie, and calls the
       given function (op) for each logline */

    while (i < num_loglines){
        /* NB: We sample cookies, not loglines below. We can't edge-encode
           *and* sample loglines at the same time effiently.

           If data overall is very unevenly distributed over cookies, sampling
           be cookies will produce suboptimal results.
        */
        uint64_t cookie_id = grouped[i].cookie_id;

        if (rand_r(&rand_state) < sample_size){

            memset(prev_values, 0, num_fields * 4);

            for (;i < num_loglines && grouped[i].cookie_id == cookie_id; i++){
                /* consider only valid timestamps (first byte = 0) */
                if ((grouped[i].timestamp & 255) == 0){
                    uint32_t n = edge_encode_fields(values,
                                                    &encoded,
                                                    &encoded_size,
                                                    prev_values,
                                                    &grouped[i]);

                    op(encoded, n, &grouped[i]);
                }
            }
        }else
            /* given that we are sampling cookies, we need to skip all loglines
               related to a cookie not included in the sample */
            for (;i < num_loglines && grouped[i].cookie_id == cookie_id; i++);
    }

    free(encoded);
    free(prev_values);
}

void init_gram_bufs(struct gram_bufs *b, uint32_t num_fields)
{
    if (!(b->chosen = malloc(num_fields * num_fields * 8)))
        DIE("Could not allocate bigram gram_buf (%u fields)\n", num_fields);

    if (!(b->scores = malloc(num_fields * num_fields * 8)))
        DIE("Could not allocate scores gram_buf (%u fields)\n", num_fields);

    if (!(b->covered = malloc(num_fields)))
        DIE("Could not allocate covered gram_buf (%u fields)\n", num_fields);

    b->num_fields = num_fields;
}

void free_gram_bufs(struct gram_bufs *b)
{
    free(b->chosen);
    free(b->scores);
    free(b->covered);
}

/* given a set of edge-encoded values (encoded), choose a set of unigrams
   and bigrams that cover the original set. In essence, this tries to
   solve Weigted Exact Cover Problem for the universe of 'encoded'. */
uint32_t choose_grams(const uint32_t *encoded,
                      uint32_t num_encoded,
                      const Pvoid_t gram_freqs,
                      struct gram_bufs *g,
                      uint64_t *grams)
{
    uint32_t i, j, k, n = 0;

    memset(g->covered, 0, g->num_fields);

    /* First, produce all candidate bigrams for this set. */
    for (k = 0, i = 0; i < num_encoded; i++){
        uint64_t unigram1 = ((uint64_t)encoded[i]) << 32;
        Word_t *ptr;

        JLF(ptr, gram_freqs, unigram1);
        if (ptr && (unigram1 >> 32) == encoded[i]){
            for (j = i + 1; j < num_encoded; j++){
                uint64_t bigram = ((uint64_t)encoded[j]) | unigram1;
                JLG(ptr, gram_freqs, bigram);
                if (ptr){
                    g->chosen[k] = bigram;
                    g->scores[k++] = *ptr;
                }
            }
        }
    }

    /* Pick non-overlapping histograms, in the order of descending score.
       As we go, mark fields covered (consumed) in the set. */
    while (1){
        uint32_t max_idx = 0;
        uint32_t max_score = 0;

        for (i = 0; i < k; i++)
            /* consider only bigrams whose both unigrams are non-covered */
            if (!(g->covered[FIELD_IDX_1(g->chosen[i])] ||
                  g->covered[FIELD_IDX_2(g->chosen[i])]) &&
                  g->scores[i] > max_score){

                max_score = g->scores[i];
                max_idx = i;
            }

        if (max_score){
            /* mark both unigrams of this bigram covered */
            uint64_t chosen = g->chosen[max_idx];
            g->covered[FIELD_IDX_1(chosen)] = 1;
            g->covered[FIELD_IDX_2(chosen)] = 1;
            grams[n++] = chosen;
        }else
            /* all bigrams used */
            break;
    }

    /* Finally, add all remaining unigrams to the result set which have not
       been covered by any bigrams */
    for (i = 0; i < num_encoded; i++)
        if (!g->covered[encoded[i] & 255])
            grams[n++] = encoded[i];

    return n;
}

static Pvoid_t find_candidates(const Pvoid_t unigram_freqs)
{
    Pvoid_t candidates = NULL;
    Word_t idx = 0;
    Word_t *ptr;
    uint64_t num_values = 0;
    uint64_t support;

    /* find all non-timestamp unigrams whose probability of
       occurrence is greater than UNIGRAM_SUPPORT */

    JLF(ptr, unigram_freqs, idx);
    while (ptr){
        num_values += *ptr;
        JLN(ptr, unigram_freqs, idx);
    }

    support = num_values * UNIGRAM_SUPPORT;
    idx = 0;

    JLF(ptr, unigram_freqs, idx);
    while (ptr){
        int tmp;
        if (*ptr > support)
            J1S(tmp, candidates, idx);
        JLN(ptr, unigram_freqs, idx);
    }

    return candidates;
}

Pvoid_t make_grams(const struct cookie_logline *grouped,
                   uint64_t num_loglines,
                   const uint32_t *values,
                   uint32_t num_fields,
                   const Pvoid_t unigram_freqs)
{
    Pvoid_t bigram_freqs = NULL;
    Pvoid_t final_freqs = NULL;
    Word_t *ptr;
    Word_t tmp;
    uint64_t *grams;
    struct gram_bufs gbufs;

    init_gram_bufs(&gbufs, num_fields);

    /* below is a very simple version of the Apriori algorithm
       for finding frequent sets (bigrams) */

    /* find unigrams that are sufficiently frequent */
    Pvoid_t candidates = find_candidates(unigram_freqs);

    if (!(grams = malloc(num_fields * 8)))
        DIE("Could not allocate grams buf (%u fields)\n", num_fields);

    /* first, collect frequencies of *all* occurring bigrams of
       candidate unigrams */
    void all_bigrams(const uint32_t *encoded,
                     uint32_t n,
                     const struct cookie_logline *line){

        int set, i, j;

        for (i = 0; i < n; i++){
            uint64_t unigram1 = encoded[i];
            J1T(set, candidates, unigram1);
            if (set){
                for (j = i + 1; j < n; j++){
                    uint64_t unigram2 = encoded[j];
                    J1T(set, candidates, unigram2);
                    if (set){
                        Word_t bigram = unigram2 | (unigram1 << 32);
                        JLI(ptr, bigram_freqs, bigram);
                        ++*ptr;
                    }
                }
            }
        }
    }

    /* secondly, collect frequencies of non-overlapping bigrams and
       unigrams (exact covering set for each logline) */
    void choose_bigrams(const uint32_t *encoded,
                        uint32_t num_encoded,
                        const struct cookie_logline *line){

        uint32_t n = choose_grams(encoded,
                                  num_encoded,
                                  bigram_freqs,
                                  &gbufs,
                                  grams);
        while (n--){
            JLI(ptr, final_freqs, grams[n]);
            ++*ptr;
        }
        /* include timestamp delta -frequencies in the final set */
        JLI(ptr, final_freqs, line->timestamp);
        ++*ptr;
    }

    /* execute the above operation, scanning over data twice */
    logline_fold(all_bigrams, grouped, num_loglines, values, num_fields);
    logline_fold(choose_bigrams, grouped, num_loglines, values, num_fields);

    J1FA(tmp, candidates);
    JLFA(tmp, bigram_freqs);
    free_gram_bufs(&gbufs);

    /* final_freqs is a combination of bigrams and unigrams with their actual,
       non-overlapping frequencies */
    return final_freqs;
}

Pvoid_t collect_unigrams(const struct cookie_logline *grouped,
                         uint64_t num_loglines,
                         const uint32_t *values,
                         uint32_t num_fields)
{
    Pvoid_t unigram_freqs = NULL;

    /* calculate frequencies of all values */

    void op(const uint32_t *encoded,
            uint32_t n,
            const struct cookie_logline *line){

        Word_t *ptr;

        while (n--){
            JLI(ptr, unigram_freqs, encoded[n]);
            ++*ptr;
        }
    }

    logline_fold(op, grouped, num_loglines, values, num_fields);
    return unigram_freqs;
}

