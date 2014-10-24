
#if 0
void bd_trail_all_freqs(struct breadcrumbs *bd)
{
    const uint32_t *toc = (const uint32_t*)bd->trails.data;
    uint64_t i;
    Pvoid_t freqs = NULL;
    const struct huff_codebook *codebook =
        (const struct huff_codebook*)bd->codebook.data;
    TDB_TIMER_DEF
    Word_t *ptr;
    Word_t idx;

    TDB_TIMER_START
    for (i = 0; i < bd->num_cookies; i++){
        const char *data = &bd->trails.data[toc[i]];
        uint64_t size = 8 * (toc[i + 1] - toc[i]);
        uint64_t offs = 3;

        size -= read_bits(data, 0, 3);

        while (offs < size){
            uint32_t val = huff_decode_value(codebook, data, &offs);

            JLI(ptr, freqs, val);
            ++*ptr;
        }
    }
    TDB_TIMER_END("decoder/trail_all_freqs");
    /*
    idx = 0;
    JLF(ptr, freqs, idx);
    while (ptr){
        printf("%llu %llu\n", idx, *ptr);
        JLN(ptr, freqs, idx);
    }
    */
}
#endif

#if 0
uint32_t bd_trail_value_freqs(const struct breadcrumbs *bd,
                              uint32_t *trail_indices,
                              uint32_t num_trail_indices,
                              uint32_t *dst_values,
                              uint32_t *dst_freqs,
                              uint32_t dst_size)
{
    /* Use Judy1 to check that only one distinct value per cookie is added to freqs */
    /* no nulls, no timestamps */
    /* return number of top values added to dst (<= dst_size) */
    return 0;
}
#endif

#if 0
static void find_bigrams(struct breadcrumbs *bd, Pvoid_t freqs)
{
    const uint32_t *toc = (const uint32_t*)bd->trails.data;
    uint64_t i;
    Pvoid_t bi_freqs = NULL;
    const struct huff_codebook *codebook =
        (const struct huff_codebook*)bd->codebook.data;
    uint32_t *values;

    if (!(values = malloc(bd->num_fields)))
        DIE("Num fields malloc failed");

    for (i = 0; i < bd->num_cookies; i++){
        const char *data = &bd->trails.data[toc[i]];
        uint64_t size = 8 * (toc[i + 1] - toc[i]);
        uint64_t offs = 3;
        uint32_t val;

        size -= read_bits(data, 0, 3);

        while (offs < size){
            int k, j = 0;
            while (offs < size){
                uint64_t prev_offs = offs;
                val = huff_decode_value(codebook, data, &offs);
                if (j == 0 || val & 255)
                    values[j++] = val;
                else{
                    offs = prev_offs;
                    break;
                }
            }
            for (k = 0; k < j; k++){
                int h, tmp;
                J1T(tmp, top_vals, values[k]);
                if (tmp){
                    for (h = k + 1; h < j; h++){
                        J1T(tmp, top_vals, values[h]);
                        if (tmp){
                            Word_t *ptr;
                            // bigram = values[k] | values[h];
                            JLI(ptr, bigram_freqs, bigram);
                            ++*ptr;
                        }
                    }
                }
            }
        }
    }

    /* loop over bigram_freqs */
    /* substract first item from freqs unigram(A) - bigram(AB) */
    /* add bigram(AB) freq to freqs */

    free(values);
}
#endif
