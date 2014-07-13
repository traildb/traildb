
#ifndef __BREADCRUMBS_DECODER__
#define __BREADCRUMBS_DECODER__

#include "breadcrumbs.h"
#include "util.h"

struct breadcrumbs *bd_open(const char *root);
void bd_close(struct breadcrumbs *bd);

uint8_t bd_field_value(uint32_t value);
uint8_t bd_field_index(uint32_t value);

const char *bd_lookup_value(const struct breadcrumbs *bd,
                            uint32_t cookie_index);
const char *bd_lookup_cookie(const struct breadcrumbs *bd,
                             uint32_t cookie_index);

uint32_t bd_trail_decode(struct breadcrumbs *bd,
                         uint32_t trail_index,
                         uint32_t *dst,
                         uint32_t dst_size,
                         int raw_values);

uint32_t bd_trail_value_freqs(const struct breadcrumbs *bd,
                              uint32_t *trail_indices,
                              uint32_t num_trail_indices,
                              uint32_t *dst_values,
                              uint32_t *dst_freqs,
                              uint32_t dst_size);

uint32_t bd_num_cookies(const struct breadcrumbs *bd);
uint32_t bd_num_loglines(const struct breadcrumbs *bd);
uint32_t bd_num_fields(const struct breadcrumbs *bd);
uint32_t bd_min_timestamp(const struct breadcrumbs *bd);
uint32_t bd_max_timestamp(const struct breadcrumbs *bd);

#endif /* __BREADCRUMBS_DECODER__ */

