
#ifndef __EXTRACTD_H__
#define __EXTRACTD_H__

#include <stdint.h>

#include "traildb.h"

struct extractd;

struct extractd *extractd_init(uint32_t num_mappers, int port);

int extractd_next_trail(struct extractd *ext,
                        tdb_cookie *cookie,
                        const uint32_t **events,
                        uint32_t *num_events,
                        uint32_t *num_fields,
                        uint32_t timeout_ms);

uint32_t extractd_get_num_fields(const struct extractd *ext);

const char *extractd_get_field_name(const struct extractd *ext, tdb_field field);

uint32_t extractd_num_tokens(const struct extractd *ext, tdb_field field);

const char *extractd_get_token(const struct extractd *ext,
                               tdb_field field,
                               uint32_t index);

int64_t extractd_get_index(const struct extractd *ext,
                           tdb_field field,
                           char *token);

#endif /* __EXTRACTD_H__ */
