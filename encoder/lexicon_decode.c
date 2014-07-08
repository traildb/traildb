
#include "breadcrumbs_decoder.h"

uint8_t bd_field_index(uint32_t value)
{
    return value & 255;
}

uint8_t bd_field_value(uint32_t value)
{
    return value >> 8;
}

const char *bd_lookup_value(const struct breadcrumbs *bd,
                            uint32_t value)
{
    return NULL;
}

const char *bd_lookup_cookie(const struct breadcrumbs *bd,
                             uint32_t cookie_index)
{
    if (cookie_index < bd->num_cookies){
        return &bd->cookies.data[cookie_index * 16];
    }else
        return NULL;
}

