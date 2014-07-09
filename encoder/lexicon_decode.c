
#include "breadcrumbs_decoder.h"

struct lexicon{
    uint32_t size;
    const uint32_t *toc;
    const char *data;
};

static int open_lexicon(const struct breadcrumbs *bd,
                        struct lexicon *lex,
                        uint32_t field)
{
    if (field < bd->num_fields){
        lex->size = *(uint32_t*)bd->lexicons[field].data;
        lex->toc = (const uint32_t*)&bd->lexicons[field].data[4];
        lex->data = (const char*)bd->lexicons[field].data;
        return 0;
    }else
        return -1;
}

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
    struct lexicon lex;
    uint32_t field = (value & 255) - 1;
    uint32_t index = value >> 8;

    if (index && !open_lexicon(bd, &lex, field) && index - 1 < lex.size)
        return &lex.data[lex.toc[index - 1]];
    else
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

