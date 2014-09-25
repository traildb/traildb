
#include "breadcrumbs_decoder.h"

int open_lexicon(const struct breadcrumbs *bd,
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
    uint32_t field = (value & 255) - 1;
    uint32_t index = value >> 8;
    return bd_lookup_value2(bd, field, index);
}

const char *bd_lookup_value2(const struct breadcrumbs *bd,
                            uint32_t field,
                            uint32_t index)
{
    struct lexicon lex;
    if (index && !open_lexicon(bd, &lex, field) && index - 1 < lex.size)
        return &lex.data[lex.toc[index - 1]];
    else
        return NULL;
}

int bd_lexicon_size(const struct breadcrumbs *bd,
                    uint32_t field,
                    uint32_t *size)
{
    struct lexicon lex;

    if (open_lexicon(bd, &lex, field)){
        *size = 0;
        return -1;
    }else{
        *size = lex.size;
        return 0;
    }
}

uint32_t bd_lookup_token(const struct breadcrumbs *bd,
                         const char *token,
                         uint32_t field)
{
    struct lexicon lex;

    if (!open_lexicon(bd, &lex, field)){
        uint32_t i;
        if (*token){
            for (i = 0; i < lex.size; i++)
                if (!strcmp(&lex.data[lex.toc[i]], token))
                    return (field + 1) | ((i + 1) << 8);
        }else
            /* valid empty value */
            return field + 1;
    }
    return 0;
}

const char *bd_lookup_cookie(const struct breadcrumbs *bd,
                             uint32_t cookie_index)
{
    if (cookie_index < bd->num_cookies){
        return &bd->cookies.data[cookie_index * 16];
    }else
        return NULL;
}

