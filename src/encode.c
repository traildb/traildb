
#include <unistd.h>
#include <stdint.h>

#include "encode.h"
#include "util.h"

void store_cookies(const Pvoid_t cookie_index,
                   uint64_t num_cookies,
                   const char *path)
{
    Word_t cookie_bytes[2];
    FILE *out;
    Word_t *ptr;

    if (!(out = fopen(path, "w")))
        DIE("Could not create cookie file: %s\n", path);

    if (ftruncate(fileno(out), num_cookies * 16LLU))
        DIE("Could not initialize lexicon file (%llu bytes): %s\n",
            (long long unsigned int)(num_cookies * 16),
            path);

    cookie_bytes[0] = 0;
    JLF(ptr, cookie_index, cookie_bytes[0]);
    while (ptr){
        const Pvoid_t cookie_index_lo = (const Pvoid_t)*ptr;
        cookie_bytes[1] = 0;
        JLF(ptr, cookie_index_lo, cookie_bytes[1]);
        while (ptr){
            SAFE_WRITE(cookie_bytes, 16, path, out);
            JLN(ptr, cookie_index_lo, cookie_bytes[1]);
        }
        JLN(ptr, cookie_index, cookie_bytes[0]);
    }
    SAFE_CLOSE(out, path);
}

static uint64_t lexicon_size(const Pvoid_t lexicon, uint64_t *size){
    uint8_t token[MAX_FIELD_SIZE];
    Word_t *ptr;
    uint64_t count = 0;

    token[0] = 0;
    JSLF(ptr, lexicon, token);
    while (ptr){
        *size += strlen((char*)token);
        ++count;
        JSLN(ptr, lexicon, token);
    }
    return count;
}

void store_lexicon(const Pvoid_t lexicon, const char *path)
{
    uint8_t token[MAX_FIELD_SIZE];
    uint64_t size = 0;
    uint64_t count = lexicon_size(lexicon, &size);
    uint64_t offset;
    FILE *out;
    Word_t *token_id;

    size += (count + 1) * 4;

    if (count > UINT32_MAX || size > UINT32_MAX)
        DIE("Lexicon %s would be huge! %llu items, %llu bytes\n",
            path,
            (long long unsigned int)count,
            (long long unsigned int)size);

    if (!(out = fopen(path, "w")))
        DIE("Could not create lexicon file: %s\n", path);

    if (ftruncate(fileno(out), size))
        DIE("Could not initialize lexicon file (%llu bytes): %s\n",
            (long long unsigned int)size,
            path);

    SAFE_WRITE(&count, 4, path, out);

    token[0] = 0;
    offset = (count + 1) * 4;
    JSLF(token_id, lexicon, token);
    while (token_id){
        uint32_t len = strlen((char*)token);

        /* note: token IDs start 1, otherwise we would need to +1 */
        SAFE_SEEK(out, *token_id * 4, path);
        SAFE_WRITE(&offset, 4, path, out);

        SAFE_SEEK(out, offset, path);
        SAFE_WRITE(token, len + 1, path, out);

        offset += len + 1;
        JSLN(token_id, lexicon, token);
    }
    SAFE_CLOSE(out, path);
}
