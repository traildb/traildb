
#ifndef __LEXICON_ENCODE_H__
#define __LEXICON_ENCODE_H__

void store_cookies(const Pvoid_t cookie_index,
                   uint64_t num_cookies,
                   const char *path);

void store_lexicon(Pvoid_t lexicon, const char *path);

#endif /* __LEXICON_ENCODE_H__ */
