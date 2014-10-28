#if defined(__linux__)
# include <features.h>
#endif

#include <stdio.h>

#if _POSIX_C_SOURCE < 200809L

FILE *open_memstream(char **ptr, size_t *sizeloc);

#endif /* _POSIX_C_SOURCE < 200809L */
