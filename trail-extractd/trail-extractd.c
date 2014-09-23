
#include <stdint.h>

#include "extractd.h"

#include <hex_encode.h>
#include <util.h>

#define DEFAULT_PORT 7676
#define MAPPER_TIMEOUT 2 * 60 * 1000

int main(int argc, char **argv)
{
    uint64_t num_mappers = parse_uint64(argv[1], "number of mappers");
    struct extractd *ext = extractd_init(num_mappers, DEFAULT_PORT);
    const char *cookie;
    const uint32_t *events;
    uint32_t i, j, k, num_fields, num_events, num_cookies = 0;
    int ret;
    static char hex_cookie[33];

    while ((ret = extractd_next_trail(ext,
                                      &cookie,
                                      &events,
                                      &num_events,
                                      &num_fields,
                                      MAPPER_TIMEOUT))){
        if (ret == -1)
            DIE("Timeout");

        hex_encode((const uint8_t*)cookie, hex_cookie);
        printf("%s", hex_cookie);
        for (i = 0, k = 0; i < num_events; i++){
            printf(" %d", events[k++]);
            for (j = 0; j < num_fields; j++)
                printf(" %s", extractd_get_token(ext, j, events[k++]));
        }
        printf("\n");

    }

    printf("NUM %u\n", num_cookies);

    return 0;
}
