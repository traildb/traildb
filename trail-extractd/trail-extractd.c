
#include <stdint.h>

#include "extractd.h"

#include <util.h>

#define DEFAULT_PORT 7676
#define MAPPER_TIMEOUT 2 * 60 * 1000

int main(int argc, char **argv)
{
    uint64_t num_mappers = parse_uint64(argv[1], "number of mappers");
    struct extractd *ext = extractd_init(num_mappers, DEFAULT_PORT);
    const char *cookie;
    const uint32_t *events;
    uint32_t num_events;
    int ret;

    while ((ret = extractd_next_trail(ext,
                                      &cookie,
                                      &events,
                                      &num_events,
                                      MAPPER_TIMEOUT))){
        if (ret == -1)
            DIE("Timeout");

    }

    return 0;
}
