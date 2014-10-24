/*
    EXTRACT PROTOCOL:

    Version: "extract-1" (9 bytes)
    Chunk-Head: [Chunk Type] (1 byte)
                [Chunk Length N] (4 bytes)
    Chunk-Body: [Chunk Data] (N bytes)

    Supported Chunk Types:

    'F' - Fields:
          [Number of Fields] (4 bytes)
              [Field String (zero terminated)]

    'L' - Lexicons:
          [Number of Lexicons] (4 bytes)
              [Number of Entries] (4 bytes)
                  [Token String (zero terminated)]

    'T' - Trails:
          Until end of chunk:
              [Cookie] (16 bytes)
              [Number of Events N] (4 bytes)
              [Events] (N * 4 bytes)

    'D' - Done

    Chunks are guaranteed to come in this order:
    'F', 'L', 'T'*, 'D'
*/

#include <poll.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>

#include "mix.h"

#define BUF_INC 1000000
#define SEND_BUF_MAX 256000
#define CONNECT_TIMEOUT_SECS 30

struct extract_ctx{
    tdb_field *fields;
    uint32_t num_fields;

    /* trail_buf buffers events of a single trail */
    uint32_t *trail_buf;
    uint32_t trail_buf_len;
    uint32_t trail_buf_size;

    /* send_buf buffers multiple full trails, around SEND_BUF_MAX */
    char *send_buf;
    uint64_t send_buf_len;
    uint64_t send_buf_size;

    int sock;
};

static int connection_attempt(const struct trail_ctx *ctx,
                              const char *host,
                              int port)
{
    struct sockaddr_in addr;
    struct pollfd pfd;
    int sock;
    struct hostent *ent;

    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    if ((ent = gethostbyname(host)))
        memcpy(&addr.sin_addr, ent->h_addr, ent->h_length);
    else{
        MSG(ctx, "Name resolution failed in extract - trying again");
        sleep(1);
        return -1;
    }

    sock = socket(AF_INET, SOCK_STREAM, 0);
    if (fcntl(sock, F_SETFL, O_NONBLOCK) == -1)
        DIE("Could not make extract socket non-blocking");

    connect(sock, (struct sockaddr*)&addr, sizeof(addr));

    memset(&pfd, 0, sizeof(pfd));
    pfd.fd = sock;
    pfd.events = POLLOUT;

    switch (poll(&pfd, 1, 1000)){
        case -1:
            MSG(ctx, "Poll() failed in extract - trying again");
            break;
        case 0:
            MSG(ctx, "Connect() timeout in extract - trying again");
            close(sock);
            return -1;
        case 1:
            if (pfd.revents == POLLOUT){
                int old = fcntl(sock, F_GETFL, 0);
                if (old == -1)
                    DIE("Could not query socket status in extract");
                if (fcntl(sock, F_SETFL, old & ~O_NONBLOCK) == -1)
                    DIE("Could not make extract socket blocking");
                return sock;
            }else
                MSG(ctx,
                    "Connect() to %s:%d failed in extract - trying again",
                    host,
                    port);
            break;
    }
    sleep(1);
    close(sock);
    return -1;
}

static void sendall(int sock, const char *buf, uint32_t size)
{
    uint32_t sent = 0;

    while (sent < size){
        ssize_t ret = send(sock, &buf[sent], size - sent, 0);
        if (ret == -1)
            DIE("Extractd closed connection prematurely: %s", strerror(errno));
        sent += ret;
    }
}

static void send_chunk(int sock, char type, const char *buf, uint32_t size)
{
    char header[5];

    header[0] = type;
    memcpy(&header[1], &size, 4);
    sendall(sock, header, 5);
    sendall(sock, buf, size);
}

static int open_connection(const struct trail_ctx *ctx,
                           const char *host,
                           int port)
{
    int i, sock;
    for (i = 0; i < CONNECT_TIMEOUT_SECS; i++)
        if ((sock = connection_attempt(ctx, host, port)) > 0){
            MSG(ctx, "Extract connected to %s:%d", host, port);
            return sock;
        }
    DIE("Extract could not connect to %s:%d", host, port);
}

static void close_connection(const struct trail_ctx *ctx, int sock)
{
    static const char DONE[] = "**DONE**";

    send_chunk(sock, 'D', DONE, sizeof(DONE));
    close(sock);
    MSG(ctx, "Connection to extractd closed");
}

static void write_fields(FILE *memio,
                         tdb *db,
                         const struct extract_ctx *ectx)
{
    uint32_t i;

    SAFE_WRITE(&ectx->num_fields, 4, "memory", memio);
    for (i = 0; i < ectx->num_fields; i++){
        const char *name = db->field_names[ectx->fields[i]];
        int len = strlen(name);
        SAFE_WRITE(name, len + 1, "memory", memio);
    }
}

static void write_lexicon(tdb_field field,
                          FILE *memio,
                          tdb *db,
                          const struct extract_ctx *ectx)
{
    uint32_t i, lexsize;

    if (tdb_lexicon_size(db, field, &lexsize))
        DIE("Could not get lexicon size for field %u", field);
    ++lexsize;

    SAFE_WRITE(&lexsize, 4, "memory", memio);

    /* index=0 is always an empty string */
    i = 0;
    SAFE_WRITE(&i, 1, "memory", memio);

    for (i = 1; i < lexsize; i++){
        const char *value = tdb_get_value(db, field, i);
        int len = strlen(value);
        SAFE_WRITE(value, len + 1, "memory", memio);
    }
}

static void send_header(const struct extract_ctx *ectx,
                        const struct trail_ctx *ctx)
{
    static const char VERSION[] = "extract-1";
    FILE *memio = NULL;
    char *buf = NULL;
    size_t buf_size;
    uint32_t i;
    uint64_t size;

    if (!(memio = open_memstream(&buf, &buf_size)))
         DIE("Could not initialize memstream in extract");

    sendall(ectx->sock, VERSION, sizeof(VERSION) - 1);

    write_fields(memio, ctx->db, ectx);

    SAFE_TELL(memio, size, "memory");
    SAFE_FLUSH(memio, "memory");
    send_chunk(ectx->sock, 'F', buf, size);

    rewind(memio);
    SAFE_WRITE(&ectx->num_fields, 4, "memory", memio);
    for (i = 0; i < ectx->num_fields; i++)
        write_lexicon(ectx->fields[i], memio, ctx->db, ectx);

    SAFE_TELL(memio, size, "memory");
    SAFE_FLUSH(memio, "memory");
    send_chunk(ectx->sock, 'L', buf, size);

    fclose(memio);
    free(buf);
}

static void add_event(struct extract_ctx *ectx,
                      const tdb_item *trail,
                      uint32_t trail_size)
{
    uint32_t i;

    if (ectx->trail_buf_len + ectx->num_fields + 1 >= ectx->trail_buf_size){
        ectx->trail_buf_size += ectx->num_fields + 1 + BUF_INC;
        if (!(ectx->trail_buf = realloc(ectx->trail_buf,
                                        ectx->trail_buf_size * 4)))
            DIE("Realloc failed in extract for %u items",
                 ectx->trail_buf_size);
    }
    /* add timestamp */
    ectx->trail_buf[ectx->trail_buf_len++] = trail[0];
    for (i = 0; i < ectx->num_fields; i++)
        /* add just the vals, not the fields */
        ectx->trail_buf[ectx->trail_buf_len++] =
            tdb_item_val(trail[ectx->fields[i] + 1]);
}

static void flush_send_buf(struct extract_ctx *ectx)
{
    send_chunk(ectx->sock, 'T', ectx->send_buf, ectx->send_buf_len);
    ectx->send_buf_len = 0;
}

static void flush_trail_buf(struct extract_ctx *ectx, const uint8_t *cookie)
{
    uint32_t size = ectx->trail_buf_len * 4 + 4 + 16;
    char *p;

    if (size + ectx->send_buf_len >= ectx->send_buf_size){
        ectx->send_buf_size = ectx->send_buf_len + size + BUF_INC;
        if (!(ectx->send_buf = realloc(ectx->send_buf, ectx->send_buf_size)))
            DIE("Realloc failed in extract for %llu bytes",
                (long long unsigned int)ectx->send_buf_size);
    }

    p = &ectx->send_buf[ectx->send_buf_len];
    ectx->send_buf_len += size;

    memcpy(p, cookie, 16);
    p += 16;
    memcpy(p, &ectx->trail_buf_len, 4);
    p += 4;
    memcpy(p, ectx->trail_buf, ectx->trail_buf_len * 4);

    if (ectx->send_buf_len > SEND_BUF_MAX)
        flush_send_buf(ectx);
}

static void init_ectx(char *arg,
                      struct extract_ctx *ectx,
                      const struct trail_ctx *ctx)
{
    char *tok = strsep(&arg, ",");
    char *addr;
    tdb_field field;
    int port;

    if (memcmp(tok, "tcp://", 6))
        DIE("Only tcp:// protocol is supported currently in extract");

    tok = &tok[6];
    addr = strsep(&tok, ":");
    if (!tok)
        DIE("Address syntax is tcp://host:port in extract");
    port = parse_uint64(tok, "extractd port");

    ectx->sock = open_connection(ctx, addr, port);
    ectx->num_fields = 0;

    while (arg){
        tok = strsep(&arg, ",");
        field = tdb_get_field(ctx->db, tok);

        if (field < 0)
            DIE("Unknown field in extract: %s", tok);

        ectx->fields[ectx->num_fields++] = field;
    }
}

void op_help_extract()
{
    INFO("help extract");
}

void *op_init_extract(struct trail_ctx *ctx,
                      const char *arg,
                      int op_index,
                      int num_ops,
                      uint64_t *flags)
{
    /* extract=tcp://localhost:5000,advertisable_eid,campaign_eid */
    char *marg;
    struct extract_ctx *ectx;

    if (!arg)
        DIE("extract requires an argument (see --help=extract)");

    if (!ctx->db)
        DIE("extract requires a DB");

    if (!ctx->opt_match_events)
        DIE("extract requires --match-events");

    if (!(ectx = calloc(1, sizeof(struct extract_ctx))))
        DIE("Malloc failed in op_init_extract");

    if (!(ectx->fields = malloc(tdb_num_fields(ctx->db) * 4)))
        DIE("Malloc failed in op_init_extract");

    if (!(marg = strdup(arg)))
        DIE("Malloc failed in op_init_extract");

    init_ectx(marg, ectx, ctx);
    send_header(ectx, ctx);

    *flags |= TRAIL_OP_PRE_TRAIL |
              TRAIL_OP_EVENT |
              TRAIL_OP_POST_TRAIL |
              TRAIL_OP_FINALIZE;

    free(marg);
    return ectx;
}

int op_exec_extract(struct trail_ctx *ctx,
                    int mode,
                    uint64_t cookie_id,
                    const tdb_item *trail,
                    uint32_t trail_size,
                    void *arg)
{
    struct extract_ctx *ectx = (struct extract_ctx*)arg;

    switch (mode){
        case TRAIL_OP_PRE_TRAIL:
            ectx->trail_buf_len = 0;
            break;

        case TRAIL_OP_EVENT:
            add_event(ectx, trail, trail_size);
            break;

        case TRAIL_OP_POST_TRAIL:
            if (ectx->trail_buf_len > 0){
                const uint8_t *cookie = tdb_get_cookie(ctx->db, cookie_id);
                flush_trail_buf(ectx, cookie);
            }
            break;

        case TRAIL_OP_FINALIZE:
            if (ectx->send_buf_len > 0)
                flush_send_buf(ectx);
            close_connection(ctx, ectx->sock);
            break;
    }

    return 0;
}
