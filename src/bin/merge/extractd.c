
#include <poll.h>
#include <errno.h>
#include <sys/socket.h>
#include <sys/types.h>
#include <netinet/in.h>
#include <arpa/inet.h>
#include <unistd.h>
#include <fcntl.h>
#include <netdb.h>
#include <stdint.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>

#include <Judy.h>

#include "extractd.h"
#include "util.h"

#define TOKEN_BUF_INC (1 << 26) /* 64 MB */
#define BUFFER_INC (1 << 20) /* 1 MB */

struct ext_mapper{
    /* local to global token index mapping */
    const uint32_t **field_mappings;
    /* number of tokens in the above */
    uint32_t *num_tokens;
    /* mapper socket */
    int sock;
    /* is this mapper done? */
    int done;

    /* receive buffer for this mapper */
    char *chunk_buf;
    uint32_t chunk_buf_len;
    uint32_t chunk_buf_size;
};

/* fields - same for all mappers */
struct ext_field{
    /* field name */
    const char *name;

    /* lexicon for this field */
    /* string array */
    char *tokens;
    /* number of tokens in the above */
    uint32_t num_tokens;
    /* array length */
    uint64_t tokens_len;
    /* array size */
    uint64_t tokens_size;

    /* index -> offset for the array above */
    uint64_t *token_offsets;
    /* offset array size */
    uint32_t offsets_size;

    /* token -> index mapping for the array above */
    Pvoid_t token_to_index;
};

struct extractd{

    /* all mappers */
    struct ext_mapper *mappers;
    uint32_t num_mappers;

    /* polling support */
    struct pollfd *fds;
    struct ext_mapper **fd_mappers;
    int server_sock;

    /* fields */
    struct ext_field *fields;
    uint32_t num_fields;

    /* currently active trail chunk */
    struct ext_mapper *active_chunk;
    uint32_t active_chunk_len;
    uint32_t active_chunk_offs;
};

static void recvall(int sock, char *buf, uint32_t size)
{
    uint32_t read = 0;

    while (read < size){
        ssize_t ret = recv(sock, &buf[read], size - read, 0);
        if (ret == -1)
            DIE("Mapper closed connection prematurely in recv\n");
        read += ret;
    }
}

/*
Bind a new server socket.
*/
static int new_server_socket(int port)
{
    struct sockaddr_in addr;
    int sock;
    int one = 1;

    if ((sock = socket(AF_INET, SOCK_STREAM, 0)) == -1)
        DIE("Opening a server socket failed");

    memset(&addr, 0, sizeof(struct sockaddr_in));
    addr.sin_addr.s_addr = INADDR_ANY;
    addr.sin_family = AF_INET;
    addr.sin_port = htons(port);

    setsockopt(sock, SOL_SOCKET, SO_REUSEADDR, &one, sizeof(one));

    if (bind(sock, (struct sockaddr*)&addr, sizeof(struct sockaddr_in)) == -1)
        DIE("Binding socket failed\n");

    if (listen(sock, 10) == -1)
        DIE("Listen failed\n");

    return sock;
}

/*
Merge the mapper-local lexicon to the global lexicon
and produce a local->global field mapping.
*/
static uint32_t *create_mapping(struct ext_field *field,
                                char *buf,
                                uint32_t buf_max,
                                uint32_t *size,
                                uint32_t *num_tokens)
{
    uint32_t i, offs = 4;
    uint32_t *local_map;

    if (buf_max < 4)
        DIE("Truncated lexicon\n");

    *num_tokens = *(uint32_t*)buf;
    if (!(local_map = malloc(*num_tokens * 4)))
        DIE("Could not malloc local map for %u tokens", *num_tokens);

    for (i = 0; i < *num_tokens && offs < buf_max; i++){
        Word_t *ptr;
        uint32_t len = strnlen(&buf[offs], buf_max - offs);

        /* lookup token */
        JHSI(ptr, field->token_to_index, &buf[offs], len);
        if (!*ptr){
            /* new token */
            *ptr = ++field->num_tokens;
            if (field->tokens_len + len + 1 > field->tokens_size){
                field->tokens_size += len + 1 + TOKEN_BUF_INC;
                if (!(field->tokens = realloc(field->tokens,
                                              field->tokens_size)))
                    DIE("Could not allocate tokens array of %llu bytes\n",
                        (unsigned long long)field->tokens_size);
            }
            memcpy(&field->tokens[field->tokens_len], &buf[offs], len + 1);

            if (field->num_tokens > field->offsets_size){
                field->offsets_size += BUFFER_INC;
                if (!(field->token_offsets = realloc(field->token_offsets,
                                                     field->offsets_size * 4)))
                    DIE("Could not allocate offsets of %u bytes\n",
                        field->offsets_size * 4);
            }

            field->token_offsets[field->num_tokens - 1] = field->tokens_len;
            field->tokens_len += len + 1;
        }
        /* map this local token to the corresponding global token */
        local_map[i] = *ptr - 1;
        offs += len + 1;
    }

    if (i != *num_tokens)
        DIE("Corrupted lexicon chunk: Too few tokens\n");

    *size = offs;
    return local_map;
}

/*
Parse lexicon chunk.

This function extracts the lexicon blob for every field and creates
the corresponding local -> global mapping.
*/
static void parse_lexicon(struct extractd *ext, struct ext_mapper *mapper)
{
    uint32_t i;
    uint32_t offs = 0;
    const char *p = mapper->chunk_buf;
    uint32_t num_lexicons = *(uint32_t*)p;
    offs += 4;

    if (!ext->fields)
        DIE("Error: Mapper sent lexicon before fields\n");

    if (num_lexicons != ext->num_fields)
        DIE("Incompatible mappers: Got %u lexicons, expected %u\n",
            num_lexicons,
            ext->num_fields);

    if (mapper->field_mappings)
        DIE("Error: Mapper sent lexicon twice\n");

    if (!(mapper->field_mappings = calloc(num_lexicons, sizeof(uint32_t*))))
        DIE("Could not malloc mapping for %u lexicons\n", num_lexicons);

    if (!(mapper->num_tokens = calloc(num_lexicons, 4)))
        DIE("Could not malloc lengths for %u lexicons\n", num_lexicons);

    for (i = 0; i < num_lexicons && offs < mapper->chunk_buf_len; i++){
        uint32_t size;
        mapper->field_mappings[i] = create_mapping(&ext->fields[i],
                                                   &mapper->chunk_buf[offs],
                                                   mapper->chunk_buf_len - offs,
                                                   &size,
                                                   &mapper->num_tokens[i]);
        offs += size;
    }

    if (offs < mapper->chunk_buf_len)
        DIE("Corrupted lexicon chunk: Extra bytes\n");
}

/*
Parse field chunk.

This funcion has two modes of operation: If this is globally the first time
we receive fields, we initialize all fields-related data structures with the
received information.

If existing fields information exists, this function makes sure that the
received data matches exactly to what have received before.
*/
static void parse_fields(struct extractd *ext, struct ext_mapper *mapper)
{
    uint32_t i;
    uint32_t offs = 0;
    const char *p = mapper->chunk_buf;
    uint32_t num_fields = *(uint32_t*)p;
    offs += 4;

    if (ext->fields){
        /* check against existing */
        if (ext->num_fields != num_fields)
            DIE("Incompatible mappers: Got %u fields, expected %u fields\n",
                num_fields, ext->num_fields);

        for (i = 0; i < ext->num_fields && offs < mapper->chunk_buf_len; i++){
            uint32_t max = mapper->chunk_buf_len - offs;

            if (strncmp(ext->fields[i].name, &p[offs], max)){
                uint32_t len = strnlen(&p[offs], max);
                DIE("Incompatible mappers: "
                    "%uth field should be '%s', got '%*s'\n",
                    i,
                    ext->fields[i].name,
                    len,
                    &p[offs]);
            }
            offs += strlen(ext->fields[i].name) + 1;
        }

    }else{
        /* initialize */
        ext->num_fields = num_fields;
        /* +1 guarantees that ext->fields != NULL */
        if (!(ext->fields = calloc(ext->num_fields + 1,
                                   sizeof(struct ext_field))))
            DIE("Could not malloc %u field structures\n", ext->num_fields);

        for (i = 0; i < ext->num_fields && offs < mapper->chunk_buf_len; i++){
            ext->fields[i].name = strndup(&p[offs],
                                          mapper->chunk_buf_len - offs);
            if (!ext->fields[i].name)
                DIE("Could not malloc field name\n");
            offs += strlen(ext->fields[i].name) + 1;
        }
    }

    if (i != ext->num_fields)
        DIE("Corrupted fields chunk: Too few fields\n");

    if (offs < mapper->chunk_buf_len)
        DIE("Corrupted fields chunk: Extra bytes\n");
}

/*
Receives and process an incoming chunk from a mapper.
*/
static int receive_chunk(struct extractd *ext, struct ext_mapper *mapper)
{
    struct head{
        char type;
        uint32_t len;
    } __attribute__((packed)) header;

    recvall(mapper->sock, (char*)&header, sizeof(struct head));

    if (header.len < 4)
        DIE("Truncated chunk (type '%c'): %u bytes\n", header.type, header.len);

    if (header.len > mapper->chunk_buf_size){
        free(mapper->chunk_buf);
        mapper->chunk_buf_size = header.len + BUFFER_INC;
        if (!(mapper->chunk_buf = malloc(mapper->chunk_buf_size)))
            DIE("Could not allocate memory for chunk of %u bytes\n",
                mapper->chunk_buf_size);
    }
    mapper->chunk_buf_len = header.len;
    recvall(mapper->sock, mapper->chunk_buf, header.len);

    switch (header.type){
        case 'F':
            parse_fields(ext, mapper);
            break;

        case 'L':
            parse_lexicon(ext, mapper);
            break;

        case 'D':
            mapper->done = 1;
            break;

        case 'T':
            return 1;

        default:
            DIE("Unknown chunk type: '%c' (%d)\n", header.type, header.type);
    }

    /* Only trail buffers are of interest to entities outside this function.
       Hide existence of other things. */
    mapper->chunk_buf_len = 0;
    return 0;
}

/*
Accept a new connection from a mapper and check its version.
*/
static void new_mapper(struct extractd *ext)
{
    static const char VERSION[] = "extract-1";
    struct sockaddr_in peer_addr;
    socklen_t peer_addr_size = sizeof(struct sockaddr_in);
    uint32_t i;
    char head[sizeof(VERSION) - 1];

    for (i = 0; i < ext->num_mappers; i++)
        if (!ext->mappers[i].sock)
            break;

    if (i == ext->num_mappers)
        DIE("Too many mappers! Expected only %u connections\n",
            ext->num_mappers);

    if ((ext->mappers[i].sock = accept(ext->server_sock,
                                       (struct sockaddr*)&peer_addr,
                                       &peer_addr_size)) == -1)
        DIE("Accept() failed\n");

    recvall(ext->mappers[i].sock, head, sizeof(VERSION) - 1);
    if (memcmp(VERSION, head, sizeof(VERSION) - 1))
        DIE("Version mismatch: Expected '%s', got '%*s'\n",
            VERSION,
            (int)sizeof(VERSION) - 1,
            head);
}

/*
Accept new mappers and receive incoming chunks.
All other chunks except trails are processed right away.
*/
static int poll_mappers(struct extractd *ext, uint32_t timeout)
{
    uint32_t i;
    uint32_t num_done = 0;
    uint32_t num_chunks = 0;
    uint32_t num_fds = 1;

    ext->fds[0].fd = ext->server_sock;
    ext->fds[0].events = POLLIN;

    for (i = 0; i < ext->num_mappers; i++){
        if (ext->mappers[i].done)
            ++num_done;
        else if (ext->mappers[i].sock){
            ext->fds[num_fds].fd = ext->mappers[i].sock;
            ext->fds[num_fds].events = POLLIN;
            ext->fd_mappers[num_fds] = &ext->mappers[i];
            ++num_fds;
        }
    }

    if (num_done == ext->num_mappers)
        return -1;

    switch (poll(ext->fds, num_fds, timeout)){

        /* poll failed */
        case -1:
            DIE("Poll failed\n");

        /* timeout */
        case 0:
            return -2;

        /* incoming data */
        default:

            if (ext->fds[0].revents & (POLLERR | POLLHUP | POLLNVAL))
                DIE("Server socket failed unexpectedly\n");
            else if (ext->fds[0].revents & POLLIN)
                new_mapper(ext);

            for (i = 1; i < num_fds; i++){
                if (ext->fds[i].revents & (POLLERR | POLLHUP | POLLNVAL))
                    DIE("Mapper closed connection unexpectedly\n");
                else if (ext->fds[i].revents & POLLIN)
                    num_chunks += receive_chunk(ext, ext->fd_mappers[i]);
            }
    }
    return num_chunks;
}

/*
Pick the next unconsumed trail chunk. If all chunks have been consumed,
call poll_mappers() to receive a new set of chunks.
*/
static int next_chunk(struct extractd *ext, uint32_t timeout)
{
    uint32_t i;

    for (i = 0; i < ext->num_mappers; i++)
        if (ext->mappers[i].chunk_buf_len){
            ext->active_chunk = &ext->mappers[i];
            ext->active_chunk_len = ext->mappers[i].chunk_buf_len;
            ext->active_chunk_offs = 0;
            ext->mappers[i].chunk_buf_len = 0;
            return 1;
        }

    /* all chunk buffers consumed, fetch the next set */
    while (1){
        switch (poll_mappers(ext, timeout)){
            case 0:
                /* we received data but no new trails yet, try again */
                break;
            case -1:
                /* all mappers done, return successfully */
                return 0;
            case -2:
                /* timeout */
                return -1;
            default:
                /* new trails received, return next_chunk as usual */
                return next_chunk(ext, timeout);
        }
    }
}

/*
Parse the next trail from the active chunk. Replace mapper-local token
IDs with global IDs.
*/
static void parse_next_trail(struct extractd *ext,
                             tdb_cookie *cookie,
                             const uint32_t **events,
                             uint32_t *num_events)
{
    /* trail:
       [ cookie: 16 bytes] [ num values 4 bytes ] [ event: K fields ]+
    */
    uint32_t i, j, num_values;
    uint32_t *values;
    const uint32_t **local_to_global = ext->active_chunk->field_mappings;
    const uint32_t *num_tokens = ext->active_chunk->num_tokens;
    char *p = &ext->active_chunk->chunk_buf[ext->active_chunk_offs];

    ext->active_chunk_offs += 20;
    if (ext->active_chunk_offs > ext->active_chunk_len)
        DIE("Truncated trail: Missing header\n");
    else{
        *cookie = (tdb_cookie)p;
        num_values = *(uint32_t*)&p[16];
    }

    /* each event has num_fields fields and a timestamp */
    if (num_values % (ext->num_fields + 1))
        DIE("Invalid trail: %u values is not divisible with %u fields\n",
            num_values, ext->num_fields + 1);
    else
        *num_events = num_values / (ext->num_fields + 1);

    ext->active_chunk_offs += num_values * 4;
    if (ext->active_chunk_offs > ext->active_chunk_len)
        DIE("Truncated trail (expected %u bytes, got only %u)\n",
            ext->active_chunk_offs,
            ext->active_chunk_len);
    else
        values = (uint32_t*)&p[20];

    /* map local token indices to global token indices, in-place, using the
       mapper's mapping */
    if (ext->num_fields)
        for (i = 0; i < num_values;){
            ++i; /* leave timestamp as-is */
            for (j = 0; j < ext->num_fields; j++)
                if (values[i] < num_tokens[j]){
                    values[i] = local_to_global[j][values[i]];
                    ++i;
                }else
                    DIE("Invalid token: %u (field '%s', max index %u)",
                        values[i],
                        ext->fields[j].name,
                        num_tokens[j]);
        }
    *events = values;
}

/*
Get the next trail from the active chunk. If no
more trails are left in the chunk, receive a new chunk from next_chunk().
*/
int extractd_next_trail(struct extractd *ext,
                        tdb_cookie *cookie,
                        const uint32_t **events,
                        uint32_t *num_events,
                        uint32_t *num_fields,
                        uint32_t timeout_ms)
{
    if (ext->active_chunk_offs < ext->active_chunk_len){
        parse_next_trail(ext, cookie, events, num_events);
        *num_fields = ext->num_fields;
        return 1;
    }else{
        int ret;
        if ((ret = next_chunk(ext, timeout_ms)) < 1)
            return ret;
        else
            return extractd_next_trail(ext,
                                       cookie,
                                       events,
                                       num_events,
                                       num_fields,
                                       timeout_ms);
    }
}

/*
Initialize extractd.
*/
struct extractd *extractd_init(uint32_t num_mappers, int port)
{
    struct extractd *ext;

    if (!(ext = calloc(1, sizeof(struct extractd))))
        DIE("Malloc failed in extractd_init\n");

    ext->num_mappers = num_mappers;

    if (!(ext->mappers = calloc(num_mappers, sizeof(struct ext_mapper))))
        DIE("Malloc failed in extractd_init (%u mappers)\n", num_mappers);

    if (!(ext->fds = calloc(num_mappers + 1, sizeof(struct pollfd))))
        DIE("Malloc failed in extractd_init (%u mappers)\n", num_mappers);

    if (!(ext->fd_mappers = calloc(num_mappers + 1, sizeof(void*))))
        DIE("Malloc failed in extractd_init (%u mappers)\n", num_mappers);

    ext->server_sock = new_server_socket(port);

    return ext;
}

/*
Returns the number of fields.

Note: Returns 0 before the first call to extractd_next_trail().
*/
uint32_t extractd_get_num_fields(const struct extractd *ext)
{
    return ext->num_fields;
}

/*
Returns the field name for the given field.

Note: Returns NULL before the first call to extractd_next_trail().
*/
const char *extractd_get_field_name(const struct extractd *ext, tdb_field field)
{
    if (field < ext->num_fields)
        return ext->fields[field].name;
    else
        return NULL;
}

/*
Returns the total number of tokens for the given field.

Note: It doesn't make much sense to call this function before all data has
been consumed, i.e. extractd_next_trail() returns 0.
*/
uint32_t extractd_num_tokens(const struct extractd *ext, tdb_field field)
{
    if (field < ext->num_fields)
        return ext->fields[field].num_tokens;
    else
        return 0;
}

/*
Returns the token (string) for the given field and index.

Note: Returns NULL before the first call to extractd_next_trail().
*/
const char *extractd_get_token(const struct extractd *ext,
                               tdb_field field,
                               uint32_t index)
{
    if (field < ext->num_fields && index < ext->fields[field].num_tokens){
        const struct ext_field *f = &ext->fields[field];
        return &f->tokens[f->token_offsets[index]];
    }else
        return NULL;
}

/*
Returns the token index for the given field and token or -1 if the token
is not found.

Note: Returns -1 before the first call to extractd_next_trail().
*/
int64_t extractd_get_index(const struct extractd *ext,
                           tdb_field field,
                           char *token)
{
    if (field < ext->num_fields){
        Word_t *ptr;
        JHSG(ptr, ext->fields[field].token_to_index, token, strlen(token));
        if (ptr)
            return *ptr - 1;
    }
    return -1;
}

