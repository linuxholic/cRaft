#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "net.h"

struct Server
{
    char *host;
    int port;
    int id;
};

// TODO: we need a common raft.h header file, right?
enum raft_rpc_type
{
    REQUEST_VOTE = 1
    , APPEND_ENTRIES
    , ADD_SERVER
    , REMOVE_SERVER
};

void addServer(net_connect_t *c, void *arg)
{
    if (c->err)
    {
        logerr("addServer: connect error.\n");
        return;
    }

    net_buf_t *reply = net_buf_create(0);

    // encode raft rpc type
    uint32_t rpc_type = htonl(ADD_SERVER);
    net_buf_copy(reply, (char*)&rpc_type, sizeof(uint32_t));

    struct Server *srv = arg;

    // TODO: better to use fixed-length ip string, ie
    //       standard IP format, say, 127.000.000.001
    /*
     * ip_len | ip_str | port | id
     */

    // encode server ip
    int ip_len = strlen(srv->host);
    int _ip_len = htonl(ip_len);
    net_buf_copy(reply, (char*)&_ip_len, sizeof(uint32_t));
    net_buf_copy(reply, srv->host, ip_len);

    // encode server port
    int port = htonl(srv->port);
    net_buf_copy(reply, (char*)&port, sizeof(uint32_t));

    // encode server id
    int id = htonl(srv->id);
    net_buf_copy(reply, (char*)&id, sizeof(uint32_t));

    list_append(&c->outbuf, &reply->node);
    net_connection_send(c);
}

void removeServer(net_connect_t *c, void *arg)
{
    if (c->err)
    {
        logerr("removeServer: connect error.\n");
        return;
    }

    /*
     * configuration entry format:
     * addr_num | ip_len | ip_str | port | id
     */
    net_buf_t *buf = net_buf_create(0);

    list_append(&c->outbuf, &buf->node);
    net_connection_send(c);
}

int process_res(char *start, size_t size, net_connect_t *c)
{
    printf("%.*s\n", (int)size, start);
    net_connection_set_close(c);
    net_loop_stop(c->loop);
    return 0;
}

int main(int argc, char *argv[])
{
    if (argc != 6)
    {
        fprintf(stderr, "usage: %s leader_id add|remove "
                "host port id\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    int   leader_id   =  strtol(argv[1], NULL, 10);
    char *change_type =         argv[2];
    char *host        =         argv[3];
    int   port        =  strtol(argv[4], NULL, 10);
    int   id          =  strtol(argv[5], NULL, 10);

    struct Server srv = {host, port, id};

    net_log_level(LOG_INFO);
    net_loop_t *loop = net_loop_init(1000);
    if (!loop)
    {
        logerr("init loop failed.\n");
        exit(EXIT_FAILURE);
    }

    net_client_t *client = net_client_init(loop, "127.0.0.1", 7777 + leader_id);

    if (strcmp(change_type, "add") == 0)
    {
        net_client_set_connection_callback(client, addServer, &srv);
    }
    else if (strcmp(change_type, "remove") == 0)
    {
        net_client_set_connection_callback(client, removeServer, &srv);
    }
    else {
        fprintf(stderr, "change type error.\n");
        fprintf(stderr, "usage: %s leader_id add|remove "
                "host port id\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    net_client_set_response_callback(client, process_res);
    net_loop_start(loop);
}
