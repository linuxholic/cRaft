#include <stdlib.h>
#include <string.h>
#include <stdio.h>

#include "net.h"

enum CMD_TYPE
{
    CMD_GET = 1,
    CMD_SET,
    CMD_NOOP
};

struct kv_req
{
    enum CMD_TYPE type;
    char *key;
    char *value;
};

/*

command NOOP
+----------+
| cmd_type |
+----------+

command GET
+--------------------------+
| cmd_type | key_len | key |
+--------------------------+

command SET
+----------------------------------------------+
| cmd_type | key_len | key | value_len | value |
+----------------------------------------------+

 */
void send_req(net_connect_t *c, void *arg)
{
    if (c->err)
    {
        logerr("send_req failed: connection error.\n");
        return;
    }

    struct kv_req *kv = arg;
    net_buf_t *buf = net_buf_create(0);

    // encode cmd type
    uint32_t cmd_type = htonl(kv->type);
    net_buf_copy(buf, (char*)&cmd_type, sizeof(uint32_t));

    // encode key
    uint32_t key_len = htonl(strlen(kv->key));
    net_buf_copy(buf, (char*)&key_len, sizeof(uint32_t));
    net_buf_copy(buf, kv->key, strlen(kv->key));

    if (kv->type == CMD_SET)
    {
        // encode value
        uint32_t value_len = htonl(strlen(kv->value));
        net_buf_copy(buf, (char*)&value_len, sizeof(uint32_t));
        net_buf_copy(buf, kv->value, strlen(kv->value));
    }

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
    if (argc < 4)
    {
        fprintf(stderr, "usage: %s leader_id <get|set> key [value]\n",
                argv[0]);
        exit(EXIT_FAILURE);
    }

    int leader_id = atoi(argv[1]);

    enum CMD_TYPE type;
    char *key = NULL, *value = NULL, *type_arg = argv[2];
    if (strcmp(type_arg, "get") == 0)
    {
        type = CMD_GET;
        key = argv[3];
    }
    else if (strcmp(type_arg, "set") == 0)
    {
        type = CMD_SET;
        key = argv[3];
        value = argv[4];
    }
    else {
        fprintf(stderr, "unknown cmd type: %s.\n", type_arg);
        exit(EXIT_FAILURE);
    }

    net_log_level(LOG_INFO);

    net_loop_t *loop = net_loop_init(1000);
    if (!loop)
    {
        logerr("init loop failed.\n");
        exit(EXIT_FAILURE);
    }

    struct kv_req kv = {type, key, value};
    net_client_t *client = net_client_init(loop, "127.0.0.1", 8888 + leader_id);
    net_client_set_connection_callback(client, send_req, &kv);
    net_client_set_response_callback(client, process_res);

    net_loop_start(loop);
}
