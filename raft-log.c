#include <stdlib.h> // exit
#include <stdio.h> // printf
#include <unistd.h> // read
#include <stdint.h> // uint32_t
#include <fcntl.h> // open
#include <arpa/inet.h> // ntohl

void decode_cmd(uint8_t *stream)
{
    uint8_t *cur = stream;

    int cmd_type = ntohl(*(uint32_t*)cur);
    cur += sizeof(uint32_t);

    if (cmd_type == 1) // CMD_GET
    {
        int key_len = ntohl(*(uint32_t*)cur);
        cur += sizeof(uint32_t);

        char *key = (char*)cur;
        cur += key_len;

        printf("cmd: GET key(%.*s)\n", key_len, key);
    }
    else if (cmd_type == 2) // CMD_SET
    {
        int key_len = ntohl(*(uint32_t*)cur);
        cur += sizeof(uint32_t);

        char *key = (char*)cur;
        cur += key_len;

        int value_len = ntohl(*(uint32_t*)cur);
        cur += sizeof(uint32_t);

        char *value = (char*)cur;
        cur += value_len;

        printf("cmd: SET key(%.*s) value(%.*s)\n",
                key_len, key, value_len, value);
    }
    else if (cmd_type == 3) // CMD_NOOP
    {
        printf("cmd: noop\n");
    }
    else {
        printf("cmd: unknown type: %d.\n", cmd_type);
    }
}

int main(int argc, char *argv[])
{
    if (argc != 2)
    {
        printf("usage: %s <log>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    char *path = argv[1];
    int fd = open(path, O_RDONLY);
    if (fd == -1)
    {
        printf("fail to open '%s'\n", path);
        exit(EXIT_FAILURE);
    }

    uint32_t currentTerm;
    if (read(fd, &currentTerm, 4) == 0)
    {
        printf("fail to read currentTerm in '%s'\n", path);
        exit(EXIT_FAILURE);
    }

    int votedFor;
    if (read(fd, &votedFor, 4) == 0)
    {
        printf("fail to read votedFor in '%s'\n", path);
        exit(EXIT_FAILURE);
    }

    printf("currentTerm: %u, votedFor: %d\n", currentTerm, votedFor);

    int index = 1;
    int term;
    while(read(fd, &term, 4))
    {
        printf("index: %d, term: %d, ", index, term);

        int len;
        read(fd, &len, 4);

        uint8_t *cmd = malloc(len);
        read(fd, cmd, len);
        decode_cmd(cmd);

        index++;
        free(cmd);
    }

    return 0;
}
