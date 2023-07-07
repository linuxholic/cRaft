#include <unistd.h> // write,lseek,fsync
#include <fcntl.h> // open
#include <stdlib.h> // malloc,calloc
#include <sys/stat.h> // fstat
#include <string.h> // memcpy
#include <errno.h> // errno

#include "raft.h"
#include "raft-log.h"

void raft_persist_votedFor(struct raft_server *rs)
{
    lseek(rs->log_fd, 4, SEEK_SET);
    write(rs->log_fd, &rs->votedFor, 4);
    fsync(rs->log_fd);
    loginfo("persist votedFor:%d\n", rs->votedFor);
}

void raft_persist_currentTerm(struct raft_server *rs)
{
    lseek(rs->log_fd, 0, SEEK_SET);
    write(rs->log_fd, &rs->currentTerm, 4);
    fsync(rs->log_fd);
    loginfo("persist currentTerm:%d\n", rs->currentTerm);
}

void raft_persist_log(struct raft_server *rs, struct raft_log_entry *entry)
{
    // TODO: 1. atomic write these pieces of data
    //       2. deal with write failure
    entry->file_offset = lseek(rs->log_fd, 0, SEEK_END);
    write(rs->log_fd, &entry->term, sizeof(entry->term));
    write(rs->log_fd, &entry->cmd.len, sizeof(entry->cmd.len));
    write(rs->log_fd, entry->cmd.buf, entry->cmd.len);
    fsync(rs->log_fd);

    rs->lastLogIndex++;
    rs->lastLogTerm = entry->term;

    loginfo("[%s] write log entry: index[%d], term[%d], cmd_len[%d]\n",
            raft_state(rs->state), rs->lastLogIndex,
            entry->term, entry->cmd.len);
}

void raft_restore_log(struct raft_server *rs, char *path)
{
    int fd = open(path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IROTH);
    if (fd < 0)
    {
        logerr("failed to open log file: %s\n", path);
    }
    rs->log_fd = fd;

    // as demo, we just load ENTIRE log file into memory
    // and flush entire file for every log entry write.
    rs->entries = calloc(MAX_ENTRIES, sizeof(struct raft_log_entry));

    struct stat stat_log;
    fstat(fd, &stat_log);
    if (stat_log.st_size > 0)
    {
        uint8_t *bytes = malloc(sizeof(uint8_t) * stat_log.st_size);
        read(fd, bytes, stat_log.st_size);

        int cursor = 0;
        memcpy(&rs->currentTerm, bytes, 4);
        cursor += 4;
        memcpy(&rs->votedFor, bytes + cursor, 4);
        cursor += 4;

        if (stat_log.st_size > cursor)
        {
            struct raft_log_entry *entries = rs->entries;
            int idx = 0;
            while (stat_log.st_size > cursor)
            {
                entries[idx].file_offset = cursor;

                memcpy(&entries[idx].term, bytes + cursor, 4);
                cursor += 4;

                memcpy(&entries[idx].cmd.len, bytes + cursor, 4);
                cursor += 4;

                entries[idx].cmd.buf = malloc(entries[idx].cmd.len);
                memcpy(entries[idx].cmd.buf,
                       bytes + cursor, entries[idx].cmd.len);
                cursor += entries[idx].cmd.len;

                idx++;
            }
            rs->lastLogIndex = idx;
            rs->lastLogTerm = entries[idx - 1].term;
        }
        else {
            rs->lastLogIndex = 0;
            rs->lastLogTerm = 0;
        }
        free(bytes);
    }
    else {
        rs->lastLogIndex = 0;
        rs->lastLogTerm = 0;
        rs->votedFor = -1;
        rs->currentTerm = 0;
    }
}

// delete log entries starting from @idx
void raft_log_delete(struct raft_server *rs, int idx)
{
    if (idx > rs->lastLogIndex) return;

    logerr("[%s] delete log entries, index: %d\n",
            raft_state(rs->state), idx);

    int iter = idx;
    while (iter < rs->lastLogIndex)
    {
        free(rs->entries[iter - 1].cmd.buf);
        rs->entries[iter - 1].cmd.buf = NULL;
        rs->entries[iter - 1].cmd.len = 0;
        iter++;
    }

    ftruncate(rs->log_fd, rs->entries[idx - 1].file_offset);
    fsync(rs->log_fd);

    rs->lastLogIndex = idx - 1;
    rs->lastLogTerm = rs->entries[rs->lastLogIndex - 1].term;
}

void raft_init(char *path)
{
    int fd = open(path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IROTH);
    if (fd < 0)
    {
        logerr("failed to open raft log '%s': %s\n",
                path, strerror(errno));
        exit(EXIT_FAILURE);
    }

    int currentTerm = 1;
    write(fd, &currentTerm, 4);
    int votedFor = -1;
    write(fd, &votedFor, 4);

    // configuration entry
    write(fd, &currentTerm, 4);

    off_t a = lseek(fd, 0, SEEK_CUR);
    int cmd_len = 10086; // placeholder
    write(fd, &cmd_len, 4);
    off_t b = lseek(fd, 0, SEEK_CUR);

    // fill command buffer: these data is transfered as raw bytes, so
    // we better convert them to network byte order in advance
    int cmd_type = htonl(3); // per membership change
    write(fd, &cmd_type, 4);
    int addr_num = htonl(1);
    write(fd, &addr_num, 4);
    char *ip = "127.0.0.1";
    int ip_len = strlen(ip);
    int _ip_len = htonl(ip_len);
    write(fd, &_ip_len, 4);
    write(fd, ip, ip_len);
    int port = htonl(7777);
    write(fd, &port, 4);
    int id = htonl(0);
    write(fd, &id, 4);

    // backpatching
    off_t c = lseek(fd, 0, SEEK_CUR);
    lseek(fd, a, SEEK_SET);
    cmd_len = c - b;
    write(fd, &cmd_len, 4);

    fsync(fd);
    close(fd);
}

