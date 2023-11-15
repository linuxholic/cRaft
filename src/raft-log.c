#include <stdio.h>        // fopen,fclose,fread,fwrite,fflush
#include <stdlib.h>       // malloc,calloc
#include <unistd.h>       // fdatasync
#include <sys/sendfile.h> // sendfile
#include <string.h>       // memcpy
#include <sys/stat.h>     // fstat
#include <errno.h>        // errno

#include "raft.h"
#include "raft-log.h"

void raft_persist_lastTerm(struct raft_server *rs)
{
    fseek(rs->log_handler, 12, SEEK_SET);
    fwrite(&rs->discard_term, 4, 1, rs->log_handler);
    fflush(rs->log_handler);
    fdatasync(fileno(rs->log_handler));
    loginfo("persist lastTerm:%d\n", rs->discard_term);
}

void raft_persist_lastIndex(struct raft_server *rs)
{
    fseek(rs->log_handler, 8, SEEK_SET);
    fwrite(&rs->discard_index, 4, 1, rs->log_handler);
    fflush(rs->log_handler);
    fdatasync(fileno(rs->log_handler));
    loginfo("persist lastIndex:%d\n", rs->discard_index);
}

void raft_persist_votedFor(struct raft_server *rs)
{
    fseek(rs->log_handler, 4, SEEK_SET);
    fwrite(&rs->votedFor, 4, 1, rs->log_handler);
    fflush(rs->log_handler);
    fdatasync(fileno(rs->log_handler));
    loginfo("persist votedFor:%d\n", rs->votedFor);
}

void raft_persist_currentTerm(struct raft_server *rs)
{
    fseek(rs->log_handler, 0, SEEK_SET);
    fwrite(&rs->currentTerm, 4, 1, rs->log_handler);
    fflush(rs->log_handler);
    fdatasync(fileno(rs->log_handler));
    loginfo("persist currentTerm:%d\n", rs->currentTerm);
}

void raft_persist_log_entry(struct raft_server *rs,
        struct raft_log_entry *entry)
{
    // TODO: 1. atomic write these pieces of data
    //       2. deal with write failure
    fseek(rs->log_handler, 0, SEEK_END);
    entry->file_offset = ftell(rs->log_handler);
    fwrite(&entry->term, sizeof(entry->term), 1, rs->log_handler);
    fwrite(&entry->cmd.len, sizeof(entry->cmd.len), 1, rs->log_handler);
    fwrite(entry->cmd.buf, entry->cmd.len, 1, rs->log_handler);
    fflush(rs->log_handler);
    fdatasync(fileno(rs->log_handler));

    rs->lastLogIndex++;
    rs->lastLogTerm = entry->term;

    loginfo("[%s] write log entry: index[%d], term[%d], cmd_len[%d]\n",
            raft_state(rs->state), rs->lastLogIndex,
            entry->term, entry->cmd.len);
}

void raft_persist_configuration(struct raft_server *rs, void *buf, int len)
{
    FILE *f = fopen(rs->configuration_path, "w");
    if (f == NULL)
    {
        logerr("fail to open file: %s\n", rs->configuration_path);
        return;
    }

    fwrite(buf, len, 1, f);
    loginfo("persist raft configuration entry into %s\n",
            rs->configuration_path);

    fflush(f);
    fdatasync(fileno(f));
    fclose(f);
}

void raft_snapshot_load(struct raft_server *rs)
{
    FILE *f = fopen(rs->configuration_path, "r");
    if (f == NULL)
    {
        loginfo("no raft snapshot file: %s\n", rs->configuration_path);
        return;
    }
    loginfo("loading raft snapshot file: %s\n", rs->configuration_path);

    struct stat stat_file;
    fstat(fileno(f), &stat_file);
    uint8_t *cur = malloc(sizeof(uint8_t) * stat_file.st_size);
    fread(cur, stat_file.st_size, 1, f);
    fclose(f);

    raft_apply_configuration(rs,
            cur + sizeof(uint32_t) // skip cmd type
            );
    free(cur);
}

void raft_log_restore(struct raft_server *rs, char *path)
{
    FILE *f = fopen(path, "r+");
    if (f == NULL && errno == ENOENT)
    {
        f = fopen(path, "w+");
        loginfo("create raft log '%s' and write meta info.\n", path);
    }
    else {
        loginfo("restore raft log '%s'\n", path);
    }

    if (f == NULL)
    {
        logerr("failed to restore raft log '%s': %s\n",
                path, strerror(errno));
        exit(EXIT_FAILURE);
    }
    rs->log_path = strdup(path);
    rs->log_handler = f;

    // as demo, we just load ENTIRE log file into memory
    // and flush entire file for every log entry write.
    rs->entries = calloc(MAX_ENTRIES, sizeof(struct raft_log_entry));

    struct stat stat_log;
    fstat(fileno(f), &stat_log);
    if (stat_log.st_size > 0)
    {
        uint8_t *bytes = malloc(sizeof(uint8_t) * stat_log.st_size);
        fread(bytes, stat_log.st_size, 1, f);

        int cursor = 0;
        memcpy(&rs->currentTerm, bytes, 4);
        cursor += 4;
        memcpy(&rs->votedFor, bytes + cursor, 4);
        cursor += 4;
        memcpy(&rs->discard_index, bytes + cursor, 4);
        cursor += 4;
        memcpy(&rs->discard_term, bytes + cursor, 4);
        cursor += 4;

        loginfo("currentTerm: %d, votedFor: %d, "
                "discard_index: %d, discard_term: %d\n",
                rs->currentTerm, rs->votedFor,
                rs->discard_index, rs->discard_term);

        rs->lastApplied = rs->discard_index;
        rs->commitIndex = rs->discard_index;

        if (stat_log.st_size > cursor)
        {
            struct raft_log_entry *entries = rs->entries;
            int idx = rs->discard_index;
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
            rs->lastLogIndex = rs->discard_index;
            rs->lastLogTerm = rs->discard_term;
        }
        free(bytes);
    }
    else {
        rs->commitIndex  = 0;
        rs->lastApplied  = 0;
        rs->lastLogIndex = 0;
        rs->lastLogTerm  = 0;

        rs->currentTerm   =  0;
        rs->votedFor      = -1;
        rs->discard_index =  0;
        rs->discard_term  =  0;

        /* persist meta info */
        fwrite(&rs->currentTerm,   4, 1, f);
        fwrite(&rs->votedFor,      4, 1, f);
        fwrite(&rs->discard_index, 4, 1, f);
        fwrite(&rs->discard_term,  4, 1, f);
        fflush(f);
        fdatasync(fileno(f));
    }
    // don't close f, we need it afterwards.
}

int raft_log_entry_type(struct raft_log_entry *e)
{
    uint32_t *buf = e->cmd.buf;
    return ntohl(buf[0]);
}

void raft_log_compaction_retain_configuration(
        struct raft_server *rs, int idx)
{
    while (idx > rs->discard_index)
    {
        struct raft_log_entry *e = &rs->entries[idx - 1];
        int type = raft_log_entry_type(e);
        if (type == RAFT_LOG_CONFIGURATION) // configuration entry
        {
            raft_persist_configuration(rs, e->cmd.buf, e->cmd.len);
            break;
        }
        idx--;
    }
}

static void _raft_log_delete_suffix(struct raft_server *rs, int idx)
{
    ftruncate(fileno(rs->log_handler), rs->entries[idx - 1].file_offset);
    fflush(rs->log_handler);
    fdatasync(fileno(rs->log_handler));

    rs->lastLogIndex = idx - 1;
    rs->lastLogTerm = rs->entries[rs->lastLogIndex - 1].term;
}

// delete log entries starting from @idx
void raft_log_delete_suffix(struct raft_server *rs, int idx)
{
    if (idx > rs->lastLogIndex) return;

    logerr("[%s] delete log entries suffix, index: %d\n",
            raft_state(rs->state), idx);

    int iter = idx;
    while (iter < rs->lastLogIndex)
    {
        free(rs->entries[iter - 1].cmd.buf);
        rs->entries[iter - 1].cmd.buf = NULL;
        rs->entries[iter - 1].cmd.len = 0;
        iter++;
    }

    _raft_log_delete_suffix(rs, idx);
}

/* delete log entries up through and INCLUDING @idx */
static void _raft_log_delete_prefix(struct raft_server *rs, int idx)
{
    struct stat stat_log;
    fstat(fileno(rs->log_handler), &stat_log);
    struct raft_log_entry *e = &rs->entries[idx - 1];
    off_t new_start = e->file_offset + 8 + e->cmd.len;
    size_t count = stat_log.st_size - new_start;

    char truncated_log[] = "raft_truncated_log_XXXXXX";
    int fd = mkstemp(truncated_log);
    write(fd, &rs->currentTerm, 4);
    write(fd, &rs->votedFor, 4);
    write(fd, &rs->discard_index, 4);
    write(fd, &rs->discard_term, 4);
    sendfile(fd, fileno(rs->log_handler), &new_start, count);
    fdatasync(fd);

    // rotate raft log file
    fclose(rs->log_handler);
    remove(rs->log_path);
    close(fd);
    rename(truncated_log, rs->log_path);
    rs->log_handler = fopen(rs->log_path, "r+");
}


void raft_log_delete_all(struct raft_server *rs)
{
    _raft_log_delete_prefix(rs, rs->lastLogIndex);
}

void raft_log_delete_prefix(struct raft_server *rs, int idx)
{
    logerr("[%s] delete log entries prefix, index: %d\n",
            raft_state(rs->state), idx);

    _raft_log_delete_prefix(rs, idx);
}

// suggested snapshot file names:
//
//   snapshot.StateMachine.ID
//   snapshot.RaftConfiguration.ID
//   snapshot.RaftLogDiscardedIndexAndTerm.ID
//
void raft_log_compaction(struct raft_server *rs)
{
    int commitIndex = rs->commitIndex;

    raft_log_compaction_retain_configuration(rs, commitIndex);

    rs->discard_index = commitIndex;
    rs->discard_term = rs->entries[commitIndex - 1].term;
    /* free disk space */
    raft_log_delete_prefix(rs, commitIndex);
    /* free mem space */
    raft_free_log_entries(rs, commitIndex);
}

void raft_init(char *path)
{
    FILE *f = fopen(path, "w");
    if (f == NULL)
    {
        logerr("failed to open raft log '%s': %s\n",
                path, strerror(errno));
        exit(EXIT_FAILURE);
    }
    loginfo("init raft log for first node.\n");

    /*              meta info                */

    int currentTerm   =  1;
    int votedFor      = -1;
    int discard_index =  0;
    int discard_term  =  0;
    fwrite(&currentTerm,   4, 1, f);
    fwrite(&votedFor,      4, 1, f);
    fwrite(&discard_index, 4, 1, f);
    fwrite(&discard_term,  4, 1, f);

    /*              configuration entry begin                */

    fwrite(&currentTerm, 4, 1, f);

    long a = ftell(f);
    int cmd_len = 10086; // placeholder
    fwrite(&cmd_len, 4, 1, f);
    long b = ftell(f);

    // fill command buffer: these data is transfered as raw bytes, so
    // we better convert them to network byte order in advance
    int cmd_type = htonl(3); // per membership change
    fwrite(&cmd_type, 4, 1, f);
    int addr_num = htonl(1);
    fwrite(&addr_num, 4, 1, f);
    char *ip = "127.0.0.1";
    int ip_len = strlen(ip);
    int _ip_len = htonl(ip_len);
    fwrite(&_ip_len, 4, 1, f);
    fwrite(ip, 1, ip_len, f);
    int port = htonl(7777);
    fwrite(&port, 4, 1, f);
    int id = htonl(0);
    fwrite(&id, 4, 1, f);

    // backpatching
    cmd_len = ftell(f) - b;
    fseek(f, a, SEEK_SET);
    fwrite(&cmd_len, 4, 1, f);

    /*              configuration entry end                  */

    fflush(f);
    fdatasync(fileno(f));
    fclose(f);
}
