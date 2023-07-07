#ifndef _RAFT_LOG_H_
#define _RAFT_LOG_H_

#include "raft.h"

#define MAX_ENTRIES 1000

struct raft_log_entry_cmd
{
    void *buf;
    int len; // bytes

    /*
     * 1: state machine
     * 2: no-op log entry, upon leader elected
     * 3: configuration entry / membership change
     */
    int type;
};

typedef void (*commit_handler)(void*);

struct raft_log_entry
{
    struct raft_log_entry_cmd cmd;
    int term;
    int file_offset;

    // user-defined state machine callback when committing
    commit_handler cb_handle;
    void *cb_arg;
};

void raft_init(char *path);
void raft_persist_votedFor(struct raft_server *rs);
void raft_persist_currentTerm(struct raft_server *rs);
void raft_persist_log(struct raft_server *rs, struct raft_log_entry *entry);
void raft_restore_log(struct raft_server *rs, char *path);
void raft_log_delete(struct raft_server *rs, int idx);

#endif // _RAFT_LOG_H_
