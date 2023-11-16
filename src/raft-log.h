#ifndef _RAFT_LOG_H_
#define _RAFT_LOG_H_

#include "raft.h"

#define MAX_ENTRIES 1000

enum raft_log_entry_type
{
    RAFT_LOG_STATE_MACHINE = 1,
    RAFT_LOG_NO_OP,
    RAFT_LOG_CONFIGURATION
};

struct raft_log_entry_cmd
{
    void *buf;
    int len; // bytes

    /*
     * 1: state machine
     * 2: no-op log entry, upon leader elected
     * 3: configuration entry / membership change
     */
    enum raft_log_entry_type type;
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

/*
 * 1. raft log layout
 *
 *   currentTerm | votedFor | index | term
 *   regualr entry | regular entry | ...
 *   regualr entry | regular entry | ...
 *   regualr entry | regular entry | ...
 *
 * 1.1 raft log entry layout
 *
 *   term | cmd_len | cmd_type | cmd_bytes
 *
 * @cmd_len includes cmd_type and cmd_bytes
 *
 * 1.1.1 raft config entry layout
 *
 *   ip_num | ip_len | ip_str | port | id
 *          | ip_len | ip_str | port | id
 *          | ip_len | ip_str | port | id
 *
 * corresponding to @cmd_bytes
 *
 */

void raft_init(char *path);

void raft_persist_lastTerm(struct raft_server *rs);
void raft_persist_lastIndex(struct raft_server *rs);
void raft_persist_votedFor(struct raft_server *rs);
void raft_persist_currentTerm(struct raft_server *rs);
void raft_persist_configuration(struct raft_server *rs, void *buf, int len);
void raft_persist_log_entry(struct raft_server *rs, struct raft_log_entry *entry);

void raft_log_restore(struct raft_server *rs, char *path);
void raft_log_delete_suffix(struct raft_server *rs, int idx);
void raft_log_delete_prefix(struct raft_server *rs, int idx);
void raft_log_compaction(struct raft_server *rs);
int  raft_log_entry_type(struct raft_log_entry *e);

void raft_snapshot_load(struct raft_server *rs);

#endif // _RAFT_LOG_H_
