#ifndef _RAFT_H_
#define _RAFT_H_

#include <stdio.h> // FILE

#include "hash.h"
#include "net.h"

struct raft_peer
{
    char *addr;
    int   port;
    int   id;
    int   nextIndex;
    int   matchIndex;
    int   inFlight;
};

struct raft_cluster
{
    int number;
    struct raft_peer *peers;
};

enum raft_server_state
{
    LEADER = 1
    , FOLLOWER
    , CANDIDATE
};

enum raft_rpc_type
{
    REQUEST_VOTE = 1
    , APPEND_ENTRIES
    , ADD_SERVER
    , REMOVE_SERVER
    , INSTALL_SNAPSHOT
};

struct raft_server
{
    net_timer_t            *heartbeat_timer;
    net_timer_t            *election_timer;
    int                     election_timer_rnd;
    net_server_t           *tcp_server;
    struct raft_cluster    *cluster;
    enum raft_server_state  state;

    int id;
    int currentTerm;
    int commitIndex;
    int lastApplied;

    /* RequestVote RPC */
    int votes;
    int votedFor;
    int lastLogIndex;
    int lastLogTerm;

    /* replicated log */
    FILE *log_handler;
    char *log_path;
    struct raft_log_entry *entries;

    /* log compaction */
    int discard_index;
    int discard_term;
    char *configuration_path; // raft cluster membership

    void *st; // state machine
};

char* raft_state(int s);
void raft_apply_configuration();
void raft_free_log_entries(struct raft_server *rs, int idx);

#endif // _RAFT_H_
