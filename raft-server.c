#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>

#include "net.h"
#include "hash.h"

struct raft_log_entry_cmd
{
    void *buf;
    int len; // bytes
};

struct kv_raft_ctx
{
    struct net_connect_t *c;
    struct kv_cmd *cmd;
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

struct raft_peer
{
    char *addr;
    int port;
};

struct raft_cluster
{
    int number;
    struct raft_peer *peers;
};

enum raft_server_state
{
    LEADER = 1,
    FOLLOWER,
    CANDIDATE
};

enum raft_rpc_type
{
    REQUEST_VOTE = 1,
    APPEND_ENTRIES
};

struct raft_server
{
    net_timer_t            *heartbeat_timer;
    net_timer_t            *election_timer;
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

    /* AppendEntries RPC */
    int prevLogIndex;
    int *nextIndex;
    int *matchIndex;
    int *inFlight;
    int prevLogTerm;

    int log_fd;
    struct raft_log_entry *entries;

    void *st; // state machine
};

struct kv_server
{
    struct hashTable *map;
    struct raft_server *rs;
};

struct kv_cmd
{
    int type;
    char *key;
    char *value;
};

void kv_set(struct kv_server *svr, char *key, char *value)
{
    // TODO: add kv_del to free memory
    // allocated by strdup()
    hashPut(svr->map, strdup(key), strdup(value));
}

char *kv_get(struct kv_server *svr, char *key)
{
    return hashGet(svr->map, key);
}

#define ElecttionTimeout 100 // ms
#define MAX_ENTRIES 1000

char* raft_state(int s)
{
    char *str = NULL;
    switch(s)
    {
        case LEADER:
            str = "LEADER";
            break;
        case FOLLOWER:
            str = "FOLLOWER";
            break;
        case CANDIDATE:
            str = "CANDIDATE";
            break;
        default:
            str = "UnknownState";
            break;
    }
    return str;
}

// TODO: move random time to election, so we can reduce random() calls
// we only need to call random() when a real election occurs.
int random_ElecttionTimeout()
{
    // TODO: align to paper's suggested value
    int timeout = 10 + random() % 10;
    loginfo("random ElecttionTimeout (in seconds): %d\n", timeout);
    return timeout;
}

void _RequestVote_receiver(net_connect_t *c, int term, int voteGranted)
{
    net_buf_t *reply = net_buf_create(0);

    int _term = htonl(term);
    net_buf_copy(reply, (char*)&_term, sizeof(int));

    int _voteGranted = htonl(voteGranted);
    net_buf_copy(reply, (char*)&_voteGranted, sizeof(int));

    loginfo("RequestVote results: term(%u), voteGranted(%u).\n",
            term, voteGranted);

    list_append(&c->outbuf, &reply->node);
    net_connection_send(c);
}

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

void RequestVote_receiver(net_connect_t *c, uint32_t *res)
{
    struct raft_server *rs = c->data;

    uint32_t term         = ntohl(res[1]);
    uint32_t candidateId  = ntohl(res[2]);
    uint32_t lastLogIndex = ntohl(res[3]);
    uint32_t lastLogTerm  = ntohl(res[4]);

    loginfo("[%s] recv RequestVote RPC: term(%u), candidateId(%u), "
            "lastLogIndex(%u), lastLogTerm(%u)\n",
            raft_state(rs->state), term, candidateId,
            lastLogIndex, lastLogTerm);

    if (rs->currentTerm > term)
    {
        _RequestVote_receiver(c, rs->currentTerm, 0);
        loginfo("recv RequestVote RPC with lower term(%d,%d), ignore it.\n",
                rs->currentTerm, term);
    }
    else {
        // whenever see larger term, update local currentTerm
        if (term > rs->currentTerm)
        {
            loginfo("[%s] node(%d) update term: %d -> %d.\n",
                    raft_state(rs->state), rs->id, rs->currentTerm, term);
            rs->currentTerm = term;
            raft_persist_currentTerm(rs);
            rs->votedFor = -1;
            raft_persist_votedFor(rs);

            // whenever see larger term, convert to FOLLOWER
            if (rs->state != FOLLOWER)
            {
                loginfo("node(%d) convert state: %s -> %s.\n", rs->id,
                        raft_state(rs->state), raft_state(FOLLOWER));
                rs->state = FOLLOWER;
                // as follower, from this moment on, node
                // need to repond to RequestVote RPCs
            }
        }

        if (rs->votedFor == -1 || rs->votedFor == candidateId)
        {
            // check which log is more up-to-date
            if (rs->lastLogTerm > lastLogTerm)
            {
                loginfo("RequestVote safety check: local term is bigger.\n");
                _RequestVote_receiver(c, rs->currentTerm, 0);
            }
            else if (rs->lastLogIndex > lastLogIndex)
            {
                loginfo("RequestVote safety check: local log is longer.\n");
                _RequestVote_receiver(c, rs->currentTerm, 0);
            }
            else {
                // grant vote
                _RequestVote_receiver(c, rs->currentTerm, 1);
                loginfo("[%s] node(%d) grant vote to node(%d) for term(%d).\n",
                        raft_state(rs->state), rs->id, candidateId, term);
                rs->votedFor = candidateId;
                raft_persist_votedFor(rs);
                net_timer_reset(rs->election_timer,
                        random_ElecttionTimeout(), 0);
            }
        }
        else {
            _RequestVote_receiver(c, rs->currentTerm, 0);
            loginfo("[%s] node(%d) deny vote to node(%d) for term(%d).\n",
                    raft_state(rs->state), rs->id, candidateId, term);
        }
    }
}

void _AppendEntries_receiver(net_connect_t *c, int term, int success)
{
    net_buf_t *reply = net_buf_create(0);

    int _term = htonl(term);
    net_buf_copy(reply, (char*)&_term, sizeof(int));

    int _success = htonl(success);
    net_buf_copy(reply, (char*)&_success, sizeof(int));

    loginfo("AppendEntries results: term(%u), success(%u).\n",
            term, success);

    list_append(&c->outbuf, &reply->node);
    net_connection_send(c);
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

// TODO:
// associate state machine command with raft log entry, so
// we can parse command byte steam just once, then reuse
// the, ie kv cmd, structure afterwards.(we can retain these
// memory until log compaction)
struct kv_cmd* parse_cmd(uint8_t *stream)
{
    struct kv_cmd *cmd = malloc(sizeof(struct kv_cmd));
    uint8_t *cur = stream;

    int cmd_type = ntohl(*(uint32_t*)cur);
    cur += sizeof(uint32_t);

    cmd->type = cmd_type;
    if (cmd_type == 1) // CMD_GET
    {
        int key_len = ntohl(*(uint32_t*)cur);
        cur += sizeof(uint32_t);

        char *key = malloc(key_len + 1);
        memcpy(key, cur, key_len);
        key[key_len] = '\0';
        cur += key_len;

        loginfo("command GET key(%s)\n", key);

        cmd->key = key;
        cmd->value = NULL;
    }
    else if (cmd_type == 2) // CMD_SET
    {
        int key_len = ntohl(*(uint32_t*)cur);
        cur += sizeof(uint32_t);

        char *key = malloc(key_len + 1);
        memcpy(key, cur, key_len);
        key[key_len] = '\0';
        cur += key_len;

        int value_len = ntohl(*(uint32_t*)cur);
        cur += sizeof(uint32_t);

        char *value = malloc(value_len + 1);
        memcpy(value, cur, value_len);
        value[value_len] = '\0';
        cur += value_len;

        loginfo("command SET key(%s), value(%s)\n",
                key, value);

        cmd->key = key;
        cmd->value = value;
    }
    else if (cmd_type == 3) // CMD_SET
    {
        loginfo("command noop.\n");
        cmd->key = NULL;
        cmd->value = NULL;
    }
    else {
        logerr("client request unknown cmd type: %d.\n", cmd_type);
    }

    return cmd;
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

// TODO: this should be registered by state machine into raft, then raft
// will call it when log entry is considered to be commited.
// ie, raft_register_state_machine_apply_callback() API
//
// maybe better parameters:
// * command buffer
// * state machine ctx
void raft_apply_state_machine(struct raft_server *rs, int index)
{
    struct raft_log_entry *e = &rs->entries[index - 1];

    struct kv_cmd *cmd = parse_cmd(e->cmd.buf);
    struct kv_server *kvs = rs->st;
    switch (cmd->type)
    {
        case 1:
            loginfo("log[%d] apply state machine, GET key: %s, value: %s\n",
                    index, cmd->key, kv_get(kvs, cmd->key));
            break;
        case 2: // TODO: only SET command need to be applied
            kv_set(kvs, cmd->key, cmd->value);
            loginfo("log[%d] apply state machine, SET key: %s, value: %s\n",
                    index, cmd->key, cmd->value);
            break;
        case 3:
            loginfo("log[%d] apply state machine, noop.\n", index);
            break;
        default:
            logerr("log[%d] apply state machine, unknown kv cmd type: %d\n",
                    index, cmd->type);
    }

    free(cmd);
}

void AppendEntries_receiver(net_connect_t *c, uint32_t *res)
{
    uint8_t *cur = (uint8_t*)(res + 1);
    struct raft_server *rs = c->data;

    uint32_t term         = ntohl(*(uint32_t*)cur); cur += sizeof(uint32_t);
    uint32_t leaderId     = ntohl(*(uint32_t*)cur); cur += sizeof(uint32_t);
    uint32_t prevLogIndex = ntohl(*(uint32_t*)cur); cur += sizeof(uint32_t);
    uint32_t prevLogTerm  = ntohl(*(uint32_t*)cur); cur += sizeof(uint32_t);

    loginfo("[%s] recv AppendEntries RPC: term(%u), leaderId(%u), "
            "prevLogIndex(%u), prevLogTerm(%u)\n",
            raft_state(rs->state), term, leaderId,
            prevLogIndex, prevLogTerm);

    uint32_t num = ntohl(*(uint32_t*)cur);
    cur += sizeof(uint32_t);

    if (rs->currentTerm > term)
    {
        loginfo("recv AppendEntries RPC with lower term(%d,%d), ignore it.\n",
                rs->currentTerm, term);
        _AppendEntries_receiver(c, rs->currentTerm, 0);
        // non-valid leader, should not reset election timer
        return;
    }
    else {
        // whenever see larger term, update local currentTerm
        if (term > rs->currentTerm)
        {
            loginfo("[%s] node(%d) update term: %d -> %d.\n",
                    raft_state(rs->state), rs->id, rs->currentTerm, term);
            rs->currentTerm = term;
            raft_persist_currentTerm(rs);
            rs->votedFor = -1;
            raft_persist_votedFor(rs);

            // whenever see larger term, convert to FOLLOWER
            if (rs->state != FOLLOWER)
            {
                loginfo("node(%d) convert state: %s -> %s.\n", rs->id,
                        raft_state(rs->state), raft_state(FOLLOWER));
                rs->state = FOLLOWER;
            }
        }
        else {
            // another candidate get majority votes within same term
            if (rs->state == CANDIDATE)
            {
                loginfo("node(%d) convert state: %s -> %s.\n", rs->id,
                        raft_state(rs->state), raft_state(FOLLOWER));
                rs->state = FOLLOWER;
            }
            else {
                // normal log replication
            }
        }

        // consistency check
        if (rs->lastLogIndex >= prevLogIndex)
        {
            if (prevLogIndex == 0)
            {
                // heartbeat
            }
            else if (rs->entries[prevLogIndex - 1].term == prevLogTerm)
            {
                raft_log_delete(rs, prevLogIndex + 1);
            }
            else {
                loginfo("log term not match\n");
                _AppendEntries_receiver(c, rs->currentTerm, 0);
                net_timer_reset(rs->election_timer,
                        random_ElecttionTimeout(), 0);
                return;
            }
        }
        else {
            loginfo("log index not match\n");
            _AppendEntries_receiver(c, rs->currentTerm, 0);
            net_timer_reset(rs->election_timer, random_ElecttionTimeout(), 0);
            return;
        }

        /*
         * append log entries into local log file
         */
        for (int i = 0; i < num; i++)
        {
            struct raft_log_entry *entry = &rs->entries[rs->lastLogIndex];

            entry->cmd.len = ntohl(*(uint32_t*)cur);
            cur += sizeof(uint32_t);

            entry->cmd.buf = malloc(entry->cmd.len);
            memcpy(entry->cmd.buf, cur, entry->cmd.len);
            cur += entry->cmd.len;

            entry->term = ntohl(*(uint32_t*)cur);
            cur += sizeof(uint32_t);

            if (rs->lastLogIndex >= MAX_ENTRIES)
            {
                logerr("exceed max entries limit.\n");
                exit(EXIT_FAILURE);
            }

            raft_persist_log(rs, entry);
        }

        _AppendEntries_receiver(c, rs->currentTerm, 1);
        net_timer_reset(rs->election_timer, random_ElecttionTimeout(), 0);
    }

    uint32_t leaderCommit = ntohl(*(uint32_t*)cur);
    cur += sizeof(uint32_t);

    loginfo("[%s] recv AppendEntries RPC: leaderCommit(%u)\n",
            raft_state(rs->state), leaderCommit);

    if (leaderCommit > rs->commitIndex)
    {
        uint32_t index_of_last_new_entry = prevLogIndex + num;

        loginfo("[%s] align commitIndex(%d) to leaderCommit(%d) "
                "or index of last new entry(%d)\n",
                raft_state(rs->state), rs->commitIndex,
                leaderCommit, index_of_last_new_entry);

        rs->commitIndex = leaderCommit < index_of_last_new_entry ?
            leaderCommit : index_of_last_new_entry;
    }

    while (rs->lastApplied < rs->commitIndex)
    {
        loginfo("[%s] increment lastApplied(%d -> %d).\n",
                raft_state(rs->state),
                rs->lastApplied, rs->lastApplied + 1);

        rs->lastApplied++;
        raft_apply_state_machine(rs, rs->lastApplied);
    }
}

int raft_follower(char *start, size_t size, net_connect_t *c)
{
    // decode results
    uint32_t *res = (uint32_t *)start;
    uint32_t rpc_type = ntohl(res[0]);

    switch (rpc_type)
    {
        case REQUEST_VOTE:
            if (size < 20)
            {
                loginfo("RequestVote RPC: partial results.\n");
                return 0;
            }
            RequestVote_receiver(c, res);
            break;

        case APPEND_ENTRIES:
            if (size < 28) // TODO: non-empty AppendEntries RPC
            {
                loginfo("AppendEntries RPC: partial results.\n");
                return 0;
            }
            AppendEntries_receiver(c, res);
            break;

        default:
            logerr("raft follower recv unknown rpc type: %u\n", rpc_type);
            break;
    }

    return size;
}

int raft_candidate(char *start, int size, net_connect_t *c)
{
    struct raft_server *rs = c->data;
    // decode results
    uint32_t *res = (uint32_t *)start;
    uint32_t rpc_type = ntohl(res[0]);

    switch (rpc_type)
    {
        case REQUEST_VOTE:
            if (size < 20)
            {
                loginfo("RequestVote RPC: partial results.\n");
                return 0;
            }
            RequestVote_receiver(c, res);
            break;

        case APPEND_ENTRIES:
            if (size < 28) // TODO: non-empty AppendEntries RPC
            {
                loginfo("AppendEntries RPC: partial results.\n");
                return 0;
            }
            AppendEntries_receiver(c, res);
            break;

        default:
            logerr("raft candidate recv unknown rpc type: %u\n", rpc_type);
            break;
    }

    return size;
}

int raft_leader(char *start, int size, net_connect_t *c)
{
    // decode results
    uint32_t *res = (uint32_t *)start;
    uint32_t rpc_type = ntohl(res[0]);

    switch (rpc_type)
    {
        case REQUEST_VOTE:
            // TODO: MAYBE convert to follower, it depends.
            //
            // if one follower can't recv packets while it can
            // send out packets, then leader should not step down.
            //
            // if there is a STABLE leader arcoss the whole
            // cluster, then RequestVote with bigger term should
            // be ignored, which is also the situation when changing
            // memebers, say, remove one from cluster.
            loginfo("raft leader recv RequestVote RPC, ignore it.\n");
            break;

        case APPEND_ENTRIES:
            if (size < 28) // TODO: non-empty AppendEntries RPC
            {
                loginfo("AppendEntries RPC: partial results.\n");
                return 0;
            }
            AppendEntries_receiver(c, res);
            break;

        default:
            logerr("raft leader recv unknown rpc type: %u\n", rpc_type);
            break;
    }

    return size;
}

int peer_rpc(char *start, size_t size, net_connect_t *c)
{
    int parsed = 0;
    struct raft_server *rs = c->data;
    switch (rs->state)
    {
        case LEADER:
            parsed = raft_leader(start, size, c);
            break;
        case FOLLOWER:
            parsed = raft_follower(start, size, c);
            break;
        case CANDIDATE:
            parsed = raft_candidate(start, size, c);
            break;
        default:
            logerr("role: undefined\n");
            break;
    }
    return parsed;
}

int raft_get_id(char *host, int port)
{
    return port - 7777;
}

void raft_on_commit(struct raft_server *rs, int index)
{
    struct raft_log_entry *e = &rs->entries[index - 1];
    if (e->cb_handle)
    {
        loginfo("log entry commited, trigger state machine callback.\n");

        rs->lastApplied = index;
        e->cb_handle(e->cb_arg);
        e->cb_handle = NULL;

        // TODO: implement keep-alive connection in libnet and
        // close idle connection when idle timer expires.
        e->cb_arg = NULL;
    }
    else {
        loginfo("log entry commited, NO state machine callback.\n");
    }
}

void raft_try_commit(struct raft_server *rs, int index)
{
    int majority = 0;
    for (int i = 0; i < rs->cluster->number; i++)
    {
        loginfo("matchIndex[%d]: %d,  try commit index: %d.\n",
                i, rs->matchIndex[i], index);

        if (rs->matchIndex[i] >= index)
        {
            majority++;
        }
    }

    if (majority > rs->cluster->number / 2
            && rs->entries[index - 1].term == rs->currentTerm)
    {
        rs->commitIndex = index;
        loginfo("log entry index[%d] commited.\n", rs->commitIndex);

        // commitIndex maybe take a jump instead of increment
        // one by one, especially when new come to power, so
        // we need following while loop to catch up.
        while (rs->lastApplied < rs->commitIndex)
        {
            loginfo("[%s] increment lastApplied(%d -> %d).\n",
                    raft_state(rs->state),
                    rs->lastApplied, rs->lastApplied + 1);

            rs->lastApplied++;
            raft_apply_state_machine(rs, rs->lastApplied);

            raft_on_commit(rs, rs->lastApplied);
        }
    }
    else {
        loginfo("log entry index[%d] NOT commited, "
                "replicated: %d, log term: %d, leader currentTerm: %d\n",
                index, majority,
                rs->entries[index - 1].term, rs->currentTerm);
    }
}

void raft_commit(struct raft_server *rs)
{
    int try_commit = rs->commitIndex + 1;
    while (try_commit <= rs->lastLogIndex)
    {
        raft_try_commit(rs, try_commit);
        try_commit++;
    }
}

int ___AppendEntries_invoke(char *start, size_t size, net_connect_t *c)
{
    // partial results
    if (size < 8) return 0;

    // decode results
    uint32_t *res = (uint32_t *)start;
    uint32_t term    = ntohl(res[0]);
    uint32_t success = ntohl(res[1]);

    struct raft_server *rs = c->client->user_data;

    loginfo("[%s] AppendEntries_invoke recv response: "
            "term(%u), success(%u).\n",
            raft_state(rs->state), term, success);

    int peer_id = raft_get_id(c->client->peer_host, c->client->peer_port);
    if (success)
    {
        if (rs->inFlight[peer_id] > 0)
        {
            rs->nextIndex[peer_id] += rs->inFlight[peer_id];
            rs->matchIndex[peer_id] = rs->nextIndex[peer_id]- 1;
            raft_commit(rs);
        }
    }
    else {
        if (term > rs->currentTerm)
        {
            rs->currentTerm = term;
            raft_persist_currentTerm(rs);

            rs->state = FOLLOWER;
            loginfo("raft leader: convert to follower.\n");

            // pause heartbeat to peers
            net_timer_reset(rs->heartbeat_timer, 0, 0);
        }

        // log consistency check failed, so decrease nextIndex
        rs->nextIndex[peer_id] -= 1;
    }

    net_connection_set_close(c); // non-keepalive
    return size;
}

void __AppendEntries_invoke(net_connect_t *c, void *arg)
{
    int peer_id = raft_get_id(c->client->peer_host,
                              c->client->peer_port);
    struct raft_server *rs = arg;

    if (c->err)
    {
        loginfo("[%s] send AppendEntries RPC to node(%d): "
                "failed to connect.\n",
                raft_state(rs->state), peer_id);
        return;
    }

    net_buf_t *reply = net_buf_create(0);

    uint32_t rpc_type = htonl(APPEND_ENTRIES);
    net_buf_copy(reply, (char*)&rpc_type, sizeof(uint32_t));

    uint32_t term = htonl(rs->currentTerm);
    net_buf_copy(reply, (char*)&term, sizeof(uint32_t));

    uint32_t leaderId = htonl(rs->id);
    net_buf_copy(reply, (char*)&leaderId, sizeof(uint32_t));

    uint32_t _prevLogIndex = rs->nextIndex[peer_id] - 1;
    uint32_t prevLogIndex = htonl(_prevLogIndex);
    net_buf_copy(reply, (char*)&prevLogIndex, sizeof(uint32_t));

    uint32_t _prevLogTerm;
    if (_prevLogIndex == 0)
    {
        _prevLogTerm = 0;
        uint32_t prevLogTerm = htonl(_prevLogTerm);
        net_buf_copy(reply, (char*)&prevLogTerm, sizeof(uint32_t));
    }
    else {
        _prevLogTerm = rs->entries[_prevLogIndex - 1].term;
        uint32_t prevLogTerm = htonl(_prevLogTerm);
        net_buf_copy(reply, (char*)&prevLogTerm, sizeof(uint32_t));
    }

    // TODO: multi log entries
    struct raft_log_entry *entries = c->client->user_data;
    if (entries)
    {
        rs->inFlight[peer_id] = 1;
    }
    else {
        if (_prevLogIndex < rs->lastLogIndex) // lagging follower
        {
            entries = &rs->entries[_prevLogIndex];
            rs->inFlight[peer_id] = 1;
        }
        else { // heartbeat
            rs->inFlight[peer_id] = 0;
        }
    }

    uint32_t _num = htonl(rs->inFlight[peer_id]);
    net_buf_copy(reply, (char*)&_num, sizeof(uint32_t));

    for (int i = 0; i < rs->inFlight[peer_id]; i++)
    {
        uint32_t len = htonl(entries[i].cmd.len);
        net_buf_copy(reply, (char*)&len, sizeof(uint32_t));
        net_buf_copy(reply, (char*)entries[i].cmd.buf, entries[i].cmd.len);

        uint32_t entry_term = htonl(entries[i].term);
        net_buf_copy(reply, (char*)&entry_term, sizeof(uint32_t));
    }

    uint32_t leaderCommit = htonl(rs->commitIndex);
    net_buf_copy(reply, (char*)&leaderCommit, sizeof(uint32_t));

    loginfo("[%s] send AppendEntries RPC: term(%d), leaderId(%d), "
            "prevLogIndex(%d), prevLogTerm(%d), logEntries[%d], "
            "leaderCommit(%d)\n", raft_state(rs->state),
            rs->currentTerm, rs->id, _prevLogIndex,
            _prevLogTerm, rs->inFlight[peer_id], rs->commitIndex);

    // send to wire
    list_append(&c->outbuf, &reply->node);
    net_connection_send(c);

    net_client_set_response_callback(c->client, ___AppendEntries_invoke);
    net_client_set_user_data(c->client, rs);
}

void _AppendEntries_invoke(struct raft_server *local, struct raft_peer *peer,
        struct raft_log_entry *entries)
{
    // skip self
    if (local->tcp_server->local_port == peer->port) return;

    net_client_t *client = net_client_init(local->tcp_server->loop,
            peer->addr, peer->port);

    if (client == NULL)
    {
        loginfo("node(%u) connect peer '%s:%d' failed.\n",
                local->id, peer->addr, peer->port);
        return;
    }

    net_client_set_connection_callback(client, __AppendEntries_invoke, local);
    net_client_set_user_data(client, entries);
}

void AppendEntries_invoke_empty(net_timer_t *timer)
{
    struct raft_server *rs = net_timer_data(timer);

    for (int i = 0; i < rs->cluster->number; i++)
    {
        struct raft_peer *peer = &rs->cluster->peers[i];
        _AppendEntries_invoke(rs, peer, NULL);
    }
}

void raft_leader_heartbeat(net_loop_t *loop, struct raft_server *rs)
{
    if (rs->heartbeat_timer)
    {
        net_timer_reset(rs->heartbeat_timer, 2, 2);
    }
    else {
        rs->heartbeat_timer = net_timer_init(loop, 2, 2);
        net_timer_start(rs->heartbeat_timer, AppendEntries_invoke_empty, rs);
    }
}

void raft_reset_nextIndex(struct raft_server *rs)
{
    for (int i = 0; i < rs->cluster->number; i++)
    {
        rs->nextIndex[i] = rs->lastLogIndex + 1;
    }
}

void raft_reset_matchIndex(struct raft_server *rs)
{
    for (int i = 0; i < rs->cluster->number; i++)
    {
        if (rs->id == i)
        {
            rs->matchIndex[i] = rs->lastLogIndex;
        }
        else {
            rs->matchIndex[i] = 0;
        }
    }
}

// TODO: this should be registered by state
// machine, then called by raft when needed.
struct raft_log_entry *raft_noop_log(struct raft_server *rs)
{
    struct raft_log_entry *e = &rs->entries[rs->lastLogIndex];

    uint32_t cmd_type = htonl(3); // no-op cmd type
    e->term = rs->currentTerm;
    e->cmd.len = sizeof(uint32_t);
    e->cmd.buf = malloc(e->cmd.len);
    memcpy(e->cmd.buf, &cmd_type, e->cmd.len);

    return e;
}

/*
 * 1. write local log
 * 2. replicate log entry to raft peers
 */
void raft_log_replication(struct raft_server *rs,
        struct raft_log_entry *entry)
{
    raft_persist_log(rs, entry);

    rs->nextIndex[rs->id]++;
    rs->matchIndex[rs->id] = rs->lastLogIndex;

    // replicate log entry to peers
    for (int i = 0; i < rs->cluster->number; i++)
    {
        struct raft_peer *peer = &rs->cluster->peers[i];
        _AppendEntries_invoke(rs, peer, entry);
    }
    net_timer_reset(rs->heartbeat_timer, 2, 2);
}

int __RequestVote_invoke(char *start, size_t size, net_connect_t *c)
{
    // partial results
    if (size < 8) return 0;

    // decode results
    uint32_t *res = (uint32_t *)start;
    uint32_t term        = ntohl(res[0]);
    uint32_t voteGranted = ntohl(res[1]);

    struct raft_server *rs = c->client->user_data;

    loginfo("[%s] RequestVote_invoke recv response: "
            "term(%u), voteGranted(%u).\n",
            raft_state(rs->state), term, voteGranted);

    // just ignore subsequent votes after becoming leader
    if (rs->state == LEADER) goto done;

    if (voteGranted == 1)
    {
        rs->votes++;
        loginfo("node(%d) recv one more vote.\n", rs->id);

        if (rs->votes > rs->cluster->number / 2)
        {
            rs->state = LEADER;
            loginfo("node(%d) recv majority votes, convert to LEADER.\n",
                    rs->id);

            net_timer_reset(rs->election_timer, 0, 0); // pause
            raft_reset_nextIndex(rs);
            raft_reset_matchIndex(rs);

            // setup periodical heartbeat to peers
            raft_leader_heartbeat(c->loop, rs);

            // commit no-op log entry, and as a side effect, this will
            // also prevent other peers from starting new election.
            struct raft_log_entry *e = raft_noop_log(rs);
            raft_log_replication(rs, e);
        }
    }
    else {
        if (term > rs->currentTerm)
        {
            rs->currentTerm = term;
            raft_persist_currentTerm(rs);
            rs->state = FOLLOWER;
            loginfo("raft candidate: convert to follower.\n");
        }
    }

done:
    net_connection_set_close(c); // non-keepalive
    return size;
}

void _RequestVote_invoke(net_connect_t *c, void *arg)
{
    struct raft_server *rs = arg;

    if (c->err)
    {
        loginfo("[%s] send RequestVote RPC: failed to connect.\n",
                raft_state(rs->state));
        return;
    }

    // encode RequestVote RPC
    net_buf_t *reply = net_buf_create(0);

    uint32_t rpc_type = htonl(REQUEST_VOTE);
    net_buf_copy(reply, (char*)&rpc_type, sizeof(uint32_t));

    uint32_t term = htonl(rs->currentTerm);
    net_buf_copy(reply, (char*)&term, sizeof(uint32_t));

    uint32_t candidateId = htonl(rs->id);
    net_buf_copy(reply, (char*)&candidateId, sizeof(uint32_t));

    uint32_t lastLogIndex = htonl(rs->lastLogIndex);
    net_buf_copy(reply, (char*)&lastLogIndex, sizeof(uint32_t));

    uint32_t lastLogTerm = htonl(rs->lastLogTerm);
    net_buf_copy(reply, (char*)&lastLogTerm, sizeof(uint32_t));

    loginfo("[%s] send RequestVote RPC: rpc_type(%u), term(%u), "
            "candidateId(%u), lastLogIndex(%u), lastLogTerm(%u)\n",
            raft_state(rs->state), REQUEST_VOTE, rs->currentTerm,
            rs->id, rs->lastLogIndex, rs->lastLogTerm);

    // send RequestVote RPC
    list_append(&c->outbuf, &reply->node);
    net_connection_send(c);

    net_client_set_response_callback(c->client, __RequestVote_invoke);
    net_client_set_user_data(c->client, rs);
}

void RequestVote_invoke(struct raft_server *local, struct raft_peer *peer)
{
    // skip self
    if (local->tcp_server->local_port == peer->port) return;

    net_client_t *client = net_client_init(local->tcp_server->loop,
            peer->addr, peer->port);

    if (client == NULL)
    {
        loginfo("node(%u) connect peer '%s:%d' failed.\n",
                local->id, peer->addr, peer->port);
        return;
    }

    net_client_set_connection_callback(client,
            _RequestVote_invoke, local);
}

void start_election(net_timer_t *election_timer)
{
    struct raft_server *rs = net_timer_data(election_timer);

    rs->state = CANDIDATE; // trigger election

    rs->votes = 1; // vote self
    rs->votedFor = rs->id;
    raft_persist_votedFor(rs);

    rs->currentTerm++;
    raft_persist_currentTerm(rs);

    loginfo("node(%d) convert to CANDIDATE "
            "and start election for term(%d)\n",
            rs->id, rs->currentTerm);

    // RequestVotes RPC
    for (int i = 0; i < rs->cluster->number; i++)
    {
        struct raft_peer *peer = &rs->cluster->peers[i];
        RequestVote_invoke(rs, peer);
    }

    // in case of split votes
    net_timer_reset(election_timer, random_ElecttionTimeout(), 0);
}

void bind_raft_server(net_connect_t *c, void *arg)
{
    struct raft_server *rs = arg;
    c->data = rs;
}

void bind_kv_server(net_connect_t *c, void *arg)
{
    struct kv_server *kvs = arg;
    c->data = kvs;
}

void client_response(void *arg)
{
    struct kv_raft_ctx *ctx = arg;
    net_connect_t *c = ctx->c;
    struct kv_cmd *cmd = ctx->cmd;

    net_buf_t *reply = net_buf_create(0);
    struct kv_server *kvs = c->data;
    switch (cmd->type)
    {
        case 1:
            net_buf_append(reply, "GET key: %s, value: %s",
                    cmd->key, kv_get(kvs, cmd->key));
            break;
        case 2:
            // kv_set(kvs, cmd->key, cmd->value);
            net_buf_append(reply, "SET key: %s, value: %s",
                    cmd->key, cmd->value);
            break;
        case 3:
            net_buf_append(reply, "noop");
            break;
        default:
            logerr("unknown kv cmd type: %d\n", cmd->type);
    }

    if (!c->err)
    {
        list_append(&c->outbuf, &reply->node);
        // NOTE: we don't close connection here, because we are
        // SERVER, we take an PASSIVE behaivor.
        net_connection_send(c);
    }

    free(ctx->cmd->key);
    free(ctx->cmd->value);
    free(ctx->cmd);
    free(ctx);
}

struct raft_log_entry *raft_fill_log_entry(struct raft_server *rs,
        char *start, size_t size)
{
    struct raft_log_entry *entry = &rs->entries[rs->lastLogIndex];

    entry->term = rs->currentTerm;
    entry->cmd.len = size;
    entry->cmd.buf = malloc(size);
    memcpy(entry->cmd.buf, start, size);

    return entry;
}

void raft_commit_callback(struct raft_log_entry *e,
        commit_handler cb, void *arg)
{
    e->cb_handle = cb;
    e->cb_arg = arg;
}

int client_request(char *start, size_t size, net_connect_t *c)
{
    struct kv_server *kvs = c->data;
    struct raft_server *rs = kvs->rs;

    struct kv_raft_ctx *ctx = malloc(sizeof(struct kv_raft_ctx));
    ctx->c = c;
    ctx->cmd = parse_cmd((uint8_t*)start);
    // TODO: log command content here instead of within parse_cmd()

    struct raft_log_entry *e = raft_fill_log_entry(rs, start, size);
    raft_log_replication(rs, e);
    raft_commit_callback(e, client_response, ctx);

    return size;
}

void raft_server_stop(net_loop_t *loop, void *arg)
{
    struct kv_server *kvs = arg;
    hashDestroy(kvs->map);
    free(kvs);

    struct raft_server *rs = kvs->rs;
    free(rs->cluster->peers);
    free(rs->cluster);
    free(rs->nextIndex);
    free(rs->matchIndex);
    free(rs);
}

int main(int argc, char *argv[])
{
    if (argc != 3)
    {
        printf("usage: %s node_id, node_amount\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    net_log_level(LOG_INFO);

    int node_id = strtol(argv[1], NULL, 10);
    int node_amount = strtol(argv[2], NULL, 10);

    net_loop_t *loop = net_loop_init(EPOLL_SIZE);
    net_server_t *server = net_server_init(loop, "0.0.0.0", 7777 + node_id);
    net_server_set_message_callback(server, peer_rpc);
    net_server_t *app_server = net_server_init(loop, "0.0.0.0", 8888 + node_id);
    net_server_set_message_callback(app_server, client_request);

    struct raft_server *rs = malloc(sizeof(struct raft_server));
    rs->tcp_server = server;
    rs->votes = 0;
    rs->id = node_id;
    rs->state = FOLLOWER;
    rs->commitIndex = 0;
    rs->lastApplied = 0;
    rs->prevLogIndex = 0;
    rs->prevLogTerm = 0;

    char path[1024];
    sprintf(path, "replicated-%d.log", node_id);
    raft_restore_log(rs, path);

    loginfo("raft node startup: state(%s), node_id(%d), node_amount(%d)\n",
            raft_state(rs->state), node_id, node_amount);

    // so we can get @rs within every incoming connection
    net_server_set_accept_callback(server, bind_raft_server, rs);

    struct kv_server *kvs = malloc(sizeof(struct kv_server));
    kvs->rs = rs;
    kvs->map = hashInit(1024);
    net_server_set_accept_callback(app_server, bind_kv_server, kvs);
    rs->st = kvs;

    // construct raft cluster
    struct raft_cluster *rc = malloc(sizeof(struct raft_cluster));
    rc->number = node_amount;

    rc->peers = malloc(sizeof(struct raft_peer) * rc->number);
    for (int i = 0; i < rc->number; i++)
    {
        rc->peers[i].addr = "0.0.0.0";
        rc->peers[i].port = 7777 + i;
    }
    rs->cluster = rc;

    rs->nextIndex = malloc(sizeof(int) * rs->cluster->number);
    raft_reset_nextIndex(rs);
    rs->matchIndex = malloc(sizeof(int) * rs->cluster->number);
    raft_reset_matchIndex(rs);

    rs->inFlight = calloc(rs->cluster->number, sizeof(int));

    srandom(time(NULL) + node_id);
    net_timer_t *timer = net_timer_init(loop, random_ElecttionTimeout(), 0);
    net_timer_start(timer, start_election, rs);
    rs->election_timer = timer;
    rs->heartbeat_timer = NULL;

    net_loop_set_stop_callback(loop, raft_server_stop, kvs);
    net_loop_start(loop);
}
