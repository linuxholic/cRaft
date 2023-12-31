#include <time.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <fcntl.h>
#include <sys/stat.h>
#include <string.h>
#include <errno.h>

#include "raft.h"
#include "raft-log.h"

struct kv_raft_ctx
{
    struct net_connect_t *c;
    struct kv_cmd *cmd;
};

struct kv_server
{
    struct hashTable *map;
    struct raft_server *rs;
    char *snapshot_path;
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

/*
 * buckets_count | keys_count
 * key_size | key | value_size | value
 * key_size | key | value_size | value
 * key_size | key | value_size | value
 */
void _kv_snapshot_store(struct kv_server *kvs, int fd)
{
    int buckets_count = kvs->map->size;
    write(fd, &buckets_count, 4);

    off_t pin = lseek(fd, 0, SEEK_CUR);
    int keys_count = 0;
    write(fd, &keys_count, 4);

    for (int i = 0; i < buckets_count; i++)
    {
        struct hashItem *item = kvs->map->arr[i];
        while (item)
        {
            char *key_str = item->key._str;
            int key_size = strlen(key_str);
            char *value_str = (char*)item->value;
            int value_size = strlen(value_str);
            write(fd, &key_size, 4);
            write(fd, key_str, key_size);
            write(fd, &value_size, 4);
            write(fd, value_str, value_size);

            keys_count++;
            item = item->next;
        }
    }

    // backpatching
    lseek(fd, pin, SEEK_SET);
    write(fd, &keys_count, 4);
}

void kv_snapshot_store(struct kv_server *kvs)
{
    struct stat buffer;
    char *path = kvs->snapshot_path;
    if (stat(path, &buffer) == 0)
    {
        remove(path);
        logerr("remove old snapshot file.\n");
    }
    else {
        if (errno == ENOENT)
        {
            logerr("create snapshot file.\n");
        }
        else {
            logerr("stat snapshot file '%s' error: %s\n",
                    path, strerror(errno));
            return;
        }
    }

    int fd = open(path, O_CREAT | O_RDWR, S_IRUSR | S_IWUSR | S_IROTH);
    if (fd < 0)
    {
        logerr("failed to open snapshot file '%s': %s\n",
                path, strerror(errno));
        return;
    }

    _kv_snapshot_store(kvs, fd);
    close(fd);
}

void kv_snapshot_replace(struct kv_server *kvs, char *tmp)
{
    struct stat buffer;
    char *path = kvs->snapshot_path;
    if (stat(path, &buffer) == 0)
    {
        remove(path);
        loginfo("kv_snapshot_replace: discard existing snapshot.\n");
    }
    else {
        if (errno == ENOENT)
        {
            loginfo("kv_snapshot_replace: no snapshot.\n");
        }
        else {
            logerr("stat snapshot file '%s' error: %s\n",
                    path, strerror(errno));
            return;
        }
    }

    rename(tmp, kvs->snapshot_path);
    loginfo("kv_snapshot_replace: rename '%s' to '%s'.\n",
            tmp, kvs->snapshot_path);
}

void _kv_snapshot_load(struct kv_server *kvs, int fd)
{
    int buckets_count;
    read(fd, &buckets_count, 4);

    if (kvs->map) hashDestroy(kvs->map);
    kvs->map = hashInit(buckets_count);

    int keys_count;
    read(fd, &keys_count, 4);
    for (int i = 0; i < keys_count; i++)
    {
        int key_size;
        read(fd, &key_size, 4);
        char *key = malloc(key_size + 1);
        read(fd, key, key_size);
        key[key_size] = '\0';

        int value_size;
        read(fd, &value_size, 4);
        char *value = malloc(value_size + 1);
        read(fd, value, value_size);
        value[value_size] = '\0';

        hashPut(kvs->map, key, value);
    }
}

int kv_snapshot_load(struct kv_server *kvs)
{
    struct stat buffer;
    char *path = kvs->snapshot_path;
    if (stat(path, &buffer) == 0)
    {
        loginfo("loading snapshot file...\n");
    }
    else {
        if (errno == ENOENT)
        {
            loginfo("no kv snapshot file, skip loading.\n");
        }
        else {
            logerr("stat snapshot file '%s' error: %s\n",
                    path, strerror(errno));
        }
        return 1;
    }

    int fd = open(path, O_RDONLY);
    if (fd < 0)
    {
        logerr("failed to open snapshot file '%s': %s\n",
                path, strerror(errno));
        return 1;
    }

    _kv_snapshot_load(kvs, fd);
    close(fd);
    return 0;
}

#define MinimumElecttionTimeout 10 // seconds

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
int random_ElecttionTimeout(int *rnd)
{
    // TODO: align to paper's suggested value
    int timeout = MinimumElecttionTimeout + random() % 10;
    loginfo("random ElecttionTimeout (in seconds): %d\n", timeout);
    *rnd = timeout - MinimumElecttionTimeout;
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

    int remain = net_timer_remain(rs->election_timer);
    if (rs->state == FOLLOWER && remain > rs->election_timer_rnd)
    {
        /*
         * if a server receives a RequestVote request within the minimum
         * election timeout of HEARING FROM A CURRENT LEADER, it does not
         * update its term or grant its vote.
         */
        loginfo("recv RequestVote RPC within MinimumElecttionTimeout(%d), "
                "ignore it.\n", MinimumElecttionTimeout);
        return;
    }

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
                        random_ElecttionTimeout(&rs->election_timer_rnd), 0);
            }
        }
        else {
            _RequestVote_receiver(c, rs->currentTerm, 0);
            loginfo("[%s] for term(%d), node(%d) already voted for node(%d),"
                    " so deny vote to node(%d).\n",
                    raft_state(rs->state), term, rs->id, rs->votedFor,
                    candidateId);
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

// TODO
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
    else {
        logerr("client request unknown cmd type: %d.\n", cmd_type);
    }

    return cmd;
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

    uint8_t *cur = e->cmd.buf;
    cur += sizeof(uint32_t); // skip cmd type

    // TODO: use gcc extentsion __attribute__(cleanup)
    struct kv_cmd *cmd = parse_cmd(cur);
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
        default:
            logerr("log[%d] apply state machine, unknown kv cmd type: %d\n",
                    index, cmd->type);
    }
    free(cmd);
}

void raft_snapshot(struct raft_server *rs)
{
    struct kv_server *kvs = rs->st;

    // TODO: state machine register this as callback into raft
    // rs->cbs.snapshot(rs->st)
    kv_snapshot_store(kvs);
}

void raft_apply_configuration(struct raft_server *rs, uint8_t *buf)
{
    uint8_t *cur = buf;

    /*
     * configuration entry format:
     * addr_num | ip_len | ip_str | port | id
     */
    int addr_num = ntohl(*(uint32_t*)cur);
    cur += sizeof(uint32_t);

    struct raft_cluster *rc = rs->cluster;
    rc->number = addr_num;
    rc->peers = realloc(rc->peers, sizeof(struct raft_peer) * rc->number);

    loginfo("apply raft configuration: addr_num(%d)\n", addr_num);

    for (int i = 0; i < addr_num; i++)
    {
        int ip_len = ntohl(*(uint32_t*)cur);
        cur += sizeof(uint32_t);

        rc->peers[i].addr = strndup((char*)cur, ip_len);
        cur += ip_len;

        int port = ntohl(*(uint32_t*)cur);
        cur += sizeof(uint32_t);

        rc->peers[i].port = port;

        int id = ntohl(*(uint32_t*)cur);
        cur += sizeof(uint32_t);

        rc->peers[i].id = id;

        loginfo("%.*s:%d(%d)\n", ip_len, rc->peers[i].addr,
                rc->peers[i].port, rc->peers[i].id);
    }
}

void raft_apply_membership_change(
        struct raft_server *rs, struct raft_log_entry *e)
{
    if (rs->state == LEADER)
    {
        loginfo("leader skip apply configuration entry\n");
        return;
    }
    raft_apply_configuration(rs,
            e->cmd.buf + sizeof(uint32_t) // skip cmd type
            );
}

void raft_apply(struct raft_server *rs, int index)
{
    struct raft_log_entry *e = &rs->entries[index - 1];

    int type = raft_log_entry_type(e);
    switch (type)
    {
        case RAFT_LOG_STATE_MACHINE:
            raft_apply_state_machine(rs, index);
            raft_snapshot(rs); // save state machine's state
            raft_log_compaction(rs); // save raft's state
            break;
        case RAFT_LOG_NO_OP:
            loginfo("apply no-op log entry.\n");
            break;
        case RAFT_LOG_CONFIGURATION:
            raft_apply_membership_change(rs, e);
            break;
        default:
            logerr("unknown log entry cmd type: %d\n", type);
    }
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
                rs->state = FOLLOWER;
                loginfo("raft candidate: convert to follower.\n");
            }
            else {
                // normal log replication
            }
        }

        // consistency check
        if (rs->lastLogIndex >= prevLogIndex
                && rs->discard_index <= prevLogIndex)
        {
            if (prevLogIndex == rs->discard_index
                    && prevLogTerm == rs->discard_term)
            {
                raft_log_delete_suffix(rs, prevLogIndex + 1);
            }
            else if (rs->entries[prevLogIndex - 1].term == prevLogTerm)
            {
                raft_log_delete_suffix(rs, prevLogIndex + 1);
            }
            else {
                loginfo("log term not match\n");
                _AppendEntries_receiver(c, rs->currentTerm, 0);
                net_timer_reset(rs->election_timer,
                        random_ElecttionTimeout(&rs->election_timer_rnd), 0);
                return;
            }
        }
        else {
            loginfo("log index not match: "
                    "lastIndex(%d), prevLogIndex(%d), lastLogIndex(%d).\n",
                    rs->discard_index, prevLogIndex, rs->lastLogIndex);

            _AppendEntries_receiver(c, rs->currentTerm, 0);
            net_timer_reset(rs->election_timer,
                    random_ElecttionTimeout(&rs->election_timer_rnd), 0);
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

            raft_persist_log_entry(rs, entry);
        }

        _AppendEntries_receiver(c, rs->currentTerm, 1);
        net_timer_reset(rs->election_timer,
                random_ElecttionTimeout(&rs->election_timer_rnd), 0);
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
        raft_apply(rs, rs->lastApplied);
    }
}

struct raft_log_entry *raft_fill_log_entry(struct raft_server *rs,
        char *start, size_t size)
{
    struct raft_log_entry *entry = &rs->entries[rs->lastLogIndex];

    entry->term = rs->currentTerm;
    entry->cmd.len = size + sizeof(uint32_t);
    entry->cmd.buf = malloc(entry->cmd.len);

    // fill command buffer
    uint32_t cmd_type = htonl(1); // per state machine
    memcpy(entry->cmd.buf, &cmd_type, sizeof(uint32_t));
    memcpy(entry->cmd.buf + sizeof(uint32_t), start, size);

    return entry;
}

struct raft_log_entry *raft_fill_configuration_entry(struct raft_server *rs,
        char *start, size_t size)
{
    struct raft_log_entry *entry = &rs->entries[rs->lastLogIndex];

    entry->term = rs->currentTerm;
    entry->cmd.len = size;
    entry->cmd.buf = malloc(entry->cmd.len);
    memcpy(entry->cmd.buf, start, size);

    return entry;
}

void raft_on_commit(struct raft_server *rs, int index)
{
    struct raft_log_entry *e = &rs->entries[index - 1];
    if (e->cb_handle)
    {
        loginfo("log entry commited, trigger callback.\n");

        rs->lastApplied = index;
        e->cb_handle(e->cb_arg);
        e->cb_handle = NULL;

        // TODO: implement keep-alive connection in libnet and
        // close idle connection when idle timer expires.
        e->cb_arg = NULL;
    }
    else {
        loginfo("log entry commited, NO callback.\n");
    }
}

void raft_try_commit(struct raft_server *rs, int index)
{
    int majority = 0;
    for (int i = 0; i < rs->cluster->number; i++)
    {
        int matchIndex = rs->cluster->peers[i].matchIndex;
        loginfo("matchIndex[%d]: %d,  try commit index: %d.\n",
                i, matchIndex, index);

        if (matchIndex >= index)
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
        // one by one, especially when new leader come to
        // power, so we need a loop to catch up.
        while (rs->lastApplied < rs->commitIndex)
        {
            loginfo("[%s] increment lastApplied(%d -> %d).\n",
                    raft_state(rs->state),
                    rs->lastApplied, rs->lastApplied + 1);

            rs->lastApplied++;
            raft_apply(rs, rs->lastApplied);

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

static void _load_file(net_buf_t *reply, char *path)
{
    FILE *f = fopen(path, "r");
    struct stat stat_file;
    fstat(fileno(f), &stat_file);
    uint8_t *cur = malloc(sizeof(uint8_t) * stat_file.st_size);
    fread(cur, stat_file.st_size, 1, f);
    fclose(f);

    uint32_t size = htonl(stat_file.st_size);
    net_buf_copy(reply, (char*)&size, sizeof(uint32_t));
    net_buf_copy(reply, (char*)cur, stat_file.st_size);
    free(cur);
}

struct raft_peer* raft_get_peer(struct raft_cluster* rc, char *addr, int port)
{
    struct raft_peer *peer = NULL;
    for (int i = 0; i < rc->number; i++)
    {
        peer = &rc->peers[i];
        if (peer->port == port
            && strcmp(peer->addr, addr) == 0)
        {
            break;
        }
    }
    return peer;
}

int _InstallSnapshot_invoke(char *start, size_t size, net_connect_t *c)
{
    // partial results
    if (size < 4) return 0;

    // decode results
    uint32_t *res = (uint32_t *)start;
    uint32_t term    = ntohl(res[0]);

    struct raft_server *rs = c->client->user_data;

    loginfo("[%s] InstallSnapshot_invoke recv response: term(%u).\n",
            raft_state(rs->state), term);

    struct raft_peer *peer = raft_get_peer(rs->cluster,
                             c->client->peer_host, c->client->peer_port);

    // whenever see larger term, update local currentTerm
    if (term > rs->currentTerm)
    {
        // pause heartbeat to peers
        net_timer_reset(rs->heartbeat_timer, 0, 0);

        loginfo("[%s] node(%d) update term: %d -> %d.\n",
                raft_state(rs->state), rs->id, rs->currentTerm, term);

        rs->currentTerm = term;
        raft_persist_currentTerm(rs);
        rs->votedFor = -1;
        raft_persist_votedFor(rs);

        rs->state = FOLLOWER;
        loginfo("raft leader: convert to follower.\n");
        net_timer_reset(rs->election_timer,
                random_ElecttionTimeout(&rs->election_timer_rnd), 0);
    }
    else {
        // TODO: rs->discard_index may has changed
        peer->nextIndex = rs->discard_index + 1;
        peer->matchIndex = peer->nextIndex - 1;
    }

    net_connection_set_close(c); // non-keepalive
    return size;
}

void InstallSnapshot_invoke(struct raft_server *rs, net_connect_t *peer_c)
{
    loginfo("send snapshot to peer.\n");
    net_buf_t *reply = net_buf_create(0);

    uint32_t rpc_type = htonl(INSTALL_SNAPSHOT);
    net_buf_copy(reply, (char*)&rpc_type, sizeof(uint32_t));

    uint32_t term = htonl(rs->currentTerm);
    net_buf_copy(reply, (char*)&term, sizeof(uint32_t));

    uint32_t leaderId = htonl(rs->id);
    net_buf_copy(reply, (char*)&leaderId, sizeof(uint32_t));

    uint32_t lastIndex = htonl(rs->discard_index);
    net_buf_copy(reply, (char*)&lastIndex, sizeof(uint32_t));

    uint32_t lastTerm = htonl(rs->discard_term);
    net_buf_copy(reply, (char*)&lastTerm, sizeof(uint32_t));

    // load raft cluster membership configuration into @reply
    _load_file(reply, rs->configuration_path);

    uint32_t offset = htonl(0);
    net_buf_copy(reply, (char*)&offset, sizeof(uint32_t));

    // load state machine snapshot data into @reply
    struct kv_server *kvs = rs->st;
    _load_file(reply, kvs->snapshot_path);

    uint32_t done = htonl(1);

    // send to wire
    list_append(&peer_c->outbuf, &reply->node);
    net_connection_send(peer_c);

    net_client_set_response_callback(peer_c->client,
            _InstallSnapshot_invoke);
    net_client_set_user_data(peer_c->client, rs);
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

    struct raft_peer *peer = raft_get_peer(rs->cluster,
                             c->client->peer_host, c->client->peer_port);

    if (success)
    {
        if (peer->inFlight > 0)
        {
            peer->nextIndex += peer->inFlight;
            peer->matchIndex = peer->nextIndex - 1;
            raft_commit(rs);
        }
    }
    else {
        // whenever see larger term, update local currentTerm
        if (term > rs->currentTerm)
        {
            // pause heartbeat to peers
            net_timer_reset(rs->heartbeat_timer, 0, 0);

            loginfo("[%s] node(%d) update term: %d -> %d.\n",
                    raft_state(rs->state), rs->id, rs->currentTerm, term);

            rs->currentTerm = term;
            raft_persist_currentTerm(rs);
            rs->votedFor = -1;
            raft_persist_votedFor(rs);

            rs->state = FOLLOWER;
            loginfo("raft leader: convert to follower.\n");
            net_timer_reset(rs->election_timer,
                    random_ElecttionTimeout(&rs->election_timer_rnd), 0);
        }
        else {
            // log consistency check failed, so decrease nextIndex
            peer->nextIndex--;
        }
    }

    net_connection_set_close(c); // non-keepalive
    return size;
}

void __AppendEntries_invoke(net_connect_t *c, void *arg)
{
    struct raft_server *rs = arg;
    struct raft_peer *peer = raft_get_peer(rs->cluster,
                             c->client->peer_host, c->client->peer_port);
    if (c->err)
    {
        loginfo("[%s] send AppendEntries RPC to node(%d): "
                "failed to connect.\n",
                raft_state(rs->state), peer->id);
        return;
    }

    net_buf_t *reply = net_buf_create(0);

    uint32_t rpc_type = htonl(APPEND_ENTRIES);
    net_buf_copy(reply, (char*)&rpc_type, sizeof(uint32_t));

    uint32_t term = htonl(rs->currentTerm);
    net_buf_copy(reply, (char*)&term, sizeof(uint32_t));

    uint32_t leaderId = htonl(rs->id);
    net_buf_copy(reply, (char*)&leaderId, sizeof(uint32_t));

    uint32_t _prevLogIndex = peer->nextIndex - 1;
    if (_prevLogIndex < rs->discard_index)
    {
        loginfo("peer nextIndex(%d) is already discarded(%d) by leader.\n",
                _prevLogIndex, rs->discard_index);
        InstallSnapshot_invoke(rs, c);
        return;
    }

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
        if (_prevLogIndex == rs->discard_index)
        {
            _prevLogTerm = rs->discard_term;
        }
        else {
            _prevLogTerm = rs->entries[_prevLogIndex - 1].term;
        }
        uint32_t prevLogTerm = htonl(_prevLogTerm);
        net_buf_copy(reply, (char*)&prevLogTerm, sizeof(uint32_t));
    }

    // TODO: multi log entries
    struct raft_log_entry *entries = c->client->user_data;
    if (entries)
    {
        peer->inFlight = 1;
    }
    else {
        if (_prevLogIndex < rs->lastLogIndex) // lagging follower
        {
            entries = &rs->entries[_prevLogIndex];
            peer->inFlight = 1;
        }
        else { // heartbeat
            peer->inFlight = 0;
        }
    }

    uint32_t _num = htonl(peer->inFlight);
    net_buf_copy(reply, (char*)&_num, sizeof(uint32_t));

    for (int i = 0; i < peer->inFlight; i++)
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
            _prevLogTerm, peer->inFlight, rs->commitIndex);

    // send to wire
    list_append(&c->outbuf, &reply->node);
    net_connection_send(c);

    net_client_set_response_callback(c->client, ___AppendEntries_invoke);
    net_client_set_user_data(c->client, rs);
}

void raft_commit_callback(struct raft_log_entry *e,
        commit_handler cb, void *arg)
{
    e->cb_handle = cb;
    e->cb_arg = arg;
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

/*
 * 1. write local log
 * 2. replicate log entry to raft peers
 */
void raft_log_replication(struct raft_server *rs,
        struct raft_log_entry *entry)
{
    raft_persist_log_entry(rs, entry);

    struct raft_peer *self = raft_get_peer(rs->cluster,
            rs->tcp_server->local_host, rs->tcp_server->local_port);
    self->nextIndex++;
    self->matchIndex = rs->lastLogIndex;

    // replicate log entry to peers
    for (int i = 0; i < rs->cluster->number; i++)
    {
        struct raft_peer *peer = &rs->cluster->peers[i];
        _AppendEntries_invoke(rs, peer, entry);
    }

    // raft cluster containing only one node, which is often the case
    // when bootstrap a whole new raft cluster.
    if (rs->cluster->number == 1)
    {
        raft_commit(rs);
    }

    net_timer_reset(rs->heartbeat_timer, 2, 2);
}

void AddServer_response(void *arg)
{
    net_connect_t *c = arg;
    if (c->err) return;

    net_buf_t *reply = net_buf_create(0);
    net_buf_append(reply, "OK");

    list_append(&c->outbuf, &reply->node);
    net_connection_send(c);
}

void AddServer_receiver(net_connect_t *c, uint32_t *res)
{
    uint8_t *cur = (uint8_t*)(res + 1); // skip raft rpc type
    struct raft_server *rs = c->data;

    uint32_t ip_len = ntohl(*(uint32_t*)cur);
    cur += sizeof(uint32_t);

    char *server_ip = strndup((char*)cur, ip_len);
    cur += ip_len;

    uint32_t server_port = ntohl(*(uint32_t*)cur);
    cur += sizeof(uint32_t);

    uint32_t server_id = ntohl(*(uint32_t*)cur);
    cur += sizeof(uint32_t);

    loginfo("[%s] recv AddServer RPC: %.*s:%d(%d)\n",
            raft_state(rs->state),
            ip_len, server_ip, server_port, server_id);

    /*
     * TODO:
     * 1. Reply NOT_LEADER if not leader
     * 2. Catch up new server
     *   2.1 Reply TIMEOUT if new server
     *     * does not make progress for an election timeout
     *     * the last round takes longer than the election timeout
     * 3. Wait until previous configuration entry is commited
     */

    /*
     * 1. rebuild raft cluster
     * 2. rebuild nextIndex[]
     * 3. rebuild matchIndex[]
     * 4. rebuild inFlight[]
     */

    struct raft_cluster *rc = rs->cluster;
    rc->number++;
    rc->peers = realloc(rc->peers, sizeof(struct raft_peer) * rc->number);
    rc->peers[rc->number - 1].addr = server_ip;
    rc->peers[rc->number - 1].port = server_port;
    rc->peers[rc->number - 1].id = server_id;
    rc->peers[rc->number - 1].nextIndex = rs->lastLogIndex + 1;
    rc->peers[rc->number - 1].matchIndex = 0;
    rc->peers[rc->number - 1].inFlight = 0;

    /*
     * fill command buffer (encoding) and refer to
     * raft_apply_membership_change for decoding
     */

    int buf_len = (sizeof(uint32_t)    // cmd type
                 + sizeof(uint32_t));; // addr num
    void *buf = malloc(buf_len);

    int cmd_type = htonl(3); // per membership change
    memcpy(buf, &cmd_type, sizeof(uint32_t));

    int addr_num = htonl(rc->number);
    memcpy(buf + sizeof(uint32_t), &addr_num, sizeof(uint32_t));

    for (int i = 0; i < rc->number; i++)
    {
        int prev_len = buf_len;

        ip_len = strlen(rc->peers[i].addr);
        buf_len += sizeof(uint32_t)    // ip len
                   + ip_len            // ip
                   + sizeof(uint32_t)  // port
                   + sizeof(uint32_t); // id

        buf = realloc(buf, buf_len);
        cur = buf + prev_len;

        uint32_t _ip_len = htonl(ip_len);
        memcpy(cur, &_ip_len, sizeof(uint32_t));
        cur += sizeof(uint32_t);

        memcpy(cur, rc->peers[i].addr, ip_len);
        cur += ip_len;

        server_port = htonl(rc->peers[i].port);
        memcpy(cur, &server_port, sizeof(uint32_t));
        cur += sizeof(uint32_t);

        server_id = htonl(rc->peers[i].id);
        memcpy(cur, &server_id, sizeof(uint32_t));
        cur += sizeof(uint32_t);
    }

    struct raft_log_entry *e = raft_fill_configuration_entry(rs, buf, buf_len);
    raft_log_replication(rs, e);
    raft_commit_callback(e, AddServer_response, c);
}

void RemoveServer_receiver(net_connect_t *c, uint32_t *res)
{
    uint8_t *cur = (uint8_t*)(res + 1); // skip raft rpc type
    struct raft_server *rs = c->data;

    uint32_t ip_len = ntohl(*(uint32_t*)cur);
    cur += sizeof(uint32_t);

    char *server_ip = strndup((char*)cur, ip_len);
    cur += ip_len;

    uint32_t server_port = ntohl(*(uint32_t*)cur);
    cur += sizeof(uint32_t);

    uint32_t server_id = ntohl(*(uint32_t*)cur);
    cur += sizeof(uint32_t);

    loginfo("[%s] recv RemoveServer RPC: %.*s:%d(%d)\n",
            raft_state(rs->state),
            ip_len, server_ip, server_port, server_id);

    /*
     * TODO:
     * 1. Reply NOT_LEADER if not leader
     * 2. Wait until previous configuration entry is commited
     */

    /*
     * 1. rebuild raft cluster
     * 2. rebuild nextIndex[]
     * 3. rebuild matchIndex[]
     * 4. rebuild inFlight[]
     */

    struct raft_cluster *rc = rs->cluster;
    for (int i = 0; i < rc->number; i++)
    {
        if (rc->peers[i].id == server_id)
        {
            free(rc->peers[i].addr);
            memcpy(&rc->peers[i],
                   &rc->peers[rc->number - 1],
                   sizeof(struct raft_peer));
            break;
        }
    }
    rc->number--;
    rc->peers = realloc(rc->peers, sizeof(struct raft_peer) * rc->number);

    /*
     * fill command buffer (encoding) and refer to
     * raft_apply_membership_change for decoding
     */

    int buf_len = (sizeof(uint32_t)    // cmd type
                 + sizeof(uint32_t));; // addr num
    void *buf = malloc(buf_len);

    int cmd_type = htonl(3); // per membership change
    memcpy(buf, &cmd_type, sizeof(uint32_t));

    int addr_num = htonl(rc->number);
    memcpy(buf + sizeof(uint32_t), &addr_num, sizeof(uint32_t));

    for (int i = 0; i < rc->number; i++)
    {
        int prev_len = buf_len;

        ip_len = strlen(rc->peers[i].addr);
        buf_len += sizeof(uint32_t)    // ip len
                   + ip_len            // ip
                   + sizeof(uint32_t)  // port
                   + sizeof(uint32_t); // id

        buf = realloc(buf, buf_len);
        cur = buf + prev_len;

        uint32_t _ip_len = htonl(ip_len);
        memcpy(cur, &_ip_len, sizeof(uint32_t));
        cur += sizeof(uint32_t);

        memcpy(cur, rc->peers[i].addr, ip_len);
        cur += ip_len;

        server_port = htonl(rc->peers[i].port);
        memcpy(cur, &server_port, sizeof(uint32_t));
        cur += sizeof(uint32_t);

        server_id = htonl(rc->peers[i].id);
        memcpy(cur, &server_id, sizeof(uint32_t));
        cur += sizeof(uint32_t);
    }

    struct raft_log_entry *e = raft_fill_configuration_entry(rs, buf, buf_len);
    raft_log_replication(rs, e);
    raft_commit_callback(e, AddServer_response, c);
}

void _InstallSnapshot_receiver(net_connect_t *c, int term)
{
    net_buf_t *reply = net_buf_create(0);

    int _term = htonl(term);
    net_buf_copy(reply, (char*)&_term, sizeof(int));

    loginfo("InstallSnapshot result: term(%u).\n", term);

    list_append(&c->outbuf, &reply->node);
    net_connection_send(c);
}

void raft_update_term(struct raft_server *rs, int term)
{
    loginfo("[%s] node(%d) update term: %d -> %d.\n",
            raft_state(rs->state), rs->id, rs->currentTerm, term);

    rs->currentTerm = term;
    raft_persist_currentTerm(rs);
    rs->votedFor = -1;
    raft_persist_votedFor(rs);
}

char *raft_save_tmp_snapshot(uint8_t *buf, int size)
{
    static char tmp_snapshot_path[] = "snapshot.StateMachine.tmp.XXXXXX";
    int fd = mkstemp(tmp_snapshot_path);
    write(fd, buf, size);
    fdatasync(fd);
    close(fd);
    return tmp_snapshot_path;
}

/* caller should guarantee that @idx > rs->discard_index */
void raft_free_log_entries(struct raft_server *rs, int idx)
{
    int iter = idx;
    while (iter > rs->discard_index)
    {
        free(rs->entries[iter - 1].cmd.buf);
        rs->entries[iter - 1].cmd.buf = NULL;
        rs->entries[iter - 1].cmd.len = 0;
        iter--;
    }
}

void InstallSnapshot_receiver(net_connect_t *c, uint32_t *res)
{
    uint8_t *cur = (uint8_t*)(res + 1); // skip raft rpc type
    struct raft_server *rs = c->data;

    uint32_t term      = ntohl(*(uint32_t*)cur); cur += sizeof(uint32_t);
    uint32_t leaderId  = ntohl(*(uint32_t*)cur); cur += sizeof(uint32_t);
    uint32_t lastIndex = ntohl(*(uint32_t*)cur); cur += sizeof(uint32_t);
    uint32_t lastTerm  = ntohl(*(uint32_t*)cur); cur += sizeof(uint32_t);

    loginfo("[%s] recv InstallSnapshot RPC: term(%u), "
            "leaderId(%u), lastIndex(%u), lastTerm(%u)\n",
            raft_state(rs->state), term,
            leaderId, lastIndex, lastTerm);

    if (rs->currentTerm > term)
    {
        _InstallSnapshot_receiver(c, rs->currentTerm);
        loginfo("recv RequestVote RPC with lower term(%d,%d), ignore it.\n",
                rs->currentTerm, term);
        return;
    }
    else {
        // whenever see larger term, update local currentTerm
        if (term > rs->currentTerm)
        {
            raft_update_term(rs, term);

            // whenever see larger term, convert to FOLLOWER
            if (rs->state != FOLLOWER)
            {
                loginfo("node(%d) convert state: %s -> %s.\n", rs->id,
                        raft_state(rs->state), raft_state(FOLLOWER));
                rs->state = FOLLOWER;
            }
        }

        // receive snapshot
        //   snapshot.StateMachine.ID
        //   snapshot.RaftConfiguration.ID
        //   snapshot.RaftLogDiscardedIndexAndTerm.ID

        uint32_t size_rcc = ntohl(*(uint32_t*)cur);
        cur += sizeof(uint32_t);

        // buffer for raft cluster configuration
        uint8_t *buf_rcc = cur;
        cur += size_rcc;

        uint32_t offset = ntohl(*(uint32_t*)cur);
        cur += sizeof(uint32_t);

        // buffer for state machine snapshot
        uint32_t size_kvs = ntohl(*(uint32_t*)cur);
        cur += sizeof(uint32_t);
        char *tmp_path = raft_save_tmp_snapshot(cur, size_kvs);
        cur += size_kvs;

        uint32_t done = ntohl(*(uint32_t*)cur);
        cur += sizeof(uint32_t);

        /* if lastIndex is larger than latest snapshot's, save snapshot
         * file and Raft state (lastIndex, lastTerm, lastConfig).
         * Discard any existing or partial snapshot. */
        if (lastIndex > rs->discard_index)
        {
            // TODO: register this to raft as callback
            kv_snapshot_replace(rs->st, tmp_path);
            remove(tmp_path);

            // overwrite previous raft cluster configuration
            raft_persist_configuration(rs, buf_rcc, size_rcc);

            loginfo("save snapshot file and Raft state.\n");

            // overwrite previous discard_index and term
            rs->discard_index = lastIndex;
            raft_persist_lastIndex(rs);
            rs->discard_term = lastTerm;
            raft_persist_lastTerm(rs);

            /* if existing log entry has same index and term as lastIndex
             * and lastTerm, discard log up through lastIndex(but retain
             * any following entries) and reply. */
            if (lastIndex <= rs->lastLogIndex
                    && rs->entries[lastIndex - 1].term == lastTerm)
            {
                // TODO: merge these two functions into one ??
                raft_log_delete_prefix(rs, lastIndex);
                raft_free_log_entries(rs, lastIndex);

                // reply to peer
                _InstallSnapshot_receiver(c, rs->currentTerm);
                loginfo("discard log up through lastIndex(%d).\n", lastIndex);
                return;
            }

            /* discard the entire log */
            raft_log_delete_prefix(rs, rs->lastLogIndex);
            raft_free_log_entries(rs, rs->lastLogIndex);
            loginfo("discard the entire log.\n");
            rs->lastLogIndex = lastIndex;
            rs->lastLogTerm = lastTerm;

            /* reset state machine using snapshot contents
             * (and load lastConfig as cluster configuration) */
            kv_snapshot_load(rs->st);
            raft_apply_configuration(rs,
                    buf_rcc + sizeof(uint32_t) // skip cmd type
                    );
            rs->commitIndex = lastIndex;
            rs->lastApplied = lastIndex;
        }
        else {
            loginfo("lastIndex(%d) is less than or equal to "
                    "lastest snapshot's(%d), ignore it.\n",
                    lastIndex, rs->discard_index);
        }

        // reply to leader
        _InstallSnapshot_receiver(c, rs->currentTerm);
        net_timer_reset(rs->election_timer,
                random_ElecttionTimeout(&rs->election_timer_rnd), 0);
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
            if (size < 28) // TODO: multiple log entries
            {
                loginfo("AppendEntries RPC: partial results.\n");
                return 0;
            }
            AppendEntries_receiver(c, res);
            break;

        case INSTALL_SNAPSHOT:
            InstallSnapshot_receiver(c, res);
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
            if (size < 28) // TODO: multiple log entries
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
            // if there is a STABLE leader arcoss the whole
            // cluster, then RequestVote with bigger term should
            // be ignored, which is also the situation when changing
            // memebers, say, remove one from cluster.
            loginfo("raft leader recv RequestVote RPC, ignore it.\n");
            break;

        case APPEND_ENTRIES:
            if (size < 28) // TODO: multiple log entries
            {
                loginfo("AppendEntries RPC: partial results.\n");
                return 0;
            }
            AppendEntries_receiver(c, res);
            break;

        case ADD_SERVER:
            if (size < 25)
            {
                loginfo("AddServer RPC: partial results.\n");
                return 0;
            }
            AddServer_receiver(c, res);
            break;

        case REMOVE_SERVER:
            if (size < 25)
            {
                loginfo("RemoveServer RPC: partial results.\n");
                return 0;
            }
            RemoveServer_receiver(c, res);
            break;


        default:
            logerr("raft leader recv unknown rpc type: %u\n", rpc_type);
            break;
    }

    return size;
}

int raft_rpc_receiver(char *start, size_t size, net_connect_t *c)
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
        rs->cluster->peers[i].nextIndex = rs->lastLogIndex + 1;
    }
}

void raft_reset_matchIndex(struct raft_server *rs)
{
    for (int i = 0; i < rs->cluster->number; i++)
    {
        if (i == rs->id)
        {
            rs->cluster->peers[i].matchIndex = rs->lastLogIndex;
        }
        else {
            rs->cluster->peers[i].matchIndex = 0;
        }
    }
}

// TODO: this should be registered by state
// machine, then called by raft when needed.
struct raft_log_entry *raft_noop_log(struct raft_server *rs)
{
    struct raft_log_entry *e = &rs->entries[rs->lastLogIndex];

    e->term = rs->currentTerm;
    e->cmd.len = sizeof(uint32_t);
    e->cmd.buf = malloc(e->cmd.len);

    // fill command buffer
    uint32_t cmd_type = htonl(2); // per no-op
    memcpy(e->cmd.buf, &cmd_type, e->cmd.len);

    return e;
}

void raft_become_leader(struct raft_server *rs)
{
    rs->state = LEADER;

    net_timer_reset(rs->election_timer, 0, 0); // pause
    raft_reset_nextIndex(rs);
    raft_reset_matchIndex(rs);

    // setup periodical heartbeat to peers
    raft_leader_heartbeat(rs->tcp_server->loop, rs);

    // commit no-op log entry, and as a side effect, this will
    // also prevent other peers from starting new election.
    struct raft_log_entry *e = raft_noop_log(rs);
    raft_log_replication(rs, e);
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
            loginfo("node(%d) recv majority votes, "
                    "convert to LEADER.\n", rs->id);
            raft_become_leader(rs);
        }
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

            rs->state = FOLLOWER;
            loginfo("raft candidate: convert to follower.\n");
            net_timer_reset(rs->election_timer,
                    random_ElecttionTimeout(&rs->election_timer_rnd), 0);
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
    net_timer_reset(election_timer,
            random_ElecttionTimeout(&rs->election_timer_rnd), 0);
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

void raft_free_cluster(struct raft_cluster *rc)
{
    // To be honest, I kinda miss the destructor function in C++.
    for (int i = 0; i < rc->number; i++)
    {
        free(rc->peers[i].addr);
    }
    free(rc->peers);
    free(rc);
}

void raft_server_stop(net_loop_t *loop, void *arg)
{
    struct kv_server *kvs = arg;
    hashDestroy(kvs->map);
    free(kvs);

    struct raft_server *rs = kvs->rs;
    raft_free_cluster(rs->cluster);
    fclose(rs->log_handler);
    free(rs);
}

struct raft_server *raft_start_node(struct net_loop_t *loop,
        int node_id, char*init)
{
    net_server_t *server =
        net_server_init(loop, "127.0.0.1", 7777 + node_id);
    net_server_set_message_callback(server, raft_rpc_receiver);

    struct raft_server *rs = malloc(sizeof(struct raft_server));
    rs->tcp_server = server;
    rs->votes = 0;
    rs->id = node_id;
    rs->state = FOLLOWER;

    char path[1024];
    sprintf(path, "replicated-%d.log", node_id);
    if (init)
    {
        // self form majority, so write local log
        // also means commited.
        rs->commitIndex = 1;
        raft_init(path);
    }
    raft_log_restore(rs, path);

    rs->configuration_path = malloc(1024);
    sprintf(rs->configuration_path,
            "snapshot.RaftConfiguration.%d", node_id);

    // so we can get @rs within every incoming connection
    net_server_set_accept_callback(server, bind_raft_server, rs);

    rs->cluster = calloc(1, sizeof(struct raft_cluster));
    if (init)
    {
        // load membership configuration to fulfil cluster struct
        rs->lastApplied++;
        raft_apply(rs, rs->lastApplied);
    }

    srandom(time(NULL) + node_id);
    net_timer_t *timer = net_timer_init(loop,
            random_ElecttionTimeout(&rs->election_timer_rnd), 0);
    net_timer_start(timer, start_election, rs);
    rs->election_timer = timer;
    rs->heartbeat_timer = NULL;

    if (init) raft_become_leader(rs);
    loginfo("raft node startup: state(%s), node_id(%d)\n",
            raft_state(rs->state), node_id);

    return rs;
}

void raft_bind_state_machine(struct raft_server *rs, void *st)
{
    rs->st = st;
}

int main(int argc, char *argv[])
{
    int node_id;
    char *init;
    switch (argc)
    {
        case 2:
            init = NULL;
            node_id = strtol(argv[1], NULL, 10);
            break;
        case 3:
            node_id = strtol(argv[1], NULL, 10);
            init = argv[2];
            break;
        default:
            printf("usage: %s node_id [init]\n", argv[0]);
            exit(EXIT_FAILURE);
    }

    if (init && strcmp(init, "init") != 0)
    {
        printf("usage: %s node_id [init]\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    net_log_level(LOG_INFO);
    net_loop_t *loop = net_loop_init(EPOLL_SIZE);

    net_server_t *app_server =
        net_server_init(loop, "127.0.0.1", 8888 + node_id);
    net_server_set_message_callback(app_server, client_request);

    struct kv_server *kvs = malloc(sizeof(struct kv_server));
    kvs->rs = raft_start_node(loop, node_id, init);
    kvs->map = NULL;
    kvs->snapshot_path = NULL;

    net_server_set_accept_callback(app_server, bind_kv_server, kvs);
    raft_bind_state_machine(kvs->rs, kvs);

    kvs->snapshot_path = malloc(1024);
    sprintf(kvs->snapshot_path, "snapshot.StateMachine.%d", node_id);
    if (init)
    {
        kvs->map = hashInit(1024);
    }
    else {
        if (kv_snapshot_load(kvs))
        {
            kvs->map = hashInit(1024);
        }
        raft_snapshot_load(kvs->rs);
    }

    net_loop_set_stop_callback(loop, raft_server_stop, kvs);
    net_loop_start(loop);

    return 0;
}
