#include <arpa/inet.h>
#include <sys/socket.h>
#include <netinet/in.h>
#include <sys/epoll.h>
#include <sys/types.h>
#include <strings.h>
#include <stdarg.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <stddef.h>
#include <signal.h>
#include <stdio.h>
#include <fcntl.h>
#include <errno.h>

#include "net.h"
#include "util.h"

int net_buf_full(net_buf_t *b)
{
    return b->pos == b->size;
}


void net_buf_reset(net_buf_t *buf)
{
    buf->pos = 0;
    buf->consume = 0;
    buf->req_cnt = 0;
}


void net_buf_copy(net_buf_t *buf, char *start, size_t size)
{
    memcpy(buf->buf + buf->pos, start, size);
    buf->pos += size;
}


net_buf_t *net_buf_create(size_t size)
{
    net_buf_t *buf = malloc(sizeof(net_buf_t));

    if (size)
    {
        buf->size = size;
        buf->auto_scale = 0;
    }
    else {
        buf->size = NET_BUF_SIZE;
        buf->auto_scale = 1;
    }

    buf->buf = malloc(buf->size);
    net_buf_reset(buf);
    list_init(&buf->node);

    return buf;
}


void net_buf_scale(net_buf_t *buf, int diff)
{
    char *new_buf = malloc(buf->size + 2*diff);
    memcpy(new_buf, buf->buf, buf->pos);
    buf->size += 2*diff;
    free(buf->buf);
    buf->buf = new_buf;
}


// NOTE: @src must be terminated with null byte.
void net_buf_append(net_buf_t *buf, const char *fmt, ...)
{
    int size = 0;
    char *p = NULL;
    va_list ap;

    // determine @fmt size.
    va_start(ap, fmt);
    size = vsnprintf(p, size, fmt, ap);
    va_end(ap);

    if (size < 0)
    {
        logerr("vsprintf error\n");
        return;
    }

    // for trailing '\0'
    size++;

    // check @buf size.
    if (size + buf->pos > buf->size)
    {
        if (buf->auto_scale)
        {
            net_buf_scale(buf, size);
        }
        else {
            logerr("buf overflow!");
        }
    }

    va_start(ap, fmt);
    size = vsnprintf(buf->buf + buf->pos, size, fmt, ap);
    va_end(ap);

    buf->pos += size;
}


void net_buf_del(net_buf_t *buf)
{
    free(buf->buf);
    list_del(&buf->node);
    free(buf);
}


net_buf_t *net_buf_realloc(net_buf_t *old)
{
    int len;
    net_buf_t *new;

    len = old->pos - old->consume;
    if (len == 0)
    {
        net_buf_reset(old);
        return old;
    }

    new = net_buf_create(old->size);
    memcpy(new->buf, old->buf + old->consume, len);
    new->pos += len;

    net_buf_del(old);
    return new;
}


int net_listen(char *host, int port)
{
    int n = 1;
    int sock_fd;
    struct sockaddr_in addr;

    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0)
    {
        logerr("socket failed: %s\n", strerror(errno));
        return -1;
    }

    if (setnoblock(sock_fd))
    {
        logerr("[%s] set non-block failed: %s\n", __func__, strerror(errno));
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(host);
    addr.sin_port = htons(port);

    if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n)))
    {
        logerr("setsockopt SO_REUSEADDR failed: %s\n", strerror(errno));
        close(sock_fd);
        return -1;
    }

    if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEPORT, &n, sizeof(n)))
    {
        logerr("setsockopt SO_REUSEPORT failed: %s\n", strerror(errno));
        close(sock_fd);
        return -1;
    }

    if (bind(sock_fd, (struct sockaddr *)&addr, sizeof(struct sockaddr)))
    {
        logerr("bind failed: %s\n", strerror(errno));
        close(sock_fd);
        return -1;
    }

    if (listen(sock_fd, 100))
    {
        logerr("listen failed: %s\n", strerror(errno));
        close(sock_fd);
        return -1;
    }

    logdebug("listen %s:%d\n", host, port);

    return sock_fd;
}


int net_connect(char *host, int port)
{
    int n = 1;
    int sock_fd;
    struct sockaddr_in addr;

    sock_fd = socket(AF_INET, SOCK_STREAM, 0);
    if (sock_fd < 0)
    {
        logerr("socket failed: %s\n", strerror(errno));
        return -1;
    }

    if (setnoblock(sock_fd))
    {
        logerr("[%s] set non-block failed: %s\n", __func__, strerror(errno));
        return -1;
    }

    memset(&addr, 0, sizeof(addr));
    addr.sin_family = AF_INET;
    addr.sin_addr.s_addr = inet_addr(host);
    addr.sin_port = htons(port);

    if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEADDR, &n, sizeof(n)))
    {
        logerr("setsockopt SO_REUSEADDR failed: %s\n", strerror(errno));
        close(sock_fd);
        return -1;
    }

    if (setsockopt(sock_fd, SOL_SOCKET, SO_REUSEPORT, &n, sizeof(n)))
    {
        logerr("setsockopt SO_REUSEPORT failed: %s\n", strerror(errno));
        close(sock_fd);
        return -1;
    }

    if (connect(sock_fd, (struct sockaddr *)&addr, sizeof(struct sockaddr)))
    {
        if (errno != EINPROGRESS)
        {
            logerr("connect to %s:%d failed: %s\n",
                    host, port, strerror(errno));
            close(sock_fd);
            return -1;
        }
    }

    return sock_fd;
}


// add io event
void net_io_start(net_loop_t *loop, net_io_t *w, enum event_type type)
{
    int op;
    struct epoll_event ee = {0};

    if (type == NET_EV_READ)
    {
        if (w->reading) return;
        ee.events |= (EPOLLIN | EPOLLRDHUP);
        w->reading = 1;
    }
    else if (type == NET_EV_WRITE)
    {
        if (w->writing) return;
        ee.events |= EPOLLOUT;
        w->writing = 1;
    }

    ee.events |= EPOLLET;
    ee.data.ptr = w;

    if (w->alive)
        op = EPOLL_CTL_MOD;
    else
    {
        op = EPOLL_CTL_ADD;
        w->alive = 1;
    }

    if ((epoll_ctl(loop->epfd, op, w->fd, &ee)) == -1)
    {
        logerr("%s: epoll_ctl failed: %s\n", __func__, strerror(errno));
    }
}


// del io event
void net_io_stop(net_loop_t *loop, net_io_t *w, enum event_type type)
{
    int op;
    struct epoll_event ee, *eep;

    ee.events = EPOLLET;
    ee.data.ptr = w;
    eep = &ee;

    op = EPOLL_CTL_MOD;

    logdebug("[fd: %d] cancel event: %d\n", w->fd, type);

    if (type == NET_EV_READ)
    {
        if (w->reading == 0) return;
        if (w->writing) ee.events |= EPOLLOUT;
        w->reading = 0;
    }
    else if (type == NET_EV_WRITE)
    {
        if (w->writing == 0) return;
        if (w->reading) ee.events |= EPOLLIN | EPOLLRDHUP;
        w->writing = 0;
    }
    else if (type == NET_EV_ALL)
    {
        eep = NULL;
        op = EPOLL_CTL_DEL;
    }
    else {
        logerr("unknown event type: %d\n", type);
        return;
    }

    if ((epoll_ctl(loop->epfd, op, w->fd, eep)) == -1)
    {
        logerr("%s: epoll_ctl failed: %s\n", __func__, strerror(errno));
    }
}


void net_io_post(net_loop_t *loop, net_io_t *w)
{
    list_append(&loop->postpone_events, &w->node);
}


net_connect_t * net_connection_new(net_loop_t *loop, int fd)
{
    net_connect_t *c = calloc(1, sizeof(net_connect_t));

    c->loop = loop;
    c->inbuf = net_buf_create(REQ_SIZE);
    list_init(&c->outbuf);
    list_init(&c->node);

    return c;
}


void net_connection_suspend(net_connect_t *c)
{
    net_io_stop(c->loop, &c->io_watcher, NET_EV_READ);
}


void net_connection_close(net_connect_t *c)
{
    list_t *node, *node_next;
    net_buf_t *output;

    logdebug("[conn: %p, fd: %d] connection closed.\n", c, c->io_watcher.fd);

    if (c->io_watcher.alive) c->io_watcher.alive = 0;
    else return;

    // del fd from epoll
    net_io_stop(c->loop, &c->io_watcher, NET_EV_ALL);

    // free input buf
    net_buf_del(c->inbuf);

    // free output buf
    LIST_FOR_EACH_SAFE(&c->outbuf, node, node_next)
    {
        output = container_of(node, net_buf_t, node);
        net_buf_del(output);
    }

    // invoke server-type callback
    if (c->server && c->server->on_close)
    {
        (c->server->on_close)(c, c->server->close_data);
    }

    // invoke client-type callback
    if (c->client && c->client->on_close)
    {
        (c->client->on_close)(c, c->client->close_data);

        // when closing one connection, its client is useless hence.
        // because one client corresponds to only one connection.
        free(c->client);
    }

    // shutdown peer connection
    close(c->io_watcher.fd);

    // del from server->conn_list
    list_del(&c->node);

    // free conn
    free(c);
}


void net_connection_set_error(net_connect_t *conn)
{
    conn->err = 1;
}


void net_connection_error(net_connect_t *conn, const char *err_msg)
{
    if (conn->on_error) (conn->on_error)(err_msg);
    logdebug("[before] err:%d fd:%d error:%s\n",
            conn->err, conn->io_watcher.fd, err_msg);
    net_connection_set_error(conn);
    logdebug("[after] err:%d fd:%d error:%s\n",
            conn->err, conn->io_watcher.fd, err_msg);
}


void net_connection_set_close(net_connect_t *conn)
{
    conn->closing = 1;
}


int net_connection_should_close(net_connect_t *c)
{
    if (c->err)
    {
        net_connection_close(c);
        return 1;
    }

    // ensure send all data before close connection
    if (list_empty(&c->outbuf))
    {
        if (c->closing)
        {
            net_connection_close(c);
            return 1;
        }
    }

    return 0;
}


void net_connection_send(net_connect_t *conn)
{
    ssize_t n;
    list_t *node, *node_next;
    net_buf_t *output;

    LIST_FOR_EACH_SAFE(&conn->outbuf, node, node_next)
    {
        output = container_of(node, net_buf_t, node);

        if (output->pos)
        {
            n = write(conn->io_watcher.fd,
                    output->buf + output->consume,
                    output->pos - output->consume);
            if (n >= 0)
            {
                output->consume += n;
                if (output->pos > output->consume)
                {
                    break;
                }
                else {
                    logdebug("[conn: %p, fd: %d] send data, size: %ld\n",
                            conn, conn->io_watcher.fd, n);
                    net_buf_del(output);
                }
            }
            else {
                if (errno == EAGAIN || errno == EWOULDBLOCK)
                {
                    // somehow, fd not ready for writing :(
                    logdebug("[conn: %p, fd: %d] write not ready now: %s\n",
                            conn, conn->io_watcher.fd, strerror(errno));
                    break;
                }
                else {
                    logerr("[conn: %p, fd: %d] write error: %s\n",
                            conn, conn->io_watcher.fd, strerror(errno));
                    conn->err = 1;
                    break;
                }
            }
        }
    }

    if (conn->err)
    {
        net_io_stop(conn->loop, &conn->io_watcher, NET_EV_WRITE);
        return;
    }

    if (list_empty(&conn->outbuf))
    {
        // it's possible we begin sending data before
        // processing connect-triggered write event.
        if (!conn->connecting)
        {
            net_io_stop(conn->loop, &conn->io_watcher, NET_EV_WRITE);
        }

        // every application net_connect_send() triggers this callback once.
        if (conn->on_write_done)
        {
            (conn->on_write_done)(conn, conn->done_data);
        }
    }
    else {
        logdebug("[conn: %p, fd: %d] write partial, wait next time.\n",
                conn, conn->io_watcher.fd);

        // if first write is not complete, we should arm WRITE event
        // but in other way, we shouldn't override connect callback.
        // (that's why we need to call net_conn_send()
        // within net_on_conncet(), to trigger possibe pending write)
        if (!conn->connecting)
        {
            net_io_start(conn->loop, &conn->io_watcher, NET_EV_WRITE);
            conn->on_write = net_connection_send;
        }
    }
}


void net_connection_on_readable(net_connect_t *c)
{
    int has_more = 1, recv_bytes, parsed_bytes = 0;

pending_data:

    // realloc buf, prepare for next req
    c->inbuf = net_buf_realloc(c->inbuf);

    recv_bytes = recv(c->io_watcher.fd, c->inbuf->buf + c->inbuf->pos,
            c->inbuf->size - c->inbuf->pos, 0);

    if (recv_bytes > 0)
    {
        logdebug("[conn: %p, fd: %d] recv data, size: %d\n",
                c, c->io_watcher.fd, recv_bytes);
        c->inbuf->pos += recv_bytes;
    }
    else if (recv_bytes == 0) {
        logdebug("[conn: %p, fd: %d] recv 0, closing connection.\n",
                c, c->io_watcher.fd);
        net_connection_close(c);
        return;
    }
    else {
        if (errno == EAGAIN || errno == EWOULDBLOCK)
        {
            has_more = 0;
        }
        else {
            logerr("[conn: %p, fd: %d] recv error: %s\n",
                    c, c->io_watcher.fd, strerror(errno));
            c->err = 1;
            net_connection_close(c);
            return;
        }
    }

    while (has_more && c->inbuf->consume < c->inbuf->pos)
    {
        if (c->server && c->server->on_message)
        {
            parsed_bytes = (c->server->on_message)(
                    c->inbuf->buf + c->inbuf->consume,
                    c->inbuf->pos - c->inbuf->consume, c);
        }
        else if(c->client)
        {
            if (c->client->on_message)
            {
                parsed_bytes = (c->client->on_message)(
                        c->inbuf->buf + c->inbuf->consume,
                        c->inbuf->pos - c->inbuf->consume, c);
            }
            else {
                logerr("[omg] client empty on_message callback\n");
                break;
            }
        }
        else {
            logerr("[omg] connection not bind with client or server.\n");
        }

        if (parsed_bytes > 0)
        {
            // proceed to next req.
            c->inbuf->consume += parsed_bytes;
            (c->inbuf->req_cnt)++;
        }
        else {
            if (parsed_bytes < 0)
            {
                if (parsed_bytes == NET_ERR)
                {
                    net_connection_error(c, "message process error");
                }
                // too much single req data
                else if (net_buf_full(c->inbuf) && c->inbuf->req_cnt == 0)
                {
                    net_connection_error(c,
                            "single packet contains too much data");
                }
                else {
                    logerr("[omg] message process error: %d\n", parsed_bytes);
                }
            }
            else { // parsed_bytes == 0
                // recv data not complete, need read more.
            }
            break;
        }
    }

    // if user require closing, then we don't need to retrive remaining data
    if (net_connection_should_close(c)) return;

    // Normally, we'll keep receiving data until EAGAIN.
    if (c->io_watcher.reading && has_more) goto pending_data;
}


// init io event
void net_io_init(net_io_t *io, net_io_cb cb, int fd)
{
    memset(io, 0,sizeof(net_io_t));
    io->fd = fd;
    io->alive = 0;
    io->cb = cb;
}


// io event callback
void net_tcp_io(net_io_t *w)
{
    uint32_t events = w->events;
    net_connect_t *c = container_of(w, net_connect_t, io_watcher);

    if (events & EPOLLIN)
    {
        logdebug("[conn: %p, fd: %d] readable event occurs.\n", c, w->fd);
        if (events & (EPOLLRDHUP|EPOLLHUP))
        {
            logdebug("[conn: %p, fd: %d] peer shutdown/close!\n", c, w->fd);
            c->peer_close = 1;
        }
        if (c->on_read) c->on_read(c);
        else logerr("[conn: %p, fd: %d] no read handler!\n", c, w->fd);
    }

    if (events & EPOLLOUT)
    {
        logdebug("[conn: %p, fd: %d] writeable event occurs.\n", c, w->fd);
        if (c->on_write) c->on_write(c);
        else logerr("[conn: %p, fd: %d] no write handler!\n", c, w->fd);
    }

    if (events & EPOLLHUP)
    {
        logdebug("[conn: %p, fd: %d] peer close!\n", c, w->fd);
    }
}


void net_on_accept(net_connect_t *c)
{
    char addr_str[INET_ADDRSTRLEN];
    struct sockaddr_in addr;
    socklen_t addr_len = sizeof(addr);
    net_server_t *server = c->server;
    int est_fd;

    while(1) /* in case of multiple ready connections */
    {
        est_fd = accept(c->io_watcher.fd, (struct sockaddr *)&addr, &addr_len);

        if (est_fd > 0)
        {
            if (setnoblock(est_fd))
            {
                logerr("set non-block failed: %s\n", strerror(errno));
                continue;
            }

            net_connect_t *new_c = net_connection_new(c->loop, est_fd);
            memcpy(&new_c->remote_addr, &addr, addr_len);

            new_c->server = server;
            list_add(&server->conn_list, &new_c->node);

            new_c->on_read = net_connection_on_readable;
            new_c->on_write_done = server->on_write_done;
            new_c->done_data = server->done_data;
            new_c->on_error = server->on_error;

            net_io_init(&new_c->io_watcher, net_tcp_io, est_fd);
            net_io_start(new_c->loop, &new_c->io_watcher, NET_EV_READ);

            if (server->on_accept)
            {
                server->on_accept(new_c, server->accept_data);
            }

            inet_ntop(AF_INET, &addr.sin_addr, addr_str, INET_ADDRSTRLEN);
            logdebug("[conn: %p, fd: %d] accept from %s:%d\n",
                    new_c, est_fd, addr_str, ntohs(addr.sin_port));
        }
        else {
            if (errno == EWOULDBLOCK || errno == EAGAIN)
            {
                logdebug("accept would block.\n");
            }
            else {
                logerr("accept failed: %s\n", strerror(errno));
            }
            break;
        }
    }
}


void net_on_connect(net_connect_t *c)
{
    int err = 1;
    socklen_t len = sizeof(err);
    net_client_t *client = c->client;
    struct sockaddr_in local_addr;
    socklen_t addr_len = sizeof(struct sockaddr_in);

    net_io_stop(c->loop, &c->io_watcher, NET_EV_WRITE);
    c->connecting = 0;

    logdebug("[conn: %p, fd: %d] check connect result.\n",
            c, c->io_watcher.fd);
    getsockopt(c->io_watcher.fd, SOL_SOCKET, SO_ERROR, &err, &len);
    if (!err)
    {
        if (getsockname(c->io_watcher.fd,
                    (struct sockaddr *)&local_addr, &addr_len) == 0)
        {
            inet_ntop(AF_INET, &local_addr.sin_addr,
                    client->local_addr, INET_ADDRSTRLEN);
            logdebug("[conn: %p, fd: %d] local %s:%d connected to %s:%d\n",
                    c, c->io_watcher.fd,
                    client->local_addr, ntohs(local_addr.sin_port),
                    client->peer_host, client->peer_port);
        }

        // in case of sending before connect is complete.
        if (!list_empty(&c->outbuf))
        {
            net_connection_send(c);
        }

        // setup handler for server response
        c->on_read = net_connection_on_readable;
        net_io_start(c->loop, &c->io_watcher, NET_EV_READ);
    }
    else {
        logerr("connected to '%s:%d' failed: %s\n",
                client->peer_host, client->peer_port, strerror(err));
        c->err = 1;
    }

    if (client->on_connect)
    {
        (client->on_connect)(c, client->connect_data);

        // better to check connection status here, in case of user ops
        if (net_connection_should_close(c)) return;
    }
}


void net_loop_start(net_loop_t *loop)
{
    int n, idx;
    list_t *node, *node_next;
    net_io_t *w;

    signal(SIGPIPE, SIG_IGN);

    while(!loop->stop)
    {
        int timer = 1 * 1000; // unit is millisecond
        n = epoll_wait(loop->epfd, loop->evlist, loop->size, timer);

        // process normal events
        for (idx = 0; idx < n; idx++)
        {
            logdebug("epoll idx: %d, total: %d\n", idx, n);
            w = loop->evlist[idx].data.ptr;
            w->events = loop->evlist[idx].events;
            w->cb(w);
        }

        // process postpone events
        LIST_FOR_EACH_SAFE(&loop->postpone_events, node, node_next)
        {
            w = container_of(node, net_io_t, node);
            list_del(&w->node);
            w->cb(w);
        }
    }

    if (loop->on_stop)
    {
        (loop->on_stop)(loop, loop->stop_data);
    }

    free(loop->evlist);
    free(loop);

    logdebug("loop end.\n");
}


void net_loop_stop(net_loop_t *loop)
{
    loop->stop = 1;
}


net_loop_t *net_loop_init(size_t epoll_size)
{
    net_loop_t *_loop = malloc(sizeof(net_loop_t));
    if(_loop == NULL)
    {
        logerr("loop malloc failed.\n");
        return NULL;
    }

    _loop->stop = 0;
    _loop->size = epoll_size;
   list_init(&_loop->postpone_events);

    _loop->evlist = malloc(sizeof(struct epoll_event) * _loop->size);
    if (_loop->evlist == NULL)
    {
        logerr("epoll malloc failed.\n");
        return NULL;
    }

    _loop->epfd = epoll_create(_loop->size);
    if (_loop->epfd == -1)
    {
        logerr("epoll_create failed.\n");
        free(_loop->evlist);
        free(_loop);
        return NULL;
    }
    logdebug("loop init succ, epfd: %d\n", _loop->epfd);

    net_loop_set_stop_callback(_loop, NULL, NULL);

    return _loop;
}


void net_loop_set_stop_callback(net_loop_t *loop, stop_handler cb, void *arg)
{
    loop->on_stop = cb;
    loop->stop_data = arg;
}


net_server_t *net_server_init(net_loop_t *loop, char *host, int port)
{
    int listen_fd;

    listen_fd = net_listen(host, port);
    if (listen_fd < 0) return NULL;

    net_server_t *server = calloc(1, sizeof(net_server_t));
    server->loop = loop;
    server->local_host = host;
    server->local_port = port;
    list_init(&server->conn_list);

    net_connect_t *c = net_connection_new(loop, listen_fd);
    c->on_read = net_on_accept;
    c->server = server;

    // arm io(read) event
    net_io_init(&c->io_watcher, net_tcp_io, listen_fd);
    net_io_start(loop, &c->io_watcher, NET_EV_READ);

    server->conn_listen = c;

    return server;
}


void net_server_set_error_callback(net_server_t *s, error_hanlder cb)
{
    s->on_error = cb;
}


void net_server_set_message_callback(net_server_t *s, io_handler cb)
{
    s->on_message = cb;
}


void net_server_set_accept_callback(
        net_server_t *s, accept_handler cb, void *arg)
{
    s->on_accept = cb;
    s->accept_data = arg;
}


void net_server_set_close_callback(
        net_server_t *s, close_handler cb, void *arg)
{
    s->on_close = cb;
    s->close_data = arg;
}


void net_server_set_done_callback(net_server_t *s, done_handler cb, void *arg)
{
    s->on_write_done = cb;
    s->done_data = arg;
}


net_client_t *net_client_init(net_loop_t *loop, char *host, int port)
{
    net_client_t *client;
    net_connect_t *conn;
    int fd;

    client = calloc(1, sizeof(net_client_t));
    client->loop = loop;
    strcpy(client->peer_host, host);
    client->peer_port = port;

    fd = net_connect(host, port);
    if (fd == -1) return NULL;

    conn = net_connection_new(loop, fd);
    conn->on_write = net_on_connect;
    conn->connecting = 1;

    // arm io(write) event
    net_io_init(&conn->io_watcher, net_tcp_io, fd);
    net_io_start(loop, &conn->io_watcher, NET_EV_WRITE);
    logdebug("[conn: %p, fd: %d] arm WRITE(connect) event.\n",
            conn, conn->io_watcher.fd);

    conn->client = client;
    client->conn = conn;

    return client;
}


void net_client_set_connection_callback(net_client_t *client,
        connect_handler cb, void *arg)
{
    client->on_connect = cb;
    client->connect_data = arg;
}


void net_client_set_response_callback(net_client_t *client, io_handler cb)
{
    client->on_message = cb;
}


void net_client_set_done_callback(net_client_t *client, done_handler cb)
{
    net_connect_t *c = client->conn;
    c->on_write_done = cb;
}


void net_client_set_user_data(net_client_t *client, void *arg)
{
    client->user_data = arg;
}


void net_client_set_keep_alive(net_client_t *client, int keep_alive)
{
    client->keep_alive = keep_alive;
}


void net_client_set_close_callback(
        net_client_t *client, close_handler cb, void *arg)
{
    client->on_close = cb;
    client->close_data = arg;
}


void net_timer_destroy(net_timer_t *timer)
{
    int fd = timer->timer_fd;
    struct itimerspec new_value = {{0},{0}};

    // disarm this timer
    if (timerfd_settime(fd, 0, &new_value, NULL) == -1)
    {
        perror("timer stop: timerfd_settime failed");
    }

    net_io_stop(timer->loop, &timer->timer_watcher, NET_EV_ALL);
    close(fd);
    free(timer);
    logdebug("timer stopped.\n");
}


// change trigger policy, also used to stop/disarm one timer
void net_timer_reset(net_timer_t *timer, int value, int interval)
{
    int fd = timer->timer_fd;
    struct itimerspec new_value = {{interval, 0},{value, 0}};

    if (timerfd_settime(fd, 0, &new_value, NULL) == -1)
    {
        perror("timer reset: timerfd_settime failed");
    }
}


void net_timer_trigger(net_io_t *w)
{
    ssize_t ret;
    uint64_t counts;
    net_timer_t *timer = container_of(w, net_timer_t, timer_watcher);

    ret = read(timer->timer_fd, &counts, sizeof(uint64_t));
    if (ret != sizeof(uint64_t))
    {
        perror("read timerfd failed");
        net_timer_destroy(timer);
        return;
    }

    if (counts > 1) logdebug("collapse num: %ld\n", counts);

    timer->timer_cb(timer);
}


net_timer_t* net_timer_init(net_loop_t *loop, int value, int interval)
{
    int fd = timerfd_create(CLOCK_REALTIME, 0);
    if (fd == -1)
    {
        perror("timerfd_create failed");
        return NULL;
    }

    struct itimerspec new_value = { {interval, 0}, {value, 0}};
    if (timerfd_settime(fd, 0, &new_value, NULL) == -1)
    {
        perror("timer init: timerfd_settime failed");
        return NULL;
    }

    net_timer_t *timer = calloc(1, sizeof(net_timer_t));
    if (timer == NULL)
    {
        perror("net_timer_init calloc failed");
        return NULL;
    }

    timer->timer_fd = fd;
    timer->loop = loop;

    net_io_init(&timer->timer_watcher, net_timer_trigger, timer->timer_fd);
    net_io_start(loop, &timer->timer_watcher, NET_EV_READ);

    return timer;
}

void net_timer_start(net_timer_t *timer, timer_handler cb, void *arg)
{
    timer->timer_cb = cb;
    timer->timer_data = arg;
}

void* net_timer_data(net_timer_t *timer)
{
    return timer->timer_data;
}
