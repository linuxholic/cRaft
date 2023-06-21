#ifndef _NET_H_
#define _NET_H_

#include <time.h>
#include <stddef.h>
#include <arpa/inet.h>
#include <sys/timerfd.h>

#include "list.h"
#include "util.h"

#define setnoblock(s) fcntl(s, F_SETFL, fcntl(s, F_GETFL) | O_NONBLOCK)
#define EPOLL_SIZE 1000
#define REQ_SIZE 512
#define NET_BUF_SIZE 1024

#define NET_OK 0
#define NET_ERR -1
#define NET_AGAIN -2

typedef struct net_connect_t net_connect_t;
typedef struct net_server_t  net_server_t;
typedef struct net_client_t  net_client_t;
typedef struct net_timer_t   net_timer_t;
typedef struct net_loop_t    net_loop_t;
typedef struct net_buf_t     net_buf_t;
typedef struct net_io_t      net_io_t;

typedef int  (*io_handler)(char *, size_t, net_connect_t *);
typedef void (*accept_handler)(net_connect_t *, void *);
typedef void (*done_handler)(net_connect_t *, void *);
typedef void (*connect_handler)(net_connect_t *, void *);
typedef void (*close_handler)(net_connect_t *, void *);
typedef void (*error_hanlder)(const char *);
typedef void (*stop_handler)(net_loop_t *, void*);
typedef void (*timer_handler)(net_timer_t *);

typedef void (*net_io_cb)(net_io_t *);

struct net_buf_t {
    list_t node;

    char *buf;
    int size;
    int pos;
    int consume;

    int req_cnt;
    int auto_scale;
};

// epoll user data ptr ( a higher level wrapper of io event )
struct net_io_t {
    list_t node;
    int fd;
    int alive;
    int writing;
    int reading;
    net_io_cb cb;
    uint32_t events;
};

struct net_connect_t {
    list_t node;

    int peer_close;
    int closing;
    int connecting;
    int err;

    net_io_t io_watcher;
    struct sockaddr_in remote_addr;

    net_buf_t *inbuf;
    list_t outbuf;

    void (*on_read)(net_connect_t *);
    void (*on_write)(net_connect_t *);
    void (*on_error)(const char *);

    void (*on_write_done)(net_connect_t *, void *);
    void *done_data;

    // user data ptr
    void *data;

    net_loop_t *loop;
    net_server_t *server;
    net_client_t *client;
};

struct net_server_t {

    // listen addr
    char *local_host;
    int local_port;

    net_connect_t *conn_listen;

    /* private members */

    list_t conn_list;
    net_loop_t *loop;

    /* public callback */

    accept_handler on_accept;
    void *accept_data;

    close_handler on_close;
    void *close_data;

    done_handler on_write_done;
    void *done_data;

    io_handler on_message;
    error_hanlder on_error;
};

struct net_client_t {

    // local addr
    char local_addr[INET_ADDRSTRLEN];

    // remote addr
    char peer_host[INET_ADDRSTRLEN];
    int peer_port;

    connect_handler on_connect;
    void *connect_data;

    close_handler on_close;
    void *close_data;

    io_handler on_message;
    done_handler on_write_done;

    void *user_data;

    int keep_alive;

    net_connect_t *conn;
    net_loop_t *loop;
};

struct net_loop_t {

    int epfd;
    struct epoll_event *evlist;
    int size;

    list_t postpone_events;

    int stop;
    stop_handler on_stop;
    void *stop_data;
};

enum event_type {
    NET_EV_READ,
    NET_EV_WRITE,
    NET_EV_ALL
};

struct net_timer_t {

    net_loop_t *loop;

    int timer_fd;

    net_io_t timer_watcher;

    timer_handler timer_cb;
    void *timer_data;

    net_connect_t *conn;
};

// buf
net_buf_t *net_buf_create(size_t);
void net_buf_append(net_buf_t *, const char *, ...);
void net_buf_copy(net_buf_t *, char *, size_t);

// loop
net_loop_t *net_loop_init(size_t);
void net_loop_set_stop_callback(net_loop_t *, stop_handler, void *);
void net_loop_start(net_loop_t *);
void net_loop_stop(net_loop_t *);

// server
net_server_t *net_server_init(net_loop_t *, char *, int);
void net_server_set_error_callback(net_server_t *, error_hanlder);
void net_server_set_message_callback(net_server_t *, io_handler);
void net_server_set_done_callback(net_server_t *, done_handler, void *);
void net_server_set_accept_callback(net_server_t *, accept_handler, void *);
void net_server_set_close_callback(net_server_t *, close_handler, void *);

// client
net_client_t *net_client_init(net_loop_t *, char *, int);
void net_client_set_connection_callback(net_client_t *, connect_handler, void *);
void net_client_set_response_callback(net_client_t *, io_handler);
void net_client_set_done_callback(net_client_t *, done_handler);
void net_client_set_user_data(net_client_t *, void *);
void net_client_set_keep_alive(net_client_t *, int);
void net_client_set_close_callback(net_client_t *, close_handler, void *);

// connection
void net_connection_set_close(net_connect_t *);
void net_connection_send(net_connect_t *);
void net_connection_close(net_connect_t *);
void net_connection_suspend(net_connect_t *);

// timer
net_timer_t* net_timer_init(net_loop_t *, int, int);
void net_timer_start(net_timer_t *, timer_handler, void *);
void* net_timer_data(net_timer_t *timer);
void net_timer_stop(net_timer_t *timer);
void net_timer_reset(net_timer_t *timer, int value, int interval);

#endif // _NET_H_
