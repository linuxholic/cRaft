#ifndef _HTTP_H_
#define _HTTP_H_

#include "net.h"
#include "list.h"

typedef  struct http_request_t http_request_t;
typedef struct http_header_t http_header_t;
typedef struct http_response_t http_response_t;
typedef struct http_route_t http_route_t;
typedef struct http_server_t http_server_t;
typedef struct http_connection_t http_connection_t;

#define HTTP_GET 0
#define HTTP_POST 1
#define HTTP_HEAD 2

#define HTTP_PARSE_REQ_LINE 0
#define HTTP_PARSE_HEADER 1
#define HTTP_PARSE_BODY 2
#define HTTP_PARSE_DONE 3

typedef void(*http_handler)(http_request_t *, http_response_t *);

struct http_header_t
{
    list_t node;
    char *header_name;
    char *header_value;
};


struct http_request_t
{
    int method;
    int version;
    char *path;
    list_t headers;
    int error;
    int parse_state;
    net_connect_t *conn;
    http_server_t *http_server;
};


struct http_response_t
{
    int status_code;
    char *status_msg;

    list_t headers;
    net_buf_t *body;
    int body_size;

    net_connect_t *conn;
};


struct http_route_t
{
    list_t node;

    char *url_path;
    http_handler url_handler;
};


struct http_connection_t
{
    list_t node;

    int fd;
    http_request_t *req;
    http_response_t *res;
};


struct http_server_t
{
    list_t routes;
    list_t http_connections;

    net_server_t *tcp_server;
};

http_server_t *http_server_init(char *, int);
void http_server_start(http_server_t *);
void http_add_route(http_server_t *, char *, http_handler);
void http_res_set_status(http_response_t *, int, char *);
void http_res_add_header(http_response_t *, char *, char *);
void http_res_set_body(http_response_t *, net_buf_t *);

#endif // _HTTP_H_
