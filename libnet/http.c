#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <strings.h>

#include "http.h"
#include "util.h"


void http_add_route(http_server_t *server, char *path, http_handler handler)
{
    http_route_t *r = malloc(sizeof(http_route_t));
    list_init(&r->node);
    r->url_handler = handler;
    r->url_path = path;
    list_add(&server->routes, &r->node);
}


http_handler http_dispatch_route(http_server_t *s, http_request_t *req)
{
    list_t *iter;
    http_route_t *r;
    http_handler matched = NULL;

    LIST_FOR_EACH(&s->routes, iter)
    {
        r = container_of(iter, http_route_t, node);
        if (strncmp(r->url_path, req->path, strlen(r->url_path)) == 0)
        {
            matched = r->url_handler;
            break;
        }
    }

    return matched;
}


http_request_t *http_request_init(http_server_t *s, net_connect_t *c)
{
    http_request_t *req = calloc(1, sizeof(http_request_t));
    req->conn = c;
    req->http_server = s;
    req->parse_state = HTTP_PARSE_REQ_LINE;
    list_init(&req->headers);
    return req;
}


http_response_t *http_response_init(net_connect_t *c)
{
    http_response_t *res = calloc(1, sizeof(http_response_t));

    res->conn = c;

    // DEFAULT status code
    res->status_code = 200;
    res->status_msg = "OK";

    // DEFAULT response headers
    list_init(&res->headers);
    http_res_add_header(res, "Server", "libnet/0.0.1");

    return res;
}


void http_send(http_response_t *res)
{
    net_buf_t  *header;
    list_t *iter;
    http_header_t *h;

    // header
    header = net_buf_create(0);
    net_buf_append(header,
            "HTTP/1.1 %d %s\r\n", res->status_code, res->status_msg);

    LIST_FOR_EACH(&res->headers, iter)
    {
        h = container_of(iter, http_header_t, node);

        net_buf_append(header, "%s", h->header_name);
        net_buf_append(header, ": ");
        net_buf_append(header, "%s", h->header_value);
        net_buf_append(header, "\r\n");
    }
    net_buf_append(header, "\r\n");
    list_append(&res->conn->outbuf, &header->node);

    // body
    if (res->body)
    {
        list_append(&res->conn->outbuf, &res->body->node);
    }
}


void http_destroy(http_request_t *req, http_response_t *res)
{
    list_t *iter, *next;
    http_header_t *h;

    LIST_FOR_EACH_SAFE(&req->headers, iter, next)
    {
        h = container_of(iter, http_header_t, node);
        free(h);
    }

    LIST_FOR_EACH_SAFE(&res->headers, iter, next)
    {
        h = container_of(iter, http_header_t, node);
        free(h);
    }

    free(req);
    free(res);
}


void http_res_set_status(http_response_t *res, int code, char *msg)
{
    res->status_code = code;
    res->status_msg = msg;
}


void http_res_add_header(http_response_t *res, char *name, char *value)
{
    list_t *iter, *next;
    http_header_t *h;

    // if exist SAME header, del it.
    LIST_FOR_EACH_SAFE(&res->headers, iter, next)
    {
        h = container_of(iter, http_header_t, node);
        if (strcmp(h->header_name, name) == 0)
        {
            list_del(&h->node);
            free(h);
        }
    }

    h = calloc(1, sizeof(http_header_t));
    h->header_name = name;
    h->header_value = value;
    list_add(&res->headers, &h->node);
}


int http_res_have_body(http_response_t *res)
{
    if (res->body && res->body->pos > 0)
        return 1;
    else
        return 0;
}


void http_res_set_body(http_response_t *res, net_buf_t *buf)
{
    res->body = buf;
    res->body_size = buf->pos;
}


void http_add_header(http_request_t *req, char *start, char *colon, char *end)
{
    http_header_t *h = calloc(1, sizeof(http_header_t));

    h->header_name = start;
    *(colon) = '\0';

    h->header_value = colon + 2;
    *(end) = '\0';

    list_add(&req->headers, &h->node);
}


const char *http_find_header(list_t *headers, const char *name)
{
    const char *value = NULL;
    list_t *iter;
    http_header_t *h;

    LIST_FOR_EACH(headers, iter)
    {
        h = container_of(iter, http_header_t, node);
        if (strcasecmp(h->header_name, name) == 0)
            value = h->header_value;
    }

    return value;
}


int http_req_keep_alive(http_request_t *req)
{
    int is_keepalive = 0;

    const char *connection = http_find_header(&req->headers, "Connection");


    if (req->version == 1)
    {
        if (connection)
        {
            if (strcasecmp(connection, "keep-alive") == 0)
            {
                is_keepalive = 1;
            }
        }
        else
        {
            // default
            is_keepalive = 1;
        }
    }
    else if (req->version == 0)
    {
        if (connection)
        {
            if (strcasecmp(connection, "keep-alive") == 0)
            {
                is_keepalive = 1;
            }
        }
        else
        {
            // default
            is_keepalive = 0;
        }
    }
    else {
        logerr("unknown http version: %d\n", req->version);
    }

    return is_keepalive;
}


int http_res_keep_alive(http_response_t *res)
{
    int is_keepalive = 0;

    const char *connection = http_find_header(&res->headers, "Connection");

    if (connection && strcasecmp(connection, "keep-alive") == 0)
    {
        is_keepalive = 1;
    }

    return is_keepalive;
}


void http_accept_cb(net_connect_t *c, void *arg)
{
    c->data = arg;

    http_server_t *s = arg;

    http_connection_t *http_c = calloc(1, sizeof(http_connection_t));
    http_c->fd = c->io_watcher.fd;
    list_init(&http_c->node);

    list_add(&s->http_connections, &http_c->node);
}


void http_close_cb(net_connect_t *c, void *arg)
{
    list_t *iter, *next;
    http_connection_t *http_c;
    http_server_t *s = arg;

    LIST_FOR_EACH_SAFE(&s->http_connections, iter, next)
    {
        http_c = container_of(iter, http_connection_t, node);
        if (http_c->fd == c->io_watcher.fd)
        {
            list_del(&http_c->node);
            free(http_c);
            break;
        }
    }
}


void http_done_cb(net_connect_t *c, void *arg)
{
    http_server_t *s = (http_server_t *)c->data;
    http_connection_t *http_c = NULL, *tmp_c;
    list_t *iter;

    // match coresponding http-connection
    LIST_FOR_EACH(&s->http_connections, iter)
    {
        tmp_c = container_of(iter, http_connection_t, node);
        if (tmp_c->fd == c->io_watcher.fd)
        {
            http_c = tmp_c;
            break;
        }
    }

    if (http_c)
    {
        if (http_res_keep_alive(http_c->res) == 0)
            net_connection_set_close(c);

        http_destroy(http_c->req, http_c->res);
        http_c->req = NULL;
        http_c->res = NULL;
    }
}


void http_404_process(http_request_t *req, http_response_t *res)
{
    http_res_set_status(res, 404, "NOT FOUND");
    http_res_add_header(res, "Server", "libnet/0.0.1");
}


void http_request_line(http_request_t *req, char *start, int size)
{
    int left_len;
    char *first_space, *second_space, *crlf;

    first_space = util_strchr(start, ' ', size);
    if (!first_space)
    {
        logerr("no space in request method.\n");
        goto fail;
    }

    // fetch http method
    if (strncmp(start, "GET", 3))
    {
        req->method = HTTP_GET;
    }
    else if (strncmp(start, "POST", 4))
    {
        req->method = HTTP_POST;
    }
    else if (strncmp(start, "HEAD", 4))
    {
        req->method = HTTP_HEAD;
    }
    else {
        logerr("unknown http method.\n");
        goto fail;
    }

    // fetch http url path
    left_len = size - (first_space + 1 - start);
    second_space = util_strchr(first_space + 1, ' ', left_len);
    if (!second_space)
    {
        logerr("no space in request url.\n");
        goto fail;
    }

    req->path = first_space + 1;
    *(second_space) = '\0';

    // fetch http version
    left_len = size - (second_space + 1 - start);
    crlf = util_strstr(second_space + 1, "\r\n", left_len);
    if (*(crlf - 1) == '1')
    {
        req->version = 1;
    }
    else if (*(crlf - 1) == '0')
    {
        req->version = 0;
    }
    else {
        logerr("unknown http version: %d\n", *(crlf - 1) - '0');
        goto fail;
    }

    return;

fail:
    req->error = 1;
    return;
}


char* http_request_parse(http_request_t *req, char *last, int size)
{
    char *crlf = NULL, *colon, *start;
    int has_more = 1;

    // save starting point.
    start = last;

    while (has_more)
    {
        if (req->parse_state == HTTP_PARSE_REQ_LINE)
        {
            crlf = util_strstr(last, "\r\n", size-(last-start));
            if (crlf)
            {
                http_request_line(req, last, crlf-last+2);
                if (req->error) return NULL;
                req->parse_state = HTTP_PARSE_HEADER;
                last = crlf + 2;
            }
            else {
                has_more = 0;
            }
        }
        else if (req->parse_state == HTTP_PARSE_HEADER)
        {
            crlf = util_strstr(last, "\r\n", size-(last-start));
            if (crlf)
            {
                colon = util_strchr(last, ':', size-(last-start));
                if (colon)
                {
                    http_add_header(req, last, colon, crlf);
                }
                else {
                    req->parse_state = HTTP_PARSE_DONE;
                    has_more = 0;
                }
                last = crlf + 2;
            }
            else {
                has_more = 0;
            }
        }
        else if (req->parse_state == HTTP_PARSE_BODY)
        {
            logerr("not supported http parse state: %d\n", req->parse_state);
        }
        else {
            logerr("unknown http parse state: %d\n", req->parse_state);
        }
    }

    return last;
}


void http_request_process(http_request_t *req, http_connection_t *http_c)
{
    char len[22];
    http_handler handler;

    http_response_t *res = http_response_init(req->conn);
    http_c->res = res;

    handler = http_dispatch_route(req->http_server, req);
    if (handler)
    {
        handler(req, res);
    }
    else {
        http_404_process(req, res);
        logerr("no matched route: %s\n", req->path);
    }

    if (http_req_keep_alive(req))
    {
        http_res_add_header(res, "Connection", "keep-alive");
        if (http_res_have_body(res))
        {
            snprintf(len, sizeof(len), "%d", res->body->pos);
            http_res_add_header(res, "Content-Length", len);
        }
        else {
            snprintf(len, sizeof(len), "%d", 0);
            http_res_add_header(res, "Content-Length", len);
        }
    }
    else {
        http_res_add_header(res, "Connection", "close");
    }

    http_send(res);
}


// user-defined OnMessage callback.
int net_request_process(char *start, size_t size, net_connect_t *c)
{
    http_server_t *s = (http_server_t *)c->data;
    http_connection_t *http_c = NULL, *tmp_c;
    http_request_t *req;
    list_t *iter;
    char *last = start;

    if (size <= 0) return NET_AGAIN;

    // match coresponding http-connection
    LIST_FOR_EACH(&s->http_connections, iter)
    {
        tmp_c = container_of(iter, http_connection_t, node);
        if (tmp_c->fd == c->io_watcher.fd)
        {
            http_c = tmp_c;
            break;
        }
    }
    if (!http_c) return NET_ERR;

    // create http req if not exist.
    http_c->req = http_c->req ? http_c->req : http_request_init(s, c);
    if(!http_c->req)
    {
        logerr("init request error\n");
        return NET_ERR;
    }
    req = http_c->req;

    /* For HTTP parsing, we need ensure one null-terminated string. */
    *(start + size) = '\0';

    last = http_request_parse(req, last, size);

    if (!last) return NET_ERR;

    if (req->parse_state == HTTP_PARSE_DONE)
    {
        http_request_process(req, http_c);
        net_connection_send(c);
    }

    if (last >= start)
    {
        return last - start;
    }
    else {
        logerr("error parse http.\n");
        return NET_ERR;
    }
}


// user-definded OnError callback.
void net_request_error(const char *err_msg)
{
    logerr("req error: %s\n", err_msg);
}


http_server_t *http_server_init(char *host, int port)
{
    net_loop_t *loop;
    net_server_t *tcp_server;
    http_server_t *http_server;

    loop = net_loop_init(EPOLL_SIZE);
    if (!loop)
    {
        logerr("init loop failed.\n");
        exit(EXIT_FAILURE);
    }

    tcp_server = net_server_init(loop, host, port);
    if (!tcp_server)
    {
        logerr("init server failed.\n");
        exit(EXIT_FAILURE);
    }

    http_server = calloc(1, sizeof(http_server_t));
    list_init(&http_server->routes);
    list_init(&http_server->http_connections);
    http_server->tcp_server = tcp_server;

    net_server_set_accept_callback(tcp_server, http_accept_cb, http_server);
    net_server_set_close_callback(tcp_server, http_close_cb, http_server);
    net_server_set_message_callback(tcp_server, net_request_process);
    net_server_set_done_callback(tcp_server, http_done_cb, http_server);
    net_server_set_error_callback(tcp_server, net_request_error);

    return http_server;
}


void http_server_start(http_server_t *s)
{
    net_loop_start(s->tcp_server->loop);
}


