
#ifndef __UTIL_H__
#define __UTIL_H__

// default log level is LOG_ERR
int log_level;

#define LOG_ERR   0
#define LOG_INFO  1
#define LOG_DEBUG 2

// default logging to stderr
#define logerr(fmt, ...)   _log(LOG_ERR, 2, "[error] "fmt, ## __VA_ARGS__)
#define loginfo(fmt, ...)  _log(LOG_INFO,  2, "[info ] "fmt, ## __VA_ARGS__)
#define logdebug(fmt, ...) _log(LOG_DEBUG, 2, "[debug] "fmt, ## __VA_ARGS__)

void net_log_level(int);
int _log(int level, int fd, const char *fmt, ...);

// string manipulation
char *util_strstr(char *haystack, char *needle, int len);
char *util_strchr(const char *s, int c, int len);

#endif
