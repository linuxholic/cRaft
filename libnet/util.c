#include <stdarg.h>
#include <time.h>
#include <stdio.h>
#include <string.h>
#include <unistd.h>
#include <sys/syscall.h>

extern int log_level;

int _log(int level, int fd, const char *fmt, ...)
{
    time_t t;
    va_list ap;
    struct tm now;
    char log_buf[1024];
    int n, prefix, content;

    if (level > log_level) return 0;

    t = time(NULL);
    localtime_r(&t, &now);

    now.tm_mon += 1;
    now.tm_year += 1900;

    prefix = snprintf(log_buf, sizeof(log_buf),
            "%04d-%02d-%02d %02d:%02d:%02d %ld ",
            now.tm_year, now.tm_mon, now.tm_mday,
            now.tm_hour, now.tm_min, now.tm_sec,
            // gettid() is only available after glibc 2.30
            syscall(SYS_gettid));

    va_start(ap, fmt);
    content = vsnprintf(log_buf + prefix, sizeof(log_buf) - prefix, fmt, ap);
    va_end(ap);

    n = write(fd, log_buf, prefix + content);
    return n;
}


void net_log_level(int level)
{
    log_level = level;
}


char *util_strchr(const char *s, int c, int len)
{
    int i;
    char *matched;

    matched = NULL;

    for (i=0; i<len; i++)
    {
        if (s[i] == c)
        {
            matched = (char *)&s[i];
            break;
        }
    }

    return matched;
}


/* @needle need to be null-terminated. */
char *util_strstr(char *haystack, char *needle, int len)
{
    char *matched, *cursor;
    int needle_len;

    if (len <= 0) return NULL;

    needle_len = strlen(needle);
    if (needle_len > len) return NULL;

    matched = NULL;

    // match first char within @needle
    cursor = util_strchr(haystack, needle[0], len);
    if (!cursor) return NULL;

    while (cursor + needle_len <= haystack + len)
    {
        if (strncmp(cursor, needle, needle_len) == 0)
        {
            matched = cursor;
            break;
        }
        else
            ++cursor;
    }

    return matched;
}
