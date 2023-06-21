#ifndef _LIST_H_
#define _LIST_H_

#define LIST_FOR_EACH(l, p) \
    for ((p) = (l)->next; (p) != (l); (p) = (p)->next)

#define LIST_FOR_EACH_SAFE(l, p, n) \
    for ((p) = (l)->next, (n) = (p)->next; \
         (p) != (l); (p) = (n), (n) = (p)->next)

#define LIST_TAIL(l, p) (p)->next == (l)

#define LIST_HEAD(ret, head) {    \
    if ((head)->next == (head)) \
        ret = NULL;\
    else{\
        ret = container_of((head)->next, __typeof__(*ret), node);\
    }}


#define container_of(ptr, type, member) \
    (type *)((char *)ptr - offsetof(type, member))

#define list_empty(node)  ((node)->next == (node) ? 1 : 0)
#define list_is_tail(l, n) ((n)->next == (l) ? 1 : 0)

struct list_s {
    struct list_s *prev;
    struct list_s *next;
};
typedef struct list_s list_t;

static inline void list_init(list_t *l)
{
    l->prev = l;
    l->next = l;
}

static inline void list_add(list_t *l, list_t *n)
{
    l->next->prev = n;
    n->next = l->next;
    n->prev = l;
    l->next = n;
}

static inline void list_append(list_t *l, list_t *n)
{
    n->prev = l->prev;
    n->prev->next = n;
    n->next = l;
    l->prev = n;
}

static inline void list_del(list_t *n)
{
    n->prev->next = n->next;
    n->next->prev = n->prev;

    n->next = n;
    n->prev = n;
}

#endif  // _LIST_H_
