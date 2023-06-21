#include <stdlib.h> // malloc
#include <stdint.h> // uint32_t,uint8_t
#include <string.h> // strlen

#include "hash.h"

/*
    hash table implementation
*/

struct hashTable* hashInit(int size)
{
    struct hashTable *t = malloc(sizeof(struct hashTable) * 1);
    t->size = size;
    t->arr = calloc(size, sizeof(struct hashItem*));
    return t;
}

void hashDestroy(struct hashTable *t)
{
    struct hashItem *item, *next;

    for (int i = 0; i < t->size; i++)
    {
        item = t->arr[i];
        if (item == NULL) continue;

        while (item)
        {
            next = item->next;
            free(item);
            item = next;
        }
    }

    free(t);
}

// hash function: FNV-1a
static uint32_t hashString(const char* key, int length)
{
    uint32_t hash = 2166136261u;
    for (int i = 0; i < length; i++) {
        hash ^= (uint8_t)key[i];
        hash *= 16777619;
    }
    return hash;
}

char* hashGet(struct hashTable *table, char *key)
{
    uint32_t hash = hashString(key, strlen(key));
    struct hashItem *i = table->arr[hash % table->size];

    while (i)
    {
        if (strcmp(i->key, key) == 0) return i->value;
        i = i->next;
    }

    return NULL;
}

void hashPut(struct hashTable *table, char *key, char *value)
{
    struct hashItem *pre = NULL;
    uint32_t hash = hashString(key, strlen(key));
    struct hashItem *i = table->arr[hash % table->size];

    while (i)
    {
        // if exist, then just update its value
        if (i->key == key)
        {
            i->value = value;
            return;
        }
        pre = i;
        i = i->next;
    }

    // not exist, create one new HashItem
    struct hashItem *i_new = malloc(sizeof(struct hashItem) * 1);
    i_new->value = value;
    i_new->key = key;
    i_new->next = NULL;

    if (pre == NULL)
    {
        table->arr[hash % table->size] = i_new;
    }
    else {
        pre->next = i_new;
    }

    return;
}

/*
 * three edge cases:
 * 1. empty slot
 * 2. delete first node
 * 3. not found target key
 */
void hashDelete(struct hashTable *table, char *key)
{
    struct hashItem *pre = NULL;
    uint32_t hash = hashString(key, strlen(key));
    struct hashItem *i = table->arr[hash % table->size];

    while (i)
    {
        if (i->key == key) goto found;
        pre = i;
        i = i->next;
    }

    // edge case 1 and 3
    if (i == NULL) return;

found:
    if (pre == NULL) // edge case 2
    {
        table->arr[hash % table->size] = i->next;
    }
    else { // normal case: delete between start and end of list
        pre->next = i->next;
    }

    free(i);
    return;
}
