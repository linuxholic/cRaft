
struct hashItem
{
    char *key;
    char *value;
    struct hashItem *next;
};

struct hashTable
{
    int size;
    struct hashItem **arr;
};

struct hashTable* hashInit(int size);
void hashDestroy(struct hashTable *t);

char* hashGet(struct hashTable *table, char *key);
void hashPut(struct hashTable *table, char *key, char *value);
void hashDelete(struct hashTable *table, char *key);
