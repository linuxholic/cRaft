
struct hashItem
{
    union {
        char *_str;
        int _int;
    } key;

    void *value;
    struct hashItem *next;
};

struct hashTable
{
    int size;
    struct hashItem **arr;
};

struct hashTable* hashInit(int size);
void hashDestroy(struct hashTable *t);

void* hashGet(struct hashTable *table, char *key);
void hashPut(struct hashTable *table, char *key, void *value);
void hashDelete(struct hashTable *table, char *key);

void* hashGetInt(struct hashTable *table, int key);
void hashPutInt(struct hashTable *table, int key, void *value);
void hashDeleteInt(struct hashTable *table, int key);
