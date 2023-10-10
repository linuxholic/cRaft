#include <stdio.h>  // printf,fopen,fclose
#include <stdlib.h> // exit

FILE *f;

void __attribute__((constructor)) open_snapshot_file(int argc, char *argv[])
{
    if (argc != 2)
    {
        printf("usage: %s <snapshot>\n", argv[0]);
        exit(EXIT_FAILURE);
    }

    char *path = argv[1];
    f = fopen(path, "r");
    if (f == NULL)
    {
        printf("fail to open '%s'\n", path);
        exit(EXIT_FAILURE);
    }
}

void __attribute__((destructor)) close_snapshot_file()
{
    if (f) fclose(f);
}

int main(int argc, char *argv[])
{
    int buckets_count;
    fread(&buckets_count, sizeof(buckets_count), 1, f);
    printf("buckets count: %d\n", buckets_count);

    int keys_count;
    fread(&keys_count, sizeof(keys_count), 1, f);
    printf("keys count: %d\n", keys_count);

    for (int i = 0; i < keys_count; i++)
    {
        int key_size;
        fread(&key_size, sizeof(key_size), 1, f);
        char *key = malloc(key_size + 1);
        fread(key, sizeof(char), key_size, f);
        key[key_size] = '\0';

        int value_size;
        fread(&value_size, sizeof(value_size), 1, f);
        char *value = malloc(value_size + 1);
        fread(value, sizeof(char), value_size, f);
        value[value_size] = '\0';

        printf("%s -> %s\n", key, value);
    }

    return 0;
}
