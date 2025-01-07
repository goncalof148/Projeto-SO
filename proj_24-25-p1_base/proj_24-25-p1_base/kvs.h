#ifndef KEY_VALUE_STORE_H
#define KEY_VALUE_STORE_H

#define TABLE_SIZE 26

#include <stddef.h>
#include <pthread.h>
#include "constants.h"
#include "string.h"

typedef struct KeyNode {
    char *key;
    char *value;
    struct KeyNode *next;
    pthread_rwlock_t lock;
} KeyNode;

typedef struct HashTable {
    KeyNode *table[TABLE_SIZE];
    pthread_mutex_t global_lock;
} HashTable;

/// Creates a new event hash table.
/// @return Newly created hash table, NULL on failure
struct HashTable *create_hash_table();

/// Appends a new key value pair to the hash table.
/// @param ht Hash table to be modified.
/// @param key Key of the pair to be written.
/// @param value Value of the pair to be written.
/// @return 0 if the node was appended successfully, 1 otherwise.
int write_pair(HashTable *ht, const char *key, const char *value);

/// Deletes the value of given key.
/// @param ht Hash table to delete from.
/// @param key Key of the pair to be deleted.
/// @return 0 if the node was deleted successfully, 1 otherwise.
char* read_pair(HashTable *ht, const char *key);

/// Appends a new node to the list.
/// @param list Event list to be modified.
/// @param key Key of the pair to read.
/// @return 0 if the node was appended successfully, 1 otherwise.
int delete_pair(HashTable *ht, const char *key);

/// Frees the hashtable.
/// @param ht Hash table to be deleted.
void free_table(HashTable *ht);

/// Locks the list of keys for writing
void keys_wrlock(HashTable *ht, size_t num_pairs, char keys[][MAX_STRING_SIZE]);
/// Locks the list of keys for reading
void keys_rdlock(HashTable *ht, size_t num_pairs, char keys[][MAX_STRING_SIZE]);
/// Unlocks the list of keys for writing
void keys_unlock(HashTable *ht, size_t num_pairs, char keys[][MAX_STRING_SIZE]);
/// Locks all keys for reading
void keys_rdlock_global(HashTable *ht);
/// Unocks all keys for reading
void keys_unlock_global(HashTable *ht);

// Comparison function for qsort
int compare_strings(const void *a, const void *b);
#endif  // KVS_H
