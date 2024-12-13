#include "kvs.h"
#include "string.h"

#include <stdio.h>
#include <stdlib.h>
#include <ctype.h>
#include "constants.h"

// Hash function based on key initial.
// @param key Lowercase alphabetical string.
// @return hash.
// NOTE: This is not an ideal hash function, but is useful for test purposes of the project
int hash(const char *key) {
    int firstLetter = tolower(key[0]);
    if (firstLetter >= 'a' && firstLetter <= 'z') {
        return firstLetter - 'a';
    } else if (firstLetter >= '0' && firstLetter <= '9') {
        return firstLetter - '0';
    }
    return -1; // Invalid index for non-alphabetic or number strings
}

int compare_strings(const void *a, const void *b) {
    const char *strA = (const char *)a;
    const char *strB = (const char *)b;
    return strcmp(strA, strB);
}

struct HashTable* create_hash_table() {
  HashTable *ht = malloc(sizeof(HashTable));
  if (!ht) return NULL;
  for (int i = 0; i < TABLE_SIZE; i++) {
      ht->table[i] = NULL;
  }
  pthread_mutex_init(&ht->global_lock, NULL);
  return ht;
}

int write_pair(HashTable *ht, const char *key, const char *value) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];

    // Search for the key node
    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            free(keyNode->value);
            keyNode->value = strdup(value);
            return 0;
        }
        keyNode = keyNode->next; // Move to the next node
    }

    // Key not found, create a new key node
    keyNode = malloc(sizeof(KeyNode));
    keyNode->key = strdup(key); // Allocate memory for the key
    keyNode->value = strdup(value); // Allocate memory for the value
    keyNode->next = ht->table[index]; // Link to existing nodes
    pthread_rwlock_init(&keyNode->lock, NULL);
    pthread_rwlock_wrlock(&keyNode->lock);
    
    ht->table[index] = keyNode; // Place new key node at the start of the list
    return 0;
}

void keys_wrlock(HashTable *ht, size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
    int exists[TABLE_SIZE];

    char sorted_keys[MAX_WRITE_SIZE][MAX_STRING_SIZE];

    memcpy(sorted_keys, keys, sizeof(char) * MAX_STRING_SIZE * MAX_WRITE_SIZE);

    qsort(sorted_keys, num_pairs, MAX_STRING_SIZE, compare_strings);

    for (size_t i = 0; i < num_pairs; i++) {
        int key_hash = hash(sorted_keys[i]);
        if (exists[key_hash]) continue;

        KeyNode *key_node = ht->table[key_hash];
        if (key_node) pthread_rwlock_wrlock(&key_node->lock);
        exists[key_hash] = 1;
    }
}

void keys_rdlock(HashTable *ht, size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  int exists[TABLE_SIZE];

  for (size_t i = 0; i < num_pairs; i++) {
    int key_hash = hash(keys[i]);
    if (exists[key_hash]) continue;

    KeyNode *key_node = ht->table[key_hash];
    if (key_node) pthread_rwlock_rdlock(&key_node->lock);
    exists[key_hash] = 1;
  }
}

void keys_rdlock_global(HashTable *ht) {
  for (size_t i = 0; i < TABLE_SIZE; i++) {
    KeyNode *key_node = ht->table[i];
    if (key_node) pthread_rwlock_rdlock(&key_node->lock);
  }
}

void keys_unlock_global(HashTable *ht) {
  for (size_t i = 0; i < TABLE_SIZE; i++) {
    KeyNode *key_node = ht->table[i];
    if (key_node) pthread_rwlock_unlock(&key_node->lock);
  }
}

void keys_unlock(HashTable *ht, size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  int exists[TABLE_SIZE];

  for (size_t i = 0; i < num_pairs; i++) {
    int key_hash = hash(keys[i]);
    if (exists[key_hash]) continue;

    KeyNode *key_node = ht->table[key_hash];
    if (key_node) pthread_rwlock_unlock(&key_node->lock);
    exists[key_hash] = 1;
  }
}

char* read_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    char* value;

    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            value = strdup(keyNode->value);
            return value; // Return copy of the value if found
        }
        keyNode = keyNode->next; // Move to the next node
    }
    return NULL; // Key not found
}

int delete_pair(HashTable *ht, const char *key) {
    int index = hash(key);
    KeyNode *keyNode = ht->table[index];
    KeyNode *prevNode = NULL;

    // Search for the key node
    while (keyNode != NULL) {
        if (strcmp(keyNode->key, key) == 0) {
            // Key found; delete this node
            if (prevNode == NULL) {
                // Node to delete is the first node in the list
                ht->table[index] = keyNode->next; // Update the table to point to the next node
            } else {
                // Node to delete is not the first; bypass it
                prevNode->next = keyNode->next; // Link the previous node to the next node
            }
            // Free the memory allocated for the key and value
            free(keyNode->key);
            free(keyNode->value);
            free(keyNode); // Free the key node itself
            return 0; // Exit the function
        }
        prevNode = keyNode; // Move prevNode to current node
        keyNode = keyNode->next; // Move to the next node
    }
    
    return 1;
}

void free_table(HashTable *ht) {
    for (int i = 0; i < TABLE_SIZE; i++) {
        KeyNode *keyNode = ht->table[i];
        while (keyNode != NULL) {
            KeyNode *temp = keyNode;
            keyNode = keyNode->next;
            free(temp->key);
            free(temp->value);
            free(temp);
        }
    }
    pthread_mutex_destroy(&ht->global_lock);
    free(ht);
}
