#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>  
#include <sys/wait.h>

#include "kvs.h"
#include "constants.h"

static struct HashTable* kvs_table = NULL;

static int running_backups = 0;
static int running_backups_limit = 10;

static int job_backup_number = 0;
static int job_backup_limit = 100;

/// Calculates a timespec from a delay in milliseconds.
/// @param delay_ms Delay in milliseconds.
/// @return Timespec with the given delay.
static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  // terminate all child processes
  while (wait(NULL) > 0);

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  return 0;
}

int kvs_read(int fd, size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  dprintf(fd, "[");
  for (size_t i = 0; i < num_pairs; i++) {
    char* result = read_pair(kvs_table, keys[i]);
    if (result == NULL) {
      dprintf(fd, "(%s,KVSERROR)", keys[i]);
    } else {
      dprintf(fd, "(%s,%s)", keys[i], result);
    }
    free(result);
  }
  dprintf(fd, "]\n");
  return 0;
}

int kvs_delete(int fd, size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        dprintf(fd, "[");
        aux = 1;
      }
      dprintf(fd, "(%s,KVSMISSING)", keys[i]);
    }
  }
  if (aux) {
    dprintf(fd, "]\n");
  }

  return 0;
}

void kvs_show(int fd) {
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      dprintf(fd, "(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next; // Move to the next node
    }
  }
}

int kvs_backup() {
  if (job_backup_number >= job_backup_limit) {
    return -1;
  }

  if (running_backups >= running_backups_limit) {
    wait(NULL);
    running_backups--;
  }

  pid_t pid = fork();

  if (pid < 0) {
    fprintf(stderr, "Failed to fork");
    return -1;
  }
  
  job_backup_number++;

  if (pid) {
    // in parent
    running_backups++;
    // ignore and let it cook
  } else {
    // in child
    
    // TODO: properly implement file name
    char filename[128];
    sprintf(filename, "file-%d.bck", job_backup_number);
    // create or truncate file if needed
    int fd = open(filename, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
    
    if (fd < 0) {
      fprintf(stderr, "Failed to open file.");
      exit(-1);
    }

    kvs_show(fd);
    exit(0);
  }

  return 0;
}

void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}