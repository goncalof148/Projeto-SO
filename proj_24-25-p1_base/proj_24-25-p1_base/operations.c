#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <time.h>
#include <unistd.h>
#include <fcntl.h>  
#include <sys/wait.h>

#include "kvs.h"
#include "constants.h"
#include <errno.h>
#include <limits.h>

static struct HashTable* kvs_table = NULL;

static int running_backups = 0;
static int running_backups_limit = 10;
static pthread_mutex_t running_backups_lock;

static int job_backup_number = 0;

static struct timespec delay_to_timespec(unsigned int delay_ms) {
  return (struct timespec){delay_ms / 1000, (delay_ms % 1000) * 1000000};
}

int kvs_init() {
  if (kvs_table != NULL) {
    fprintf(stderr, "KVS state has already been initialized\n");
    return 1;
  }

  kvs_table = create_hash_table();
  pthread_mutex_init(&running_backups_lock, NULL);
  return kvs_table == NULL;
}

int kvs_terminate() {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  while (wait(NULL) > 0);

  free_table(kvs_table);
  return 0;
}

int kvs_write(size_t num_pairs, char keys[][MAX_STRING_SIZE], char values[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }

  keys_wrlock(kvs_table, num_pairs, keys);

  for (size_t i = 0; i < num_pairs; i++) {
    if (write_pair(kvs_table, keys[i], values[i]) != 0) {
      fprintf(stderr, "Failed to write keypair (%s,%s)\n", keys[i], values[i]);
    }
  }

  keys_unlock(kvs_table, num_pairs, keys);

  return 0;
}

int kvs_read(int fd, size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
    if (kvs_table == NULL) {
        fprintf(stderr, "KVS state must be initialized\n");
        return 1;
    }

    // Sort the keys array in ascending lexicographical order
    qsort(keys, num_pairs, MAX_STRING_SIZE, compare_strings);

    keys_wrlock(kvs_table, num_pairs, keys);

    dprintf(fd, "[");
    for (size_t i = 0; i < num_pairs; i++) {
        char *result = read_pair(kvs_table, keys[i]);
        if (result == NULL) {
            dprintf(fd, "(%s,KVSERROR)", keys[i]);
        } else {
            dprintf(fd, "(%s,%s)", keys[i], result);
        }
        free(result);
    }
    dprintf(fd, "]\n");

    keys_unlock(kvs_table, num_pairs, keys);

    return 0;
}


int kvs_delete(int fd, size_t num_pairs, char keys[][MAX_STRING_SIZE]) {
  if (kvs_table == NULL) {
    fprintf(stderr, "KVS state must be initialized\n");
    return 1;
  }
  int aux = 0;

  keys_wrlock(kvs_table, num_pairs, keys);

  for (size_t i = 0; i < num_pairs; i++) {
    if (delete_pair(kvs_table, keys[i]) != 0) {
      if (!aux) {
        dprintf(fd, "[");
        aux = 1;
      }
      dprintf(fd, "(%s,KVSMISSING)", keys[i]);
    }
  }

  keys_unlock(kvs_table, num_pairs, keys);

  if (aux) {
    dprintf(fd, "]\n");
  }
  return 0;
}

void kvs_show(int fd) {
  keys_rdlock_global(kvs_table);
  for (int i = 0; i < TABLE_SIZE; i++) {
    KeyNode *keyNode = kvs_table->table[i];
    while (keyNode != NULL) {
      dprintf(fd, "(%s, %s)\n", keyNode->key, keyNode->value);
      keyNode = keyNode->next; 
    }
  }
  keys_unlock_global(kvs_table);
}

int kvs_backup(const char *base_file_path) {
  // 2
  // A...............
  //     B.......|
  //        C|   |...
  //             D
    pthread_mutex_lock(&running_backups_lock);
    if (running_backups >= running_backups_limit) {
        wait(NULL);
        running_backups--;
    }

    pid_t pid = fork();
    if (pid < 0) {
        fprintf(stderr, "Failed to fork\n");
        pthread_mutex_unlock(&running_backups_lock);
        return -1;
    }

    job_backup_number++;

    if (pid > 0) {
        running_backups++;
        pthread_mutex_unlock(&running_backups_lock);
    } else {
        char filename[PATH_MAX];
        
        snprintf(filename, sizeof(filename), "%s-%d.bck", base_file_path, job_backup_number);

        int fd = open(filename, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
        if (fd < 0) {
            fprintf(stderr, "Failed to open backup file '%s': %s\n", filename, strerror(errno));
            _exit(EXIT_FAILURE);
        }

        kvs_show(fd);

        close(fd);  
        _exit(EXIT_SUCCESS);
    }

    return 0;
}


void kvs_wait(unsigned int delay_ms) {
  struct timespec delay = delay_to_timespec(delay_ms);
  nanosleep(&delay, NULL);
}
