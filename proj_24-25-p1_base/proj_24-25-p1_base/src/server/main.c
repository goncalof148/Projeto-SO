#include <dirent.h>
#include <fcntl.h>
#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <sys/types.h>
#include <sys/wait.h>
#include <unistd.h>
#include <sys/stat.h>
#include <semaphore.h>
#include <errno.h>
#include <signal.h>

#include "../common/constants.h"
#include "../common/protocol.h"
#include "../common/io.h"
#include "constants.h"
#include "io.h"
#include "operations.h"
#include "parser.h"
#include "pthread.h"
#include "kvs.h"

struct SharedData {
  DIR *dir;
  char *dir_name;
  pthread_mutex_t directory_mutex;
};


struct HostThreadData {
  char const* host_pipe_path;
  int host_pipe_fd;
};

typedef struct Client {
    int req_pipe;         // File descriptor for request pipe
    int resp_pipe;        // File descriptor for response pipe
    int notif_pipe;       // File descriptor for notification pipe
    int id;               // Unique client identifier (for debugging only)
    pthread_t thread;   // Thread handling the client
} Client;

Client clients_buf[S_VALUE];    // Array to store client information
Client *thread_clients[S_VALUE]; // Array of currently executing clients, per thread

int total_client_count = 0;           // Number of total clients

int client_cons_idx;
int client_prod_idx;
sem_t client_cons_sem;
sem_t client_prod_sem;

int signal_usr1_flag = 0;

pthread_mutex_t client_mutex = PTHREAD_MUTEX_INITIALIZER;  // Mutex to protect client list

pthread_mutex_t lock = PTHREAD_MUTEX_INITIALIZER;
pthread_mutex_t n_current_backups_lock = PTHREAD_MUTEX_INITIALIZER;

size_t active_backups = 0; // Number of active backups
size_t max_backups;        // Maximum allowed simultaneous backups
size_t max_threads;        // Maximum allowed simultaneous threads
char *jobs_directory = NULL;

int filter_job_files(const struct dirent *entry) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot != NULL && strcmp(dot, ".job") == 0) {
    return 1; // Keep this file (it has the .job extension)
  }
  return 0;
}

static int entry_files(const char *dir, struct dirent *entry, char *in_path,
                       char *out_path) {
  const char *dot = strrchr(entry->d_name, '.');
  if (dot == NULL || dot == entry->d_name || strlen(dot) != 4 ||
      strcmp(dot, ".job")) {
    return 1;
  }

  if (strlen(entry->d_name) + strlen(dir) + 2 > MAX_JOB_FILE_NAME_SIZE) {
    fprintf(stderr, "%s/%s\n", dir, entry->d_name);
    return 1;
  }

  strcpy(in_path, dir);
  strcat(in_path, "/");
  strcat(in_path, entry->d_name);

  strcpy(out_path, in_path);
  strcpy(strrchr(out_path, '.'), ".out");

  return 0;
}

static int run_job(int in_fd, int out_fd, char *filename) {
  size_t file_backups = 0;
  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;

    switch (get_next(in_fd)) {
    case CMD_WRITE:
      num_pairs =
          parse_write(in_fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_write(num_pairs, keys, values)) {
        write_str(STDERR_FILENO, "Failed to write pair\n");
      }
      break;

    case CMD_READ:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_read(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to read pair\n");
      }
      break;

    case CMD_DELETE:
      num_pairs =
          parse_read_delete(in_fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

      if (num_pairs == 0) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (kvs_delete(num_pairs, keys, out_fd)) {
        write_str(STDERR_FILENO, "Failed to delete pair\n");
      }
      break;

    case CMD_SHOW:
      kvs_show(out_fd);
      break;

    case CMD_WAIT:
      if (parse_wait(in_fd, &delay, NULL) == -1) {
        write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
        continue;
      }

      if (delay > 0) {
        printf("Waiting %d seconds\n", delay / 1000);
        kvs_wait(delay);
      }
      break;

    case CMD_BACKUP:
      pthread_mutex_lock(&n_current_backups_lock);
      if (active_backups >= max_backups) {
        wait(NULL);
      } else {
        active_backups++;
      }
      pthread_mutex_unlock(&n_current_backups_lock);
      int aux = kvs_backup(++file_backups, filename, jobs_directory);

      if (aux < 0) {
        write_str(STDERR_FILENO, "Failed to do backup\n");
      } else if (aux == 1) {
        return 1;
      }
      break;

    case CMD_INVALID:
      write_str(STDERR_FILENO, "Invalid command. See HELP for usage\n");
      break;

    case CMD_HELP:
      write_str(STDOUT_FILENO,
                "Available commands:\n"
                "  WRITE [(key,value)(key2,value2),...]\n"
                "  READ [key,key2,...]\n"
                "  DELETE [key,key2,...]\n"
                "  SHOW\n"
                "  WAIT <delay_ms>\n"
                "  BACKUP\n" // Not implemented
                "  HELP\n");

      break;

    case CMD_EMPTY:
      break;

    case EOC:
      printf("EOF\n");
      return 0;
    }
  }
}

// frees arguments
static void *get_file(void *arguments) {
  struct SharedData *thread_data = (struct SharedData *)arguments;
  DIR *dir = thread_data->dir;
  char *dir_name = thread_data->dir_name;

  if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to lock directory_mutex\n");
    return NULL;
  }

  struct dirent *entry;
  char in_path[MAX_JOB_FILE_NAME_SIZE], out_path[MAX_JOB_FILE_NAME_SIZE];
  while ((entry = readdir(dir)) != NULL) {
    if (entry_files(dir_name, entry, in_path, out_path)) {
      continue;
    }

    if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to unlock directory_mutex\n");
      return NULL;
    }

    int in_fd = open(in_path, O_RDONLY);
    if (in_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open input file: ");
      write_str(STDERR_FILENO, in_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out_fd = open(out_path, O_WRONLY | O_CREAT | O_TRUNC, 0666);
    if (out_fd == -1) {
      write_str(STDERR_FILENO, "Failed to open output file: ");
      write_str(STDERR_FILENO, out_path);
      write_str(STDERR_FILENO, "\n");
      pthread_exit(NULL);
    }

    int out = run_job(in_fd, out_fd, entry->d_name);

    close(in_fd);
    close(out_fd);

    if (out) {
      if (closedir(dir) == -1) {
        fprintf(stderr, "Failed to close directory\n");
        return 0;
      }

      exit(0);
    }

    if (pthread_mutex_lock(&thread_data->directory_mutex) != 0) {
      fprintf(stderr, "Thread failed to lock directory_mutex\n");
      return NULL;
    }
  }

  if (pthread_mutex_unlock(&thread_data->directory_mutex) != 0) {
    fprintf(stderr, "Thread failed to unlock directory_mutex\n");
    return NULL;
  }

  pthread_exit(NULL);
}

int send_response(struct Client *client, int opcode, char response) {
  char buf[2];
  buf[0] = '0' + opcode;
  buf[1] = response;
  ssize_t n = write(client->resp_pipe, buf, 2);
  
  if (n < 2) {
    return 1;
  }

  return 0;
}

void add_client(int req_pipe, int resp_pipe, int notif_pipe) {
    sem_wait(&client_prod_sem);

    pthread_mutex_lock(&client_mutex);
    clients_buf[client_prod_idx].req_pipe = req_pipe;
    clients_buf[client_prod_idx].resp_pipe = resp_pipe;
    clients_buf[client_prod_idx].notif_pipe = notif_pipe;
    clients_buf[client_prod_idx].id = total_client_count++;
    printf("Client added: %d\n", clients_buf[client_prod_idx].id);
    client_prod_idx = (client_prod_idx + 1) % S_VALUE;
    pthread_mutex_unlock(&client_mutex);
    
    sem_post(&client_cons_sem);
}

void close_client(struct Client *client, int thread_id) {
    close(client->req_pipe);
    close(client->resp_pipe);
    close(client->notif_pipe);

    client->req_pipe = -1;

    thread_clients[thread_id] = NULL;
}

void remove_client(struct Client *client, int thread_id) {
    pthread_mutex_lock(&client_mutex);
     // Log client removal
    printf("Removing client with id %d\n", client->id);
    
    kvs_unsubscribe_client(client->notif_pipe);

    if (client->req_pipe == -1) {
      pthread_mutex_unlock(&client_mutex);
      return;
    }

    // Close file descriptors
    close_client(client, thread_id);

    pthread_mutex_unlock(&client_mutex);
    sem_post(&client_prod_sem);
}

static void process_client(struct Client *client) {
  send_response(client, OP_CODE_CONNECT, '0');

  for (;;) {
    char req_buf[121];
    char *sub_buf = req_buf + 1;
    int n = read(client->req_pipe, req_buf, sizeof(req_buf));
    int res = -1;

    if (n <= 0) {
      return;
    }

    int opcode = (int)(req_buf[0] - '0');
    switch (opcode) {
      case OP_CODE_DISCONNECT:
        printf("Closing client\n");
        send_response(client, OP_CODE_DISCONNECT, '0');
        return;
      case OP_CODE_SUBSCRIBE:
        res = kvs_subscribe(sub_buf, client->notif_pipe);
        send_response(client, OP_CODE_SUBSCRIBE, '0' + res);
        break;
      case OP_CODE_UNSUBSCRIBE:
        res = kvs_unsubscribe(sub_buf, client->notif_pipe);
        send_response(client, OP_CODE_UNSUBSCRIBE, '0' + res);
        break;
      default:
        fprintf(stderr, "Error processing request: unknown opcode %d\n", opcode);
        break;
    }
  }
}

static void get_client(void *arg) {
  int thread_id = *(int *)arg;
  free(arg);

  for (;;) {
    sem_wait(&client_cons_sem);

    pthread_mutex_lock(&client_mutex); 
    // NOTE: passes by value, so client is a copy of the one in buffer.
    // this is necessary for a producer-consumer buffer 
    struct Client client = clients_buf[client_cons_idx];
    client_cons_idx = (client_cons_idx + 1) % S_VALUE;
    thread_clients[thread_id] = &client;
    pthread_mutex_unlock(&client_mutex);

    process_client(&client);
    remove_client(&client, thread_id);
  }
}

void init_clients() {
  sem_init(&client_cons_sem, 0, 0);
  sem_init(&client_prod_sem, 0, S_VALUE);

  for (size_t i = 0; i < S_VALUE; i++) {
    clients_buf[i].req_pipe = -1;
    clients_buf[i].resp_pipe = -1;
    clients_buf[i].notif_pipe = -1;
    
    int *client_data = (int *)malloc(sizeof(int));
    *client_data = i;

    if (pthread_create(&clients_buf[i].thread, NULL, (void *)get_client, (void *)client_data) != 0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      return;
    }
  }
}

void close_all_signal() {
  signal_usr1_flag = 1;
  signal(SIGUSR1, close_all_signal);
}
  
void welcome_clients(void* arg) {
  int fserv, frep, fresp, fnot;
  ssize_t n;

  char buf[121], rep_pipe_path[41] = {0}, resp_pipe_path[41] = {0}, notifications_pipe_path[41] = {0};

  struct HostThreadData *data = (struct HostThreadData *) (arg);
  fserv = data->host_pipe_fd;

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  pthread_sigmask(SIG_UNBLOCK, &set, NULL);

  signal(SIGUSR1, close_all_signal);

  for (;;) {
    if (signal_usr1_flag) {
      signal_usr1_flag = 0;
      
      pthread_mutex_lock(&client_mutex);
      for (int i = 0; i < S_VALUE; i++) {
        if (thread_clients[i] == NULL) continue;
        close_client(thread_clients[i], i);
      }
      pthread_mutex_unlock(&client_mutex); 
    }

    int interrupt = 0; 
    n = read_all(fserv, buf, 121, &interrupt);
    
    if (interrupt) {
      continue;
    }

    if (n <= 0) break;
    printf("%s\n", buf);

    if (buf[0] != '1') return;
    
    strncpy(rep_pipe_path, buf + 1, 40);
    rep_pipe_path[40] = '\0';

    strncpy(resp_pipe_path, buf + 41, 40);
    resp_pipe_path[40] = '\0';
    
    strncpy(notifications_pipe_path, buf + 81, 40);
    notifications_pipe_path[40] = '\0';

    if ((frep = open(rep_pipe_path, O_RDONLY)) < 0) {
      perror("Error opening the named pipe");
      exit(1);
    } else {
      printf("REP pipe opened: %s\n", rep_pipe_path);
    }

    if ((fresp = open(resp_pipe_path, O_WRONLY)) < 0) {
      perror("Error opening the named pipe");
      exit(1);
    } else{
      printf("RESP pipe opened: %s\n", resp_pipe_path);
    }

    if ((fnot = open(notifications_pipe_path, O_WRONLY)) < 0) {
      perror("Error opening the named pipe");
      exit(1);
    } else{
      printf("NOT pipe opened: %s\n", notifications_pipe_path);
    }

    add_client(frep, fresp, fnot);
  }
}

static void dispatch_threads(DIR *dir, char const* host_pipe_path) {
  pthread_t *threads = malloc(max_threads * sizeof(pthread_t));
  int fserv;

  if (threads == NULL) {
    fprintf(stderr, "Failed to allocate memory for threads\n");
    return;
  }

  struct SharedData thread_data = {dir, jobs_directory,
                                   PTHREAD_MUTEX_INITIALIZER};

  sigset_t set;
  sigemptyset(&set);
  sigaddset(&set, SIGUSR1);
  sigaddset(&set, SIGPIPE);
  pthread_sigmask(SIG_BLOCK, &set, NULL);

  init_clients();

  unlink(host_pipe_path);
  if (mkfifo(host_pipe_path, 0666) < 0){
    printf("Path: %s\n", host_pipe_path);
    perror("Error creating named pipe");
    pthread_mutex_destroy(&thread_data.directory_mutex);
    free(threads);
    return;
  }
  
  printf("Pipe created: %s\n", host_pipe_path);

  if ((fserv = open(host_pipe_path, O_RDWR)) < 0) {
    perror("Error opening the named pipe");
    pthread_mutex_destroy(&thread_data.directory_mutex);
    free(threads);
    return;
  }

  printf("Server listening on pipe: %s\n", host_pipe_path);

  struct HostThreadData data;
  data.host_pipe_path = host_pipe_path;
  data.host_pipe_fd = fserv;
  
  pthread_t host_thread;
  if (pthread_create(&host_thread, NULL, (void*)welcome_clients, (void*)(&data)) != 0) {
      fprintf(stderr, "Failed to create welcome thread\n");
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads); 
      return;
  }

  for (size_t i = 0; i < max_threads; i++) {
    if (pthread_create(&threads[i], NULL, get_file, (void *)&thread_data) !=
        0) {
      fprintf(stderr, "Failed to create thread %zu\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  for (unsigned int i = 0; i < max_threads; i++) {
    if (pthread_join(threads[i], NULL) != 0) {
      fprintf(stderr, "Failed to join thread %u\n", i);
      pthread_mutex_destroy(&thread_data.directory_mutex);
      free(threads);
      return;
    }
  }

  if (pthread_mutex_destroy(&thread_data.directory_mutex) != 0) {
    fprintf(stderr, "Failed to destroy directory_mutex\n");
  }

  free(threads);
  close(fserv);
}


int main(int argc, char **argv) {
  if (argc < 4) {
    write_str(STDERR_FILENO, "Usage: ");
    write_str(STDERR_FILENO, argv[0]);
    write_str(STDERR_FILENO, " <jobs_dir>");
    write_str(STDERR_FILENO, " <max_threads>");
    write_str(STDERR_FILENO, " <max_backups> \n");
    return 1;
  }

  jobs_directory = argv[1];

  char *endptr;
  max_backups = strtoul(argv[3], &endptr, 10);

  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_proc value\n");
    return 1;
  }

  max_threads = strtoul(argv[2], &endptr, 10);
  
  if (*endptr != '\0') {
    fprintf(stderr, "Invalid max_threads value\n");
    return 1;
  }

  if (max_backups <= 0) {
    write_str(STDERR_FILENO, "Invalid number of backups\n");
    return 0;
  }

  if (max_threads <= 0) {
    write_str(STDERR_FILENO, "Invalid number of threads\n");
    return 0;
  }

  if (kvs_init()) {
    write_str(STDERR_FILENO, "Failed to initialize KVS\n");
    return 1;
  }

  DIR *dir = opendir(argv[1]);
  if (dir == NULL) {
    fprintf(stderr, "Failed to open directory: %s\n", argv[1]);
    return 0;
  }

  dispatch_threads(dir, argv[4]);

  if (closedir(dir) == -1) {
    fprintf(stderr, "Failed to close directory\n");
    return 0;
  }

  while (active_backups > 0) {
    wait(NULL);
    active_backups--;
  }

  kvs_terminate();
  printf("Closing pipe: %s\n", argv[4]);
  unlink(argv[4]);

  return 0;
}
