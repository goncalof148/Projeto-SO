#define _GNU_SOURCE

#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <string.h>
#include <fcntl.h>
#include <dirent.h>    
#include <sys/stat.h>   
#include <errno.h> 

#include "constants.h"
#include "parser.h"
#include "operations.h"

#include <pthread.h>

typedef struct {
  char file_path[128];
  char file_path_output[128];
  int result;  
}  job_thread_args_t;

int has_job_extension(const char *filename) {
    const char *dot = strrchr(filename, '.');
    return (dot && strcmp(dot, ".job") == 0);
}

int process_job(char *input_file_path, char *output_file_path) {
  // if (fgets(file_path, sizeof(file_path), stdin) == NULL) {
  //   fprintf(stderr, "Error reading file path\n");
  //   return EXIT_FAILURE;
  // }

  int fd_input = open(input_file_path, O_RDONLY);
  if (fd_input == -1) {
      fprintf(stderr, "Error opening input .job file '%s': %s\n", input_file_path, strerror(errno));
      return EXIT_FAILURE;
  }

  int fd_output = open(output_file_path, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
  if (fd_output == -1) {
      fprintf(stderr, "Error opening/creating output file '%s': %s\n", output_file_path, strerror(errno));
      close(fd_input);
      return EXIT_FAILURE;
  }

  printf("Processing file: %s\n", input_file_path);
  fflush(stdout);

  while (1) {
      char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
      char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
      unsigned int delay;
      size_t num_pairs;
      off_t last_position = lseek(fd_input, 0, SEEK_CUR);
      if (last_position == (off_t)-1) {
          perror("lseek failed");
          break;
      }

      switch (get_next(fd_input)) { 

          case CMD_WRITE:
              if (lseek(fd_input, last_position + 6, SEEK_SET) == (off_t)-1) { 
                  perror("lseek failed");
                  break;
              }
              num_pairs = parse_write(fd_input, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
              if (num_pairs == 0) {
                  fprintf(stderr, "Invalid WRITE command. See HELP for usage\n");
                  continue;
              }

              if (kvs_write(num_pairs, keys, values)) {
                  fprintf(stderr, "Failed to write pair\n");
              }

              break;

          case CMD_READ:
              if (lseek(fd_input, last_position + 5, SEEK_SET) == (off_t)-1) { 
                  perror("lseek failed");
                  break;
              }
              num_pairs = parse_read_delete(fd_input, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
              if (num_pairs == 0) {
                  fprintf(stderr, "Invalid READ command. See HELP for usage\n");
                  continue;
              }

              if (kvs_read(fd_output, num_pairs, keys)) { 
                  fprintf(stderr, "Failed to read pair\n");
              }
              break;

          case CMD_DELETE:
              if (lseek(fd_input, last_position + 7, SEEK_SET) == (off_t)-1) { 
                  perror("lseek failed");
                  break;
              }
              num_pairs = parse_read_delete(fd_input, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

              if (num_pairs == 0) {
                  fprintf(stderr, "Invalid DELETE command. See HELP for usage\n");
                  continue;
              }

              if (kvs_delete(fd_output, num_pairs, keys)) { 
                  fprintf(stderr, "Failed to delete pair\n");
              }
              break;

          case CMD_SHOW:
              kvs_show(fd_output); 
              break;

          case CMD_WAIT:
              if (lseek(fd_input, last_position + 5, SEEK_SET) == (off_t)-1) { 
                  perror("lseek failed");
                  break;
              }
              if (parse_wait(fd_input, &delay, NULL) == -1) {
                  fprintf(stderr, "Invalid WAIT command. See HELP for usage\n");
                  continue;
              }

              if (delay > 0) {
                  dprintf(fd_output, "Waiting...\n");
                  kvs_wait(delay);
              }
              break;

          case CMD_BACKUP:
              if (kvs_backup()) {
                  fprintf(stderr, "Failed to perform backup.\n");
              }
              break;

          case CMD_INVALID:
              fprintf(stderr, "Invalid command. See HELP for usage\n");
              break;

          case CMD_HELP:
              printf( 
                  "Available commands:\n"
                  "  WRITE [(key,value)(key2,value2),...]\n"
                  "  READ [key,key2,...]\n"
                  "  DELETE [key,key2,...]\n"
                  "  SHOW\n"
                  "  WAIT <delay_ms>\n"
                  "  BACKUP\n" 
                  "  HELP\n"
              );
              break;

          case CMD_EMPTY:
              break;

          case EOC:
              printf("Done!\n\n");
              close(fd_input);
              close(fd_output);
              return EXIT_SUCCESS;
          default:
              fprintf(stderr, "Unknown command encountered.\n");
              break;
        }
    }

    return EXIT_FAILURE;

}

void *process_job_thread(void *arg) {
  job_thread_args_t *targ = (job_thread_args_t *) arg;
  
  targ->result = process_job(targ->file_path, targ->file_path_output);
  return &targ->result;
}


int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <directory_path> <backups_number>\n", argv[0]);
        return EXIT_FAILURE;
    }

    char dir_path[1024];
    strncpy(dir_path, argv[1], sizeof(dir_path) - 1);
    dir_path[sizeof(dir_path) - 1] = '\0';

    int backups_number = atoi(argv[2]);
    printf("Number of backups: %d\n", backups_number);
    fflush(stdout);

    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return EXIT_FAILURE;
    }

    DIR *dir = opendir(dir_path);
    if (dir == NULL) {
        perror("Error opening directory");
        kvs_terminate();
        return EXIT_FAILURE;
    }

    // FIXME: should not be assuming arbitrary max size limit
    pthread_t thread_ids[16];
    job_thread_args_t args[16];

    struct dirent *entry;
    int job_id = 0;
    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type != DT_REG && entry->d_type != DT_UNKNOWN) {
            continue;
        }

        if (!has_job_extension(entry->d_name)) {
            continue;
        }

        char input_file_path[2048];
        if (dir_path[strlen(dir_path) - 1] == '/') {
            snprintf(input_file_path, sizeof(input_file_path), "%s%s", dir_path, entry->d_name);
        } else {
            snprintf(input_file_path, sizeof(input_file_path), "%s/%s", dir_path, entry->d_name);
        }

        char output_file_path[2048];
        strncpy(output_file_path, input_file_path, sizeof(output_file_path) - 1);
        output_file_path[sizeof(output_file_path) - 1] = '\0';

        char *dot_position = strstr(output_file_path, ".job");
        if (dot_position != NULL) {
            *dot_position = '\0'; 
        }
        strncat(output_file_path, ".output", sizeof(output_file_path) - strlen(output_file_path) - 1);

        job_thread_args_t *arg = &args[job_id];
        strcpy(arg->file_path, input_file_path);
        // arg->file_path = file_path;
        strcpy(arg->file_path_output, output_file_path);
        // arg->file_path_output = file_path_output;
        arg->result = 0;

        job_id++;
    }

    closedir(dir);

    for (int i = 0; i < job_id; i++) {
        // process_job(input_file_path, output_file_path);
        job_thread_args_t *arg = &args[i];

        if (pthread_create(&thread_ids[i], NULL, process_job_thread, arg) != 0) {
            fprintf(stderr, "Failed to create thread");
            continue;
        }
    }

    for (int i = 0; i < job_id; i++) {
      pthread_join(thread_ids[i], NULL);
    }

    fflush(stdout);

    kvs_terminate();
    return EXIT_SUCCESS;

    // for (int i = 0; i < dir_size; i++) {
    //   if (i == 0) {
    //     file_path = "./jobs/test1.job";
    //     file_path_output = "./jobs/test1.out";
    //   } else {
    //     file_path = "./jobs/test2.job";
    //     file_path_output = "./jobs/test2.out";
    //   }
}
