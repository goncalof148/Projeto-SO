#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include <string.h>
#include <fcntl.h>

int main(int argc, char *argv[]) {
  char file_path[1024];
  strcpy(file_path,argv[1]);
  int backups_number = atoi(argv[2]);
  char file_path_output[1024];

  if (argc < 2) {
        fprintf(stderr, "Usage: %s <backups_number>\n", argv[0]);
        return 1;
  }

  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }

  printf("%d\n", backups_number);
  //printf("> ");
  fflush(stdout);

  file_path[strcspn(file_path, "\n")] = '\0';
  strcpy(file_path_output,file_path);
  char *dot_position = strstr(file_path_output, ".job");
  if (dot_position != NULL) {
      *dot_position = '\0';
  }
  strcat(file_path_output, ".output");
  int fd = open(file_path, O_RDONLY);
  int fd2 = open(file_path_output, O_CREAT | O_TRUNC | O_WRONLY, S_IRUSR | S_IWUSR);
  
  if (fd == -1) {
    perror("Error opening file");
    return EXIT_FAILURE;
  }

  if (fd2 == -1) {
    perror("Error opening file");
    return EXIT_FAILURE;
  }

  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;
    off_t last_position = lseek(fd, 0, SEEK_CUR);
    switch (get_next(fd)) {

      case CMD_WRITE:
        lseek(fd, last_position + 6, SEEK_SET);
        num_pairs = parse_write(fd, keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          fprintf(stderr, "Failed to write pair\n");
        }

        break;

      case CMD_READ:
        lseek(fd, last_position + 5, SEEK_SET);
        num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(fd2,num_pairs, keys)) {
          fprintf(stderr, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        lseek(fd, last_position + 7, SEEK_SET);
        num_pairs = parse_read_delete(fd, keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(fd2,num_pairs, keys)) {
          fprintf(stderr, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:
        kvs_show(fd2);
        break;

      case CMD_WAIT:
        lseek(fd, last_position + 5, SEEK_SET);
        if (parse_wait(fd, &delay, NULL) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          dprintf(fd2,"Waiting...\n");
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
            "  BACKUP\n" // Not implemented
            "  HELP\n"
        );

        break;
        
      case CMD_EMPTY:
        break;

      case EOC:
        close(fd);
        close(fd2);
        kvs_terminate();
        return 0;
      
    }
  }
}
