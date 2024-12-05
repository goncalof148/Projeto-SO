#include <limits.h>
#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>

#include "constants.h"
#include "parser.h"
#include "operations.h"
#include <string.h>

int main() {
  char file_path[1024];
  char file_path_output[1024];
  FILE *file;
  FILE *file_output;
  if (kvs_init()) {
    fprintf(stderr, "Failed to initialize KVS\n");
    return 1;
  }
  printf("> ");
  fflush(stdout);
    
  if (fgets(file_path, sizeof(file_path), stdin) == NULL) {
    fprintf(stderr, "Error reading file path\n");
    return EXIT_FAILURE;
  }

  file_path[strcspn(file_path, "\n")] = '\0';
  strcpy(file_path_output,file_path);
  char *dot_position = strstr(file_path_output, ".job");
  if (dot_position != NULL) {
      *dot_position = '\0';
  }
  strcat(file_path_output, ".output");
  file = fopen(file_path, "r");
  file_output = fopen(file_path_output, "w");
  if (file == NULL) {
    perror("Error opening file");
    return EXIT_FAILURE;
  }

  if (file_output == NULL) {
    perror("Error opening file");
    return EXIT_FAILURE;
  }

  while (1) {
    char keys[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    char values[MAX_WRITE_SIZE][MAX_STRING_SIZE] = {0};
    unsigned int delay;
    size_t num_pairs;
    off_t last_position = lseek(fileno(file), 0, SEEK_CUR);
    switch (get_next(fileno(file))) {

      case CMD_WRITE:
        lseek(fileno(file), last_position + 6, SEEK_SET);
        num_pairs = parse_write(fileno(file), keys, values, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_write(num_pairs, keys, values)) {
          fprintf(stderr, "Failed to write pair\n");
        }

        break;

      case CMD_READ:
        lseek(fileno(file), last_position + 5, SEEK_SET);
        num_pairs = parse_read_delete(fileno(file), keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);
        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_read(file_output,num_pairs, keys)) {
          fprintf(stderr, "Failed to read pair\n");
        }
        break;

      case CMD_DELETE:
        lseek(fileno(file), last_position + 7, SEEK_SET);
        num_pairs = parse_read_delete(fileno(file), keys, MAX_WRITE_SIZE, MAX_STRING_SIZE);

        if (num_pairs == 0) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (kvs_delete(file_output,num_pairs, keys)) {
          fprintf(stderr, "Failed to delete pair\n");
        }
        break;

      case CMD_SHOW:
        kvs_show(file_output);
        break;

      case CMD_WAIT:
        lseek(fileno(file), last_position + 5, SEEK_SET);
        if (parse_wait(fileno(file), &delay, NULL) == -1) {
          fprintf(stderr, "Invalid command. See HELP for usage\n");
          continue;
        }

        if (delay > 0) {
          fprintf(file_output,"Waiting...\n");
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
        fclose(file);
        fclose(file_output);
        kvs_terminate();
        return 0;
      
    }
  }
}
