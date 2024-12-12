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
#include <semaphore.h>

pthread_rwlock_t rwlock;

// static unsigned int concurrent_threads = 0;
static sem_t threads_mutex;

typedef struct {
    char file_path_base[PATH_MAX];
    int result;  
} job_thread_args_t;

int has_job_extension(const char *filename) {
    const char *dot = strrchr(filename, '.');
    return (dot && strcmp(dot, ".job") == 0);
}

int process_job(const char *base_file_path) {
    char input_file_path[PATH_MAX];
    strncpy(input_file_path, base_file_path, sizeof(input_file_path) - 1);
    strncat(input_file_path, ".job", sizeof(input_file_path) - strlen(input_file_path) - 1);

    char output_file_path[PATH_MAX];
    strncpy(output_file_path, base_file_path, sizeof(output_file_path) - 1);
    strncat(output_file_path, ".out", sizeof(output_file_path) - strlen(output_file_path) - 1);

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

                if (pthread_rwlock_wrlock(&rwlock) != 0) {
                    perror("Failed to acquire write lock");
                    close(fd_input);
                    close(fd_output);
                    return EXIT_FAILURE;
                }

                if (kvs_write(num_pairs, keys, values)) {
                    fprintf(stderr, "Failed to write pair\n");
                }

                pthread_rwlock_unlock(&rwlock);

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

                if (pthread_rwlock_rdlock(&rwlock) != 0) {
                    perror("Failed to acquire read lock");
                    close(fd_input);
                    close(fd_output);
                    return EXIT_FAILURE;
                }

                if (kvs_read(fd_output, num_pairs, keys)) { 
                    fprintf(stderr, "Failed to read pair\n");
                }

                pthread_rwlock_unlock(&rwlock);

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

                if (pthread_rwlock_wrlock(&rwlock) != 0) {
                    perror("Failed to acquire write lock");
                    close(fd_input);
                    close(fd_output);
                    return EXIT_FAILURE;
                }

                if (kvs_delete(fd_output, num_pairs, keys)) { 
                    fprintf(stderr, "Failed to delete pair\n");
                }

                pthread_rwlock_unlock(&rwlock);

                break;

            case CMD_SHOW:
                if (pthread_rwlock_rdlock(&rwlock) != 0) {
                    perror("Failed to acquire read lock");
                    close(fd_input);
                    close(fd_output);
                    return EXIT_FAILURE;
                }

                kvs_show(fd_output); 

                pthread_rwlock_unlock(&rwlock);

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
                if (pthread_rwlock_wrlock(&rwlock) != 0) {
                    perror("Failed to acquire write lock");
                    close(fd_input);
                    close(fd_output);
                    return EXIT_FAILURE;
                }

                if (kvs_backup(base_file_path)) {
                    fprintf(stderr, "Failed to perform backup.\n");
                }

                pthread_rwlock_unlock(&rwlock);

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
    close(fd_input);
    close(fd_output);
    return EXIT_FAILURE;
}

void *process_job_thread(void *arg) {
    job_thread_args_t *targ = (job_thread_args_t *) arg;
    
    targ->result = process_job(targ->file_path_base);
    sem_post(&threads_mutex);
    return &targ->result;
}

int main(int argc, char *argv[]) {
    int ret = EXIT_FAILURE;
    char *dir_path = NULL;
    pthread_t *thread_ids = NULL;
    job_thread_args_t *args = NULL;
    unsigned int max_threads = 0;
    unsigned int job_count = 0;
    DIR *dir = NULL;
    struct dirent *entry = NULL;

    if (argc < 3) {
        fprintf(stderr, "Usage: %s <directory_path> <backups_number>\n", argv[0]);
        return EXIT_FAILURE;
    }

    if (kvs_init()) {
        fprintf(stderr, "Failed to initialize KVS\n");
        return EXIT_FAILURE;
    }

    dir_path = malloc(PATH_MAX);
    if (dir_path == NULL) {
        fprintf(stderr, "Failed to allocate memory for directory path.\n");
        goto cleanup;
    }

    strncpy(dir_path, argv[1], PATH_MAX - 1);
    dir_path[PATH_MAX - 1] = '\0';

    int backups_number = atoi(argv[2]);
    unsigned long max_threads_l = strtoul(argv[2], NULL, 10);

    if (max_threads_l > UINT_MAX) {
        fprintf(stderr, "Error: Max threads too big");
        return EXIT_FAILURE;
    }

    max_threads = (unsigned int) max_threads_l;

    printf("Number of backups: %d\n", backups_number);
    fflush(stdout);

    if (pthread_rwlock_init(&rwlock, NULL) != 0) {
        perror("Failed to initialize read-write lock");
        goto cleanup;
    }

    dir = opendir(dir_path);
    if (dir == NULL) {
        perror("Error opening directory");
        goto cleanup;
    }

    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type != DT_REG && entry->d_type != DT_UNKNOWN) {
            continue;
        }

        if (!has_job_extension(entry->d_name)) {
            continue; 
        }

        job_count++;
    }

    if (job_count == 0) {
        printf("No job files found in the directory.\n");
        ret = EXIT_SUCCESS;
        goto cleanup;
    }

    thread_ids = malloc(sizeof(pthread_t) * job_count);
    if (thread_ids == NULL) {
        fprintf(stderr, "Failed to allocate memory for thread IDs.\n");
        goto cleanup;
    }

    args = malloc(sizeof(job_thread_args_t) * job_count);
    if (args == NULL) {
        fprintf(stderr, "Failed to allocate memory for thread arguments.\n");
        goto cleanup;
    }
    
    rewinddir(dir);
    int job_id = 0;

    while ((entry = readdir(dir)) != NULL) {
        if (entry->d_type != DT_REG && entry->d_type != DT_UNKNOWN) {
            continue; 
        }

        if (!has_job_extension(entry->d_name)) {
            continue; 
        }

        char input_file_path[PATH_MAX];
        int ret_snprintf;

        if (dir_path[strlen(dir_path) - 1] == '/') {
            ret_snprintf = snprintf(input_file_path, sizeof(input_file_path), "%s%s", dir_path, entry->d_name);
        } else {
            ret_snprintf = snprintf(input_file_path, sizeof(input_file_path), "%s/%s", dir_path, entry->d_name);
        }

        if (ret_snprintf < 0 || ret_snprintf >= (int)sizeof(input_file_path)) {
            fprintf(stderr, "Path too long: %s%s\n", dir_path, entry->d_name);
            continue; 
        }

        char base_file_path[PATH_MAX];
        strncpy(base_file_path, input_file_path, sizeof(base_file_path) - 1);
        base_file_path[sizeof(base_file_path) - 1] = '\0'; 

        char *dot_position = strstr(base_file_path, ".job");
        if (dot_position != NULL) {
            *dot_position = '\0';
        }

        job_thread_args_t *arg = &args[job_id];
        strncpy(arg->file_path_base, base_file_path, sizeof(arg->file_path_base) - 1);
        arg->file_path_base[sizeof(arg->file_path_base) - 1] = '\0';
        arg->result = 0;

        job_id++;
    }

    if (sem_init(&threads_mutex, 0, max_threads) != 0) {
        goto cleanup;
    }

    for (size_t i = 0; i < job_count; i++) {
        job_thread_args_t *arg = &args[i];
        
        // TODO: add limit to concurrent threads
        sem_wait(&threads_mutex);

        if (pthread_create(&thread_ids[i], NULL, process_job_thread, arg) != 0) {
            fprintf(stderr, "Failed to create thread for job %s\n", arg->file_path_base);
        }
    }

    for (size_t i = 0; i < job_count; i++) {
        pthread_join(thread_ids[i], NULL);
    }

    ret = EXIT_SUCCESS;

cleanup:
    if (thread_ids) free(thread_ids);
    if (args) free(args);
    if (dir) closedir(dir);
    if (dir_path) free(dir_path);
    pthread_rwlock_destroy(&rwlock);
    kvs_terminate();

    return ret;
}
