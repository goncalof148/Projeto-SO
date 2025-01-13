#include <fcntl.h>
#include <pthread.h>
#include <stdio.h>
#include <stdlib.h>
#include <string.h>
#include <unistd.h>
#include <sys/stat.h>
#include <errno.h>

#include "parser.h"
#include "api.h"
#include "../common/constants.h"
#include "../common/io.h"

typedef struct ThreadData {
    int notif_fd;
} ThreadData;

// Thread function to handle notifications
void *notification_handler(void *arg) {
    ThreadData *data = (ThreadData *)arg;
    int notif_fd = data->notif_fd;
    char notif_buf[256] = {0};

    while (1) {
        ssize_t bytes_read = read(notif_fd, notif_buf, sizeof(notif_buf) - 1);

        if (bytes_read > 0) {
            notif_buf[bytes_read] = '\0'; // Null-terminate the string
            printf("Notification received: %s\n", notif_buf);
        } else if (bytes_read == 0) {
            printf("SIGUSR1 close");
            _exit(0);
        } else if (bytes_read < 0 && errno != EAGAIN) {
            perror("Error reading notification pipe");
            printf("Error occurred on FD: %d\n", notif_fd);
            break;
        }

        memset(notif_buf, 0, sizeof(notif_buf)); // Clear buffer
    }

    return NULL;
}

// Thread function to handle commands
void *command_handler() {
    while (1) {
        char keys[MAX_NUMBER_SUB][MAX_STRING_SIZE] = {0};
        unsigned int delay_ms;
        size_t num;

        switch (get_next(STDIN_FILENO)) {
        case CMD_DISCONNECT:
            if (kvs_disconnect() != 0) {
                fprintf(stderr, "Failed to disconnect to the server\n");
                return NULL;
            }

            printf("Disconnected from server\n");
            exit(0); // Exit the program
            break;

        case CMD_SUBSCRIBE:
            num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
            if (num == 0) {
                fprintf(stderr, "Invalid command. See HELP for usage\n");
                continue;
            }

            if (kvs_subscribe(keys[0])) {
                fprintf(stderr, "Command subscribe failed\n");
            }

            break;

        case CMD_UNSUBSCRIBE:
            num = parse_list(STDIN_FILENO, keys, 1, MAX_STRING_SIZE);
            if (num == 0) {
                fprintf(stderr, "Invalid command. See HELP for usage\n");
                continue;
            }

            if (kvs_unsubscribe(keys[0])) {
                fprintf(stderr, "Command unsubscribe failed\n");
            }

            break;

        case CMD_DELAY:
            if (parse_delay(STDIN_FILENO, &delay_ms) == -1) {
                fprintf(stderr, "Invalid command. See HELP for usage\n");
                continue;
            }

            if (delay_ms > 0) {
                printf("Waiting...\n");
                delay(delay_ms);
            }
            break;

        case CMD_INVALID:
            fprintf(stderr, "Invalid command. See HELP for usage\n");
            break;

        case CMD_EMPTY:
            break;

        case EOC:
            // input should end in a disconnect, or it will loop here forever
            break;
        }
    }

    return NULL;
}

int main(int argc, char *argv[]) {
    if (argc < 3) {
        fprintf(stderr, "Usage: %s <client_unique_id> <register_pipe_path>\n", argv[0]);
        return 1;
    }

    char req_pipe_path[256] = "/tmp/req";
    char resp_pipe_path[256] = "/tmp/resp";
    char notif_pipe_path[256] = "/tmp/notif";
    int notif_fd;

    strncat(req_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
    strncat(resp_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));
    strncat(notif_pipe_path, argv[1], strlen(argv[1]) * sizeof(char));

    // Connect to the server
    if (kvs_connect(req_pipe_path, resp_pipe_path, argv[2], notif_pipe_path, &notif_fd) == 1) {
        fflush(stdout);
        exit(1);
    }

    printf("Connected to server.\n");

    // Create thread data
    ThreadData thread_data = {notif_fd};

    // Create threads
    pthread_t notif_thread, cmd_thread;
    if (pthread_create(&notif_thread, NULL, notification_handler, (void *)&thread_data) != 0) {
        perror("Failed to create notification thread");
        return 1;
    }

    if (pthread_create(&cmd_thread, NULL, command_handler, NULL) != 0) {
        perror("Failed to create command thread");
        return 1;
    }

    // Wait for threads to finish
    pthread_join(notif_thread, NULL);
    pthread_join(cmd_thread, NULL);

    return 0;
}
