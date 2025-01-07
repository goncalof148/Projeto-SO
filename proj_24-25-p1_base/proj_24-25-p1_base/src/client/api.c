#include "api.h"
#include "../common/constants.h"
#include "../common/protocol.h"
#include <fcntl.h>
#include <stdlib.h>   
#include <unistd.h>  
#include <sys/stat.h>
#include <string.h>  
#include <stdio.h>
#include <errno.h>

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path,
                int *notif_pipe) {


    int req_pipe, resp_pipe, fserv;
    char buf[40];
    
    if ((fserv = open(server_pipe_path, O_RDWR)) < 0) {
        perror("Error opening server pipe");
        return -1;
    }
    
    strcpy(buf, "OP_CODE=1");

    if (write(fserv, buf, strlen(buf) + 1) < 0) {
        perror("Error writing to server pipe");
        return -1;
    }

    unlink(req_pipe_path);
    unlink(resp_pipe_path);

    if (mkfifo(req_pipe_path, 0666) < 0) {
        perror("Error creating request pipe");
        exit(1);
    } else {
        printf("Request pipe '%s' created successfully\n", req_pipe_path);
    }


    if (mkfifo(resp_pipe_path, 0666) < 0) {
      perror("Error creating response pipe");
      exit(1);
    }

    printf("aquii\n");

    // Open the request pipe (write-only)
    if ((req_pipe = open(req_pipe_path, O_WRONLY | O_NONBLOCK)) < 0) {
        printf("Failed to open request pipe '%s', errno: %d\n", req_pipe_path, errno);
        perror("Error opening request pipe");
        return -1;
    }

    printf("aquii2\n");
    // Open the response pipe (read-only)
    if ((resp_pipe = open(resp_pipe_path, O_RDONLY)) < 0) {
        perror("Error opening response pipe");
        close(req_pipe);
        return -1;
    }

    // Open the server pipe (write-only)
    if ((fserv = open(server_pipe_path, O_WRONLY)) < 0) {
        perror("Error opening server pipe");
        close(req_pipe);
        close(resp_pipe);
        return -1;
    }

    // Open the notification pipe (optional)
    if (notif_pipe_path != NULL && notif_pipe != NULL) {
        if ((*notif_pipe = open(notif_pipe_path, O_RDONLY | O_NONBLOCK)) < 0) {
            perror("Error opening notification pipe");
            close(req_pipe);
            close(resp_pipe);
            close(fserv);
            return -1;
        }
    }

    printf("Client connected to server: %s\n", server_pipe_path);

    // Example communication: Send a message to the server
    if (write(fserv, "aaa", strlen("aaa") + 1) < 0) {
        perror("Error writing to server pipe");
        close(req_pipe);
        close(resp_pipe);
        close(fserv);
        if (notif_pipe) close(*notif_pipe);
        return -1;
    }

    // Close server pipe after writing the message
    close(fserv);
    close(req_pipe);
    close(resp_pipe);

    return 0;
}

int kvs_disconnect(void) {
  // close pipes and unlink pipe files
  return 0;
}

int kvs_subscribe(const char *key) {
  // send subscribe message to request pipe and wait for response in response
  // pipe
  return 0;
}

int kvs_unsubscribe(const char *key) {
  // send unsubscribe message to request pipe and wait for response in response
  // pipe
  return 0;
}
