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
    } else {
        printf("Response pipe '%s' created successfully\n", req_pipe_path);
    }

    if ((fserv = open(server_pipe_path, O_RDWR)) < 0) {
        perror("Error opening server pipe");
        return -1;
    }
    
    char buffer[121];
    memset(buffer, 0, sizeof(buffer));  // Initialize the buffer

    // Copy OP_CODE to the buffer
    char op_code[20];
    sprintf(op_code, "%d", OP_CODE_CONNECT);

    strcat(buffer, op_code);
    strcat(buffer, "|");
    strcat(buffer, req_pipe_path);
    strcat(buffer, "|");
    strcat(buffer, resp_pipe_path);
    strcat(buffer, "|");
    strcat(buffer, notif_pipe_path);

    if (write(fserv, buffer, sizeof(buffer)) < 0) {
        perror("Error writing to server pipe");
        return -1;
    }

    // Open the request pipe (write-only)
    
    if ((req_pipe = open(req_pipe_path, O_WRONLY )) < 0) {
        perror("Error opening request pipe");
        return -1;
    } else {
        printf("Request pipe '%s' opened successfully\n", req_pipe_path);
    }

    // Open the response pipe (read-only)
    if ((resp_pipe = open(resp_pipe_path, O_RDONLY)) < 0) {
        perror("Error opening response pipe");
        close(req_pipe);
        return -1;
    } else{

        printf("Response pipe '%s' opened successfully\n", resp_pipe_path);
    }

    // Open the notification pipe (optional)
    if (notif_pipe_path != NULL && notif_pipe != NULL) {
        printf("NULL\n");
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
    printf("%s", key);
  // send subscribe message to request pipe and wait for response in response
  // pipe
  return 0;
}

int kvs_unsubscribe(const char *key) {
    printf("%s", key);
  // send unsubscribe message to request pipe and wait for response in response
  // pipe
  return 0;
}
