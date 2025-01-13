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

#define FIELD_SIZE 40
#define REQUEST_BUFFER_SIZE (1 + FIELD_SIZE * 3)

struct Connection {
  int req_pipe;
  int resp_pipe;
  int notif_pipe;

  const char *req_pipe_path;
  const char *resp_pipe_path;
  const char *notif_pipe_path;
};

struct Connection connection;

int make_pipe(char const *pipe_path) { 
  unlink(pipe_path);
  if (mkfifo(pipe_path, 0666) < 0) {
    perror("Error creating pipe");
    return 1;
  }

  // printf("Pipe '%s' created successfully\n", pipe_path);
  return 0;
}

int open_pipe(char const *pipe_path, int pipe_flags, int *fd_out) { 
  if ((*fd_out = open(pipe_path, pipe_flags)) < 0) {
    perror("Error opening pipe");
    return 1;
  }

  // printf("Pipe '%s' opened successfully\n", pipe_path);
  return 0;
}

int read_response(int opcode) {
  char buf[2];
  char buf2[11];
  ssize_t n = read(connection.resp_pipe, buf, 2);
  
  if (n < 2) {
    return -1;
  }
  
if (buf[0] == '1') {
    strcat(buf2, "CONNECT");
} else if (buf[0] == '2') {
    strcat(buf2, "DISCONNECT");
} else if (buf[0] == '3') {
    strcat(buf2, "SUBSCRIBE");
} else if (buf[0] == '4') {
    strcat(buf2, "UNSUBSCRIBE");
} 

printf("Server returned %c for operation: %s\n", buf[1], buf2);


  if (buf[0] != '0' + opcode) {
    return -1;
  }

  return buf[1];
}

int send_message(int opcode, char *message_buf) {
  message_buf[0] = '0' + (char)opcode;

  if (write(connection.req_pipe, message_buf, sizeof(message_buf)) < 0) {
    perror("Error sending request to server");
    return 1;
  }

  int response = read_response(opcode);
  printf("%d", response);
  return 0;
}

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path) {
  int fserv;

  connection.req_pipe_path = req_pipe_path;
  connection.resp_pipe_path = resp_pipe_path;
  connection.notif_pipe_path = notif_pipe_path;

  if (make_pipe(req_pipe_path) != 0
    || make_pipe(resp_pipe_path) != 0
    || make_pipe(notif_pipe_path) != 0) {
    return 1;
  }

  if (open_pipe(server_pipe_path, O_RDWR, &fserv) != 0) {
    fprintf(stderr, "Could not open server pipe\n");
    return 1;
  }
  
  char buffer[121] = {0};

  // Copy OP_CODE to the buffer
  buffer[0] = '0' + OP_CODE_CONNECT;
  strcpy(buffer + 1, req_pipe_path);
  strcpy(buffer + 41, resp_pipe_path);
  strcpy(buffer + 81, notif_pipe_path);
  
  if (write(fserv, buffer, sizeof(buffer)) < 0) {
    perror("Error writing to server pipe");
    return 1;
  }

  if (open_pipe(req_pipe_path, O_WRONLY, &connection.req_pipe) != 0
    || open_pipe(resp_pipe_path, O_RDONLY, &connection.resp_pipe) != 0
    || open_pipe(notif_pipe_path, O_RDONLY | O_NONBLOCK, &connection.notif_pipe) != 0) {
    return 1;
  }

  printf("Waiting for server response...\n");

  fflush(stdout);

  if (read_response(OP_CODE_CONNECT) != '0') {
    return 1;
  }

  printf("Connection ready\n");
  fflush(stdout);

  // Close server pipe after writing the message
  close(fserv);
  return 0;
}

int kvs_disconnect(void) {
  char buffer[1] = {0};
  send_message(OP_CODE_DISCONNECT, buffer);

  close(connection.req_pipe);
  close(connection.resp_pipe);
  close(connection.notif_pipe);

  unlink(connection.req_pipe_path);
  unlink(connection.resp_pipe_path);
  unlink(connection.notif_pipe_path);

  // close pipes and unlink pipe files
  return 0;
}


int kvs_subscribe(const char *key) {
  char buffer[42] = {0};
  strcpy(buffer + 1, key);

  return send_message(OP_CODE_SUBSCRIBE, buffer);
}

int kvs_unsubscribe(const char *key) {
  char buffer[42] = {0};
  strcpy(buffer + 1, key);

  return send_message(OP_CODE_UNSUBSCRIBE, buffer);
}
