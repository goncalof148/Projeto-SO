#include "api.h"
#include "src/common/constants.h"
#include "src/common/protocol.h"

int kvs_connect(char const *req_pipe_path, char const *resp_pipe_path,
                char const *server_pipe_path, char const *notif_pipe_path,
                int *notif_pipe) {

  // create pipes and connect
  if (mkfifo(req_pipe_path, O_WRONLY) < 0)
    exit (1);
  if (mkfifo(resp_pipe_path,  O_RDONLY) < 0)
    exit (1);
  if (mkfifo(notif_pipe_path, O_RDONLY) < 0)
    exit (1);
  int req_pipe = open(req_pipe_path, O_WRONLY);
  int resp_pipe = open(resp_pipe_path, O_RDONLY);
  int notif_pipe_fd = open(notif_pipe_path, O_RDONLY);
  if(req_pipe < 0 || resp_pipe == 0 || notif_pipe_fd == 0){
    exit(1);
  }

  int fserv = open(server_pipe_path, O_WRONLY);
  if(fserv < 0)
    exit(1);
    
  write(server_fd, "aaa", strlen("aaa") + 1);
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
