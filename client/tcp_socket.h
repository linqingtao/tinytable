#ifndef _TCP_SOCKET_H_
#define _TCP_SOCKET_H_
#include <unistd.h>
#include <stdio.h>
#include <string.h>
#include <stdlib.h>
#include <netinet/tcp.h>

extern int tcp_connect(const char *host, int port, int milisecs);
extern ssize_t tcp_write(int sock, const void *buffer, size_t len, int milisecs);
extern ssize_t tcp_read(int sock, void *str, size_t len, int milisecs);


#endif

