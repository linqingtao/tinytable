#include "tcp_socket.h"
#include <netdb.h>
#include <errno.h>
#include <fcntl.h>
#include <limits.h>
#include <sys/time.h>
#include <sys/types.h>
#include <sys/poll.h>
#include <sys/socket.h>
#include <arpa/inet.h>
#include <netinet/in.h>



int tcp_poll(struct pollfd *fd, unsigned int nfds, int timeout)
{

	int sock;
    do {
		sock = poll(fd, nfds, timeout);
		if (sock < 0) {
			if (errno == EINTR) {
				continue;
			}
		}
		return sock;
	} while (true);
	return sock;
}


int tcp_connect(const char *host, int port, int milisecs)
{
	int fd;
	struct sockaddr_in sin;
	char buf[8193];
	struct hostent he, *p = NULL;
	int err = 0, ret = 0;
	if ((fd = socket(AF_INET, SOCK_STREAM, 0)) < 0) {
		return -1;
	}
	bzero(&sin, sizeof(sin));

	if ((sin.sin_addr.s_addr = inet_addr(host)) == INADDR_NONE) {
		if ((ret = gethostbyname_r(host, &he, buf, sizeof(buf), &p, &err)) < 0) {
			close(fd);
			return -1;
		}
        if (err != 0 || p == NULL) {
            close(fd);
            return -1;
        }
        memcpy(&sin.sin_addr.s_addr, he.h_addr, sizeof(sin.sin_addr.s_addr));
    }
    sin.sin_family = AF_INET;
    sin.sin_port = htons((uint16_t)port);
    struct sockaddr * addr = (struct sockaddr*)&sin;
    int flags;
    socklen_t len = sizeof(sin);
    err = 0;
    flags = fcntl(fd, F_GETFL, 0);
    fcntl(fd, F_SETFL, flags | O_NONBLOCK);
    ret = connect(fd, addr, len);
    if (ret < 0) {
        if (errno != EINPROGRESS) {
            close(fd);
            return -1;
        }
    }
    if (ret == 0) {
        fcntl(fd, F_SETFL, flags);
    } else {
        struct pollfd poll_fd;
        poll_fd.fd = fd;
        poll_fd.events = POLLIN | POLLOUT;
        ret = tcp_poll(&poll_fd, 1, milisecs);
        if (ret == 0) {
            close(fd);
            errno = ETIMEDOUT;
            return -1;
        }

        if ( (poll_fd.revents & POLLIN) || (poll_fd.revents & POLLOUT) ) {
            len = sizeof(err);
            if (getsockopt(fd, SOL_SOCKET, SO_ERROR, &err, &len) < 0) {
                close(fd);
                return -1;
            }
        } else {
            close(fd);
            return -1;
        }
        if (err) {
            close(fd);
            return -1;
        }
    }
	return fd;
}


ssize_t tcp_read(int sock, void *str, size_t len, int milisecs) {
    struct timeval tv;
    struct timeval ori_tv;
    ssize_t read_len;
    int sockflag;
    int ret;
    socklen_t tv_len = (socklen_t)sizeof(tv);
    
    read_len = recv(sock, str, len, MSG_DONTWAIT);
    if (read_len == (ssize_t)len) {
        return read_len;
    }
    if ((read_len < 0) && (errno != EAGAIN) && (errno != EINTR)) {
        return -1;
    }
    if (read_len < 0) {
        read_len = 0;
    }
    if (0 == milisecs) {
        errno = ETIMEDOUT;
        return -1;
    }
    if (milisecs < 0) {
       milisecs = INT_MAX; 
    }
    if (getsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &ori_tv, &tv_len)<0) {
        return -1;
    }
    if ((sockflag = fcntl(sock, F_GETFL, 0))<0) {
        return -1;
    }
    if (sockflag&O_NONBLOCK) {
        if (fcntl(sock, F_SETFL, (sockflag)&(~O_NONBLOCK))<0) {
            return -1;
        }
    }
    do {
      tv.tv_sec = milisecs/1000;
      tv.tv_usec = (milisecs%1000)*1000;
      if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &tv, tv_len)<0)
          return -1;
      do {
        ret = recv(sock, (char*)str+read_len, len - (size_t)read_len, MSG_WAITALL); 
      } while (ret < 0 && errno == EINTR);
      if (ret<0 && EAGAIN==errno) {
        errno = ETIMEDOUT;
      }
      if (ret < 0) {
        read_len = -1;
      } else {
        read_len += ret;
      }
    } while (ret > 0 && len != (size_t)read_len);
    if (sockflag&O_NONBLOCK) {
        if (fcntl(sock, F_SETFL, sockflag)<0) {
            return -1;
        }
    }
    if (setsockopt(sock, SOL_SOCKET, SO_RCVTIMEO, &ori_tv, tv_len)<0) {
        return -1;
    }
    return read_len;
}


ssize_t tcp_write(int sock, const void *str, size_t len, int milisecs)
{
    struct timeval tv;
    struct timeval ori_tv;
    ssize_t write_len = 0;
    int ret = 0;
    int sockflag = 0;
    socklen_t tv_len = sizeof(tv);

    write_len = send(sock, str, len, MSG_DONTWAIT);
    if ((write_len == (ssize_t)len)) {
        return write_len;
    }
    if ((write_len < 0) && (errno != EAGAIN) && (errno != EWOULDBLOCK) && (errno != EINTR)) {
        return -1;
    }
    if (write_len < 0) {
        write_len = 0;
    }

    if (0 == milisecs) {
        errno = ETIMEDOUT;
        return -1;
    } else if (milisecs < 0) {
       milisecs = INT_MAX; 
    }

    if (getsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &ori_tv, &tv_len)<0) {
        return -1;
    }
    if ((sockflag = fcntl(sock, F_GETFL, 0))<0) {
        return -1;
    }
    if (sockflag&O_NONBLOCK) {
        if (fcntl(sock, F_SETFL, (sockflag)&(~O_NONBLOCK))<0) {
            return -1;
        }
    }
    tv_len = sizeof(tv);
    do {
        // send sendtimeout
        tv.tv_sec = milisecs/1000;
        tv.tv_usec = (milisecs%1000)*1000;
        if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &tv, tv_len)<0) {
            return -1;
        }
        do { 
            // send from the offset
            ret = send(sock, (char*)str + write_len, len - (size_t)write_len, MSG_WAITALL); 
        } while (ret < 0 && EINTR == errno);
        if (ret < 0) {
            write_len = -1;
        } else {
            write_len += ret;
        }
    } while (ret > 0 && len != (size_t)write_len);
    // recover the socket
    if (sockflag & O_NONBLOCK) {
        if (fcntl(sock, F_SETFL, sockflag)<0) {
            return -1;
        }
    }
    if (setsockopt(sock, SOL_SOCKET, SO_SNDTIMEO, &ori_tv, tv_len)<0) {
        return -1;
    }
    return write_len;
}

