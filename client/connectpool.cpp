#include "connectpool.h"
#include <unistd.h>
#include <stdlib.h>
#include <stdio.h>
#include <string.h>
#include <errno.h>
#include "tcp_socket.h"
#include <assert.h>
#include <sys/socket.h>
#include <netinet/tcp.h>
#include <netinet/in.h>
#define SafeCloseSock(s) if (s>=0) { close(s); }


ConnectPool::ConnectPool()
: m_sockPerServer(1)
, m_freeSockCount(0)
, m_sockArr(NULL)
, m_longConnect(true)
, m_retryCount(1) {
    pthread_mutex_init(&m_mutex, NULL);
    pthread_cond_init(&m_condition, NULL);
}

ConnectPool::~ConnectPool() {
    if (m_sockArr) {
        for (int i = 0; i < m_sockPerServer; i++) {
            SafeCloseSock(m_sockArr[i].sock);
        }
        delete m_sockArr;
        m_sockArr = NULL;
    }
    pthread_cond_destroy(&m_condition);
    pthread_mutex_destroy(&m_mutex);
}

int ConnectPool::init(const server_conf_t& serverConf, int sockPerServer, int connectRetry, bool longConnect) {
    if (sockPerServer <= 0) {
        return ERR_PARAM;
    }
    if (connectRetry < 0) {
        return ERR_PARAM;
    }
    m_sockPerServer = sockPerServer;

    m_sockArr = new sock_info_t[m_sockPerServer];
    if (!m_sockArr) {
        return ERR_MEM;
    }
    for (int i = 0; i < m_sockPerServer; ++i) {
        m_sockArr[i].sock = -1;
        m_sockArr[i].status = STATUS_INVALID;
    }
    m_freeSockCount = m_sockPerServer;
    const server_conf_t& conf = serverConf;
    server_info_t& server = m_serverArr;
    if (conf.addr[0] == '\0' || conf.port <= 0) {
        return ERR_PARAM;
    }
    memcpy(server.addr, conf.addr, sizeof(server.addr));
    server.addr[sizeof(server.addr) - 1] = 0;
    server.port = conf.port;
    server.ctimeout_ms = conf.ctimeout_ms;

    m_longConnect = longConnect;
    m_retryCount = connectRetry;
    return 0;
}

int ConnectPool::Connect(int idx) {
    if (idx < 0 || idx >= m_sockPerServer) {
        return ERR_ASSERT;
    }
    if (m_sockArr[idx].status != STATUS_BUSY) {
        return ERR_ASSERT;
    }
    int ret = ERR_FAILED;
    server_info_t& server = m_serverArr;
    int sock = tcp_connect(server.addr, server.port, server.ctimeout_ms);
    if (sock < 0) {
        ret = ERR_CONN;
    } else {
        pthread_mutex_lock(&m_mutex);
        m_sockArr[idx].sock = sock;
        pthread_mutex_unlock(&m_mutex);
        int on = 1;
        setsockopt(m_sockArr[idx].sock, IPPROTO_TCP, TCP_NODELAY, &on, sizeof (on));
        ret = 0;
    }
    return ret;
}


int ConnectPool::check(int sock) {
    char buf[1];
    ssize_t ret = recv(sock, buf, sizeof(buf), MSG_DONTWAIT);
    if (ret > 0) {
        return -1;
    } else if (ret == 0) {
        return -1;
    } else {
        if (errno == EWOULDBLOCK) {
            return 0;
        } else {
            return -1;
        }
    }
}

int ConnectPool::FetchSocket() {
    int retry = m_retryCount;
    while (retry >= 0) {
        int old_status;
        int sockIdx = FetchSocket(old_status);
        if (sockIdx < 0) {
            return ERR_ASSERT;
        }
        if (m_longConnect) {
            if (old_status == STATUS_READY) {
                return m_sockArr[sockIdx].sock;
            } else {
                if (Connect(sockIdx) >= 0) {
                    return m_sockArr[sockIdx].sock;
                } else {
                    int ret = FreeIdxSocket(sockIdx, true);
                    if (ret < 0) {
                        return ERR_ASSERT;
                    }
                }
            }
        } else {
            if (Connect(sockIdx) >= 0) {
                return m_sockArr[sockIdx].sock;
            } else {
                int ret = FreeIdxSocket(sockIdx, true);
                if (ret < 0) {
                    return ret;
                }
            }
        }
        retry--;
    }
    return ERR_CONN;
}

int ConnectPool::FetchSocket(int& old_status) {
    pthread_mutex_lock(&m_mutex);
    while (m_freeSockCount <= 0) {
        pthread_cond_wait(&m_condition, &m_mutex);
    }

    int sock_idx = -1;
    for (int i = 0; i < m_sockPerServer; ++i) {
        if (m_sockArr[i].status != STATUS_BUSY) {
            sock_idx = i;
            break;
        }
    }
    assert(sock_idx >= 0);
    old_status = m_sockArr[sock_idx].status;
    m_sockArr[sock_idx].status = STATUS_BUSY;
    m_freeSockCount--;

    if (old_status == STATUS_READY) {
        if (check(m_sockArr[sock_idx].sock)) {
            SafeCloseSock(m_sockArr[sock_idx].sock);
            old_status = STATUS_INVALID;
        }
    }
    pthread_mutex_unlock(&m_mutex);
    return sock_idx;
}

int ConnectPool::FreeSocket(int sock, bool errclose) {
    int idx = m_sockPerServer - 1;

    if (sock < 0) {
        return ERR_ASSERT;
    }
    while (idx >= 0 && m_sockArr[idx].sock != sock) {
        idx--;
    }

    if (idx < 0) {
        return ERR_SOCK;
    }
    return FreeIdxSocket(idx, errclose);
}

int ConnectPool::FreeIdxSocket(int idx, bool errclose) {
    assert(idx >= 0 && idx < m_sockPerServer);
    int ret = ERR_FAILED;
    pthread_mutex_lock(&m_mutex);
    if (m_sockArr[idx].status == STATUS_BUSY) {
        if (m_longConnect && !errclose) {
            m_sockArr[idx].status = STATUS_READY;
        } else {
            SafeCloseSock(m_sockArr[idx].sock);
            m_sockArr[idx].status = STATUS_INVALID;
        }
        m_freeSockCount++;
        ret = 0;
    } else {
        ret = ERR_SOCK;
    }
    pthread_cond_signal(&m_condition);
    pthread_mutex_unlock(&m_mutex);
    return ret;
}


