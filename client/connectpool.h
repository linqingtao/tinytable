#ifndef _CONNECTPOOL_H_
#define _CONNECTPOOL_H_

#define MAX_NETPOOL_ADDR_LEN  128

#include <pthread.h>

class ConnectPool
{
    public:
        enum {
            ERR_SUCCESS = 0,
            ERR_FAILED = -1,
            ERR_CONN = -2,
            ERR_MEM = -3,
            ERR_RETRY = -4,
            ERR_PARAM = -5,
            ERR_SOCK = -6,
            ERR_ASSERT = -7,
        };

        struct server_conf_t {
            char addr[MAX_NETPOOL_ADDR_LEN];
            int port;
            int ctimeout_ms;//ms
        };

        int  init(const server_conf_t& serverConf, int sockPerServer, int connectRetry=1, bool longConnect=true);
        int  FetchSocket();
        int  FreeSocket(int sock, bool errclose);
        ConnectPool();
        ~ConnectPool();	
    protected:
        enum {
            STATUS_BUSY=0,
            STATUS_READY=1,
            STATUS_INVALID=2		
        };

        struct sock_info_t {
            int sock;
            int status;
        };

        struct server_info_t {
            char addr[MAX_NETPOOL_ADDR_LEN];
            int port;
            int ctimeout_ms;
        };
        int Connect(int idx);
        int check(int sock);
        int FetchSocket(int& oldStatus);
        int FreeIdxSocket(int sockIdx, bool errclose);
    protected:
        int  m_sockPerServer;
        int  m_freeSockCount;
        server_info_t m_serverArr;
        sock_info_t* m_sockArr;
        pthread_mutex_t m_mutex;
        pthread_cond_t m_condition;	
        bool m_longConnect;
        int  m_retryCount;
};

#endif


