#include "netpool.h"
#include "tcp_socket.h"
#include <new>
#include <sys/socket.h>
#include  <netinet/tcp.h>
#include "connectpool.h"

#define TT_WARN_LOG printf
#define TT_DEBUG_LOG printf
#define TT_FATAL_LOG printf


SocketClient::SocketClient() {
	m_read_timeout = 0;
	m_write_timeout = 0;
	m_select_timeout = 0;
	m_connect_timeout = 0;
	_connection_num = 0;
}

SocketClient::~SocketClient() {
}


bool SocketClient::init(const char* client, const char *ip, short port, int connection_num, int write_timeout, int read_timeout, int connect_timeout, int select_timeout)
{
    m_write_timeout = write_timeout;
    m_read_timeout = read_timeout;
    m_connect_timeout = connect_timeout;
    m_select_timeout = select_timeout;
    _connection_num = connection_num;
    _client_name = client;
    ConnectPool::server_conf_t pool_server;
    pool_server.port = port;
    snprintf(pool_server.addr, sizeof(pool_server.addr), "%s", ip);
    pool_server.ctimeout_ms = m_connect_timeout;
    int ret = _pool.init(pool_server, _connection_num, 1, true);
    if(ret != 0) {
        return false;
    }
    return true;
}


int SocketClient::return_socket(SocketClient::socket_item_t& socket, bool status)
{
	if(socket.m_socket == -1) {
		return -1;
	}
	if(ConnectPool::ERR_SUCCESS == _pool.FreeSocket(socket.m_socket, !status)) {
		socket.m_socket = -1;
		return 1;
	} else {
		socket.m_socket = -1;
		return -2;
	}
    socket.m_socket = -1;
	return 1;
}

bool SocketClient::fetch_socket(SocketClient::socket_item_t &sock) {
    sock.m_socket = _pool.FetchSocket();
    if(sock.m_socket >= 0) {
        return true;
    }
    sock.m_socket = -1;
    return false;
}

