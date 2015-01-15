#ifndef _NET_POOL_H__
#define _NET_POOL_H__
#include <stdlib.h>
#include <string>
#include "connectpool.h"


class SocketClient
{

public:
	class socket_item_t
	{
	public:
		socket_item_t()
		: m_socket(-1) {}
		~socket_item_t() {}
		int m_socket;
	};
	SocketClient();
	~SocketClient();
	bool fetch_socket(socket_item_t &sock);
	int return_socket(socket_item_t& socket, bool status);
    bool init(const char* client, const char* ip, short port, int connection_num, int write_timeout = 50, int read_timeout = 200, int connect_timeout = 30, int select_timeout = 40);
    std::string getClientName() {return _client_name;}
	int m_read_timeout;
	int m_write_timeout;
	int m_select_timeout;
	int m_connect_timeout;
protected:
    ConnectPool _pool;
	int _connect_timeout;
	int _connection_num;
    std::string  _client_name;
};

#endif
