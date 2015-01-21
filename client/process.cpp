#include <stdio.h>
#include "process.h"
#include "sql_parser.h"
#include "tcp_socket.h"
#include "rpc_protocol.h"

void sendAck(int socket, int write_timeout, bool success) {
    char buffer[128];
    RpcPacketHead* head = (RpcPacketHead*)buffer;
    head->magic = MAGIC_NUM;
    if (success) {
        head->cmd = RPC_CMD_OK;
    } else {
        head->cmd = RPC_CMD_FAIL;
    }
    head->table_id = 0;
    head->msg_len = 0;
    tcp_write(socket, buffer, RPC_HEAD_SIZE, write_timeout);
}

bool process(SocketClient* client, char* res, std::string& ip, int port, std::string command) {
    SocketClient::socket_item_t socket;
    int ret = 0;
    bool flag = false;
    char buffer[100];
    RpcPacketHead* head = (RpcPacketHead*)buffer;
    head->magic = MAGIC_NUM;
    head->cmd = RPC_CMD_CLIENT;
    head->server_id = 0;
    head->table_id = 0;
    head->msg_len = 0;
    if (command == "") {
        return true;
    }
    // parse command
    tinytable::sql_parser parser;
    parser.SetSQLString(command);
    if (!parser.ParseSQLString()) {
        printf("\ninput is invalid sql!\n");
        return false;
    } else {
        // send command to server
        if (!client->fetch_socket(socket)) {
            printf("connect %s:%d error\n", ip.c_str(), port);
            return false;
        } else {
            head->msg_len = command.length();
            ret = tcp_write(socket.m_socket, buffer, RPC_HEAD_SIZE, client->m_write_timeout);
            if (ret != RPC_HEAD_SIZE) {
                printf("connect %s:%d error\n", ip.c_str(), port);
                goto END;
            }
            ret = tcp_write(socket.m_socket, command.c_str(), head->msg_len, client->m_write_timeout);
            if (ret != head->msg_len) {
                printf("connect %s:%d error\n", ip.c_str(), port);
                goto END;
            }
RETRY:
            // wait for res
            ret = tcp_read(socket.m_socket, res, RPC_HEAD_SIZE, client->m_read_timeout);
            if (ret != RPC_HEAD_SIZE) {
                printf("connect %s:%d error\n", ip.c_str(), port);
                goto END;
            }
            RpcPacketHead* res_head = (RpcPacketHead*)res;
            int msg_len = res_head->msg_len;
            if (res_head->cmd == RPC_CMD_OK && msg_len == 0) {
                flag = true;
                goto END;
            }
            ret = tcp_read(socket.m_socket, res, msg_len, client->m_read_timeout);
            if (ret == msg_len) {
                *(res + msg_len) = 0;
                // send ack
                sendAck(socket.m_socket, client->m_write_timeout, true);
                printf("%s", res);
                goto RETRY;
            }
        }
    }
END:
    client->return_socket(socket, false);
    return flag;
}

