#ifndef _RPC_PROTOCOL_H_
#define _RPC_PROTOCOL_H_
#include <stdint.h>

enum {
    RPC_CMD_OK,
    RPC_CMD_FAIL,
    RPC_CMD_MOD,
    RPC_CMD_DEL,
    RPC_CMD_GET,
    RPC_CMD_RES,
    RPC_CMD_CLEAR,
    RPC_CMD_LOGIN,
    RPC_CMD_LOGOUT,
    RPC_CMD_DISABLE,
    RPC_CMD_MASTER,
    RPC_CMD_QUERY_SERVER,
    RPC_CMD_RES_SERVER,
    RPC_CMD_BUFFER,
    RPC_CMD_CLIENT,
    RPC_CMD_END,
};


#define MAGIC_NUM 0xabcd
#define MAX_INDEX_NUM 4
#define MAX_PACKET_SERVER_NUM 100

struct RpcPacketHead {
    unsigned short magic;
    unsigned char cmd;    //
    long long     server_id;
    unsigned short table_id;
    unsigned int msg_len;//
}__attribute__ ((packed));


static const int RPC_HEAD_SIZE = sizeof(RpcPacketHead);


#endif

