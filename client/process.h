#ifndef _CLIENT_PROCESS_H_
#define _CLIENT_PROCESS_H_
#include <string>
#include "netpool.h"

extern bool process(SocketClient* client, char* res, std::string& ip, int port, std::string command);


#endif

