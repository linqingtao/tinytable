#ifndef _PROTO_CONFIG_H_
#define _PROTO_CONFIG_H_
#include "config.h"


class ProtoToConfig {
private:
    ProtoToConfig(){}
    ~ProtoToConfig(){}
public: 
    static bool loadTableIDConfig(const char* path, const char* file, TableIDConfig* config);
    static bool loadTableXmlConfig(const char* path, const char* file, TableConfig* config, TableIDConfig* idconfig);
};
#endif

