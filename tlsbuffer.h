#ifndef _TLS_BUFFER_H_
#define _TLS_BUFFER_H_
#include <stdint.h>
#include <string>
#include <pthread.h>
#include "tinytable_log_utils.h"


class TlsBuffer {
private:
    TlsBuffer(){}
    ~TlsBuffer(){}
public:
    static bool tls_init(int len);
    static bool tls_destroy();
    static char* get();
private:
    static pthread_key_t    _key;
    static int _len;
};



#endif

