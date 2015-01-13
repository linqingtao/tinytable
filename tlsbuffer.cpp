#include "tlsbuffer.h"

pthread_key_t TlsBuffer::_key = 0;
int TlsBuffer::_len = 0;

void free_local_buffer (void* data) {
    delete (char*)data;
}

bool TlsBuffer::tls_init(int len) {    
    if (_key > 0) {return true;}
    int ret = pthread_key_create(&_key, free_local_buffer);
    if (0 != ret) {
        TT_WARN_LOG("local key create error");
        return false;
    }
    _len = len;
    return true;
}

char*  TlsBuffer::get() {
    char* obj = (char*)pthread_getspecific(_key);
    if (obj == NULL) {
        // for string we new SIZE+1
        obj = new(std::nothrow) char[_len + 1];
        if (NULL == obj) {
            return NULL;
        }
        // insert to local storage
        int ret = pthread_setspecific(_key, obj);
        if (ret != 0) {
            TT_WARN_LOG("insert local storage error");
            delete obj;
            obj = NULL;
            return NULL;
        }
    }
    return obj;
}

bool TlsBuffer::tls_destroy() {
    return true;
}



