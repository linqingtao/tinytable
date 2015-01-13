#ifndef _TLS_H_
#define _TLS_H_
#include <stdint.h>
#include <string>
#include <pthread.h>
#include "tinytable_log_utils.h"

template <typename T>
class Tls {
public:
    Tls()
    : _key(0) {}
    ~Tls(){}
public:
    bool tls_init();
    bool tls_destroy();
    T* get();
private:
    pthread_key_t    _key;
};

template <typename T>
void free_local_T (void* data) {
    delete (T*)data;
}


template <typename T>
bool Tls<T>::tls_init() {
    if (_key > 0) {return true;}
    int ret = pthread_key_create(&_key, free_local_T<T>);
    if (0 != ret) {
        return false;
    }
    return true;
}

template <typename T>
bool Tls<T>::tls_destroy() {
    return true;
}

template <typename T>
T* Tls<T>::get() {
    T* obj = (T*)pthread_getspecific(_key);
    if (obj == NULL) {
        obj = new(std::nothrow) T;
        if (NULL == obj) {
            return NULL;
        }
        // insert to local storage
        int ret = pthread_setspecific(_key, obj);
        if (ret != 0) {
            delete obj;
            obj = NULL;
            return NULL;
        }
    }
    return obj;   
}



#endif

