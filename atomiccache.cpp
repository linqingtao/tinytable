#include "atomiccache.h"

AtomicQueue<AtomicCacheBase*>* AtomicCacheBase::m_caches = NULL;
int AtomicCacheBase::m_max_delay_seconds = 3;
pthread_rwlock_t* AtomicCacheBase::m_rwlock = NULL;
bool AtomicCacheBase::m_run_thread = false;
uint64_t AtomicCacheBase::m_last_expired_ms = 0;
pthread_t* AtomicCacheBase::m_delete_thread = NULL;

uint64_t currentms() {
    struct timespec ts; 
    clock_gettime(CLOCK_MONOTONIC, &ts);
    return ts.tv_sec *1000ll + ts.tv_nsec/1000000ll;
}

void* run_free_cache(void* args) {
    AtomicCacheBase::m_last_expired_ms = currentms();
    uint64_t cur_ms = 0;
    while (AtomicCacheBase::m_run_thread) {
       int ret = pthread_rwlock_tryrdlock(AtomicCacheBase::m_rwlock);
        Sleeper sleeper;
        while (ret != 0) {
            sleeper.wait();
            ret = pthread_rwlock_tryrdlock(AtomicCacheBase::m_rwlock);
        }
       // printf("rd lock %d\n",ret);  
        AtomicQueue<AtomicCacheBase*>::iterator iter = AtomicCacheBase::m_caches->begin();
        int i = 0;
        for (; iter != AtomicCacheBase::m_caches->end(); iter++) {
            (*iter)->run_free();
            //printf("free caches %d node_t %p base %p\n", i++, iter.ptr, *iter);
        }
       pthread_rwlock_unlock(AtomicCacheBase::m_rwlock);
       // printf("un lock rd %d\n",ret);  
        // sleep
        // if process has timer it will break when the timer expires
        // so we need to make sure the real time
        cur_ms = currentms();
        while (cur_ms < AtomicCacheBase::m_last_expired_ms + AtomicCacheBase::m_max_delay_seconds * 1000) {
            usleep(1000 * (AtomicCacheBase::m_last_expired_ms + AtomicCacheBase::m_max_delay_seconds * 1000 - cur_ms));
            cur_ms = currentms();
        }
        AtomicCacheBase::m_last_expired_ms = cur_ms;
    }
    return NULL;
}
