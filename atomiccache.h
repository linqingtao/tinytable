#ifndef _ATOMIC_CACHE_H_
#define _ATOMIC_CACHE_H_

#include <stdio.h>
#include <stdlib.h>
#include <unistd.h>
#include <pthread.h>
#include "atomicqueue.h"
#include "locks.h"



typedef void (destroy_func)(void* item, void* arg);
extern void* run_free_cache(void* args);


class AtomicCacheBase {
public:
    AtomicCacheBase() {}
    virtual ~AtomicCacheBase() {
    }
    virtual void run_free() = 0;
    virtual bool init(size_t delay_seconds = 3, bool free_T = false, bool  reuse_mem = true, destroy_func* func = NULL, void* destroy_arg = NULL) = 0;

public:
    static AtomicQueue<AtomicCacheBase*>* m_caches;
    static int  m_max_delay_seconds;
    static pthread_rwlock_t* m_rwlock;
    static bool m_run_thread;
    static uint64_t m_last_expired_ms;
protected:
    static pthread_t*      m_delete_thread;
};

template<typename T>
class AtomicCache : public AtomicCacheBase {
public:
    AtomicCache()
    : AtomicCacheBase() {}
    virtual ~AtomicCache() {}
    virtual bool init(size_t delay_seconds = 3, bool free_T = false, bool  reuse_mem = true,  destroy_func* func = NULL, void* destroy_arg = NULL) = 0;
public:
    virtual void push(T t) = 0;
    virtual bool pop(T& t) = 0;
};

template<typename T>
class AtomicLazyCache : public AtomicCache<T> {
public:
    AtomicLazyCache()
    : AtomicCache<T>()
    , m_last_time(0)
    , m_delay_seconds(3)
    , m_reuse_mem(true)
    , m_free_T(false)
    , m_is_init(false)
    , m_refresh(false)
    , m_pop_free(false)
    , m_delete_cache(NULL)
    , m_delay_cache(NULL)
    , m_pop_cache(NULL)
    , m_free_cache(NULL)
    , m_reuse_cache(NULL) {}
    virtual ~AtomicLazyCache() {
        //printf("~Lazycache %p\n", this); 
        run_free();
        if (m_delete_cache != NULL) {
            delete m_delete_cache;
            m_delete_cache = NULL;
        }
        if (m_delay_cache != NULL) {
            delete m_delay_cache;
            m_delay_cache = NULL;
        }
        if (m_free_cache != NULL) {
            AtomicQueue<T>* ptr;
            while (m_free_cache->dequeue(ptr)) {
                if (NULL != ptr) {
                    ptr->setFreeT(m_free_T);
                    delete ptr;
                    ptr = NULL;
                }
            }
            delete m_free_cache;
            m_free_cache = NULL;
        }
        if (m_reuse_cache != NULL) {
            delete m_reuse_cache;
            m_reuse_cache = NULL;
        }
        if (m_pop_cache != NULL) {
            delete m_pop_cache;
            m_pop_cache = NULL;
        }
    }
public:
    virtual bool init(size_t delay_seconds = 3, bool free_T = false, bool  reuse_mem = true,  destroy_func* func = NULL, void* destroy_arg = NULL) {
        if (m_is_init) {
            return true;
        }       
        m_delay_seconds = delay_seconds;
        m_free_T = free_T;
        m_reuse_mem = reuse_mem;
        m_delete_cache = new (std::nothrow)AtomicQueue<T>(m_free_T);
        m_delay_cache = new (std::nothrow)AtomicQueue<T>(m_free_T);
        m_pop_cache = new (std::nothrow)AtomicQueue<T>(m_free_T);
        m_free_cache = new (std::nothrow)AtomicQueue<AtomicQueue<T>*>();
        m_reuse_cache = new (std::nothrow)AtomicQueue<AtomicQueue<T>*>(true);
        if (m_delete_cache == NULL || m_delay_cache == NULL || m_pop_cache == NULL || m_free_cache == NULL ||m_reuse_cache == NULL) {
            goto FAIL;
        }
        m_is_init = true;
        return true;
FAIL:
        if (m_delete_cache != NULL) {
            delete m_delete_cache;
            m_delete_cache = NULL;
        }
        if (m_delay_cache != NULL) {
            delete m_delay_cache;
            m_delay_cache = NULL;
        }
        if (m_free_cache != NULL) {
            delete m_free_cache;
            m_free_cache = NULL;
        }
        if (m_reuse_cache != NULL) {
            delete m_reuse_cache;
            m_reuse_cache = NULL;
        }
        if (m_pop_cache != NULL) {
            delete m_pop_cache;
            m_pop_cache = NULL;
        }
        m_is_init = false;
        return false;
    }
    virtual void push(T t) {
        time_t now = time(NULL);
        if (now - m_last_time >= m_delay_seconds) {
            if (__sync_bool_compare_and_swap(&m_refresh, false, true)) {
                m_last_time = now;
                run_free();
                m_refresh = false;
            }
        }
        m_delete_cache->enqueue(t);
    }
    virtual bool pop(T& t) {
        time_t now = time(NULL);
        if (now - m_last_time >= m_delay_seconds) {
            if (__sync_bool_compare_and_swap(&m_refresh, false, true)) {
                m_last_time = now;
                run_free();
                m_refresh = false;
            }
        }
        if (m_pop_cache->dequeue(t)) {
            return true;
        }
        // set lock to dequeue a new
        if (!__sync_bool_compare_and_swap(&m_pop_free, false, true)) {
            return false;
        }
        AtomicQueue<T>* ptr = NULL;
        if (!m_free_cache->dequeue(ptr)) {
            m_pop_free = false;
            return false;
        }
        m_reuse_cache->enqueue(m_pop_cache);
        (void)__sync_lock_test_and_set(&m_pop_cache, ptr);
        m_pop_free = false;
        return m_pop_cache->dequeue(t);
    }
    virtual void run_free()
    {
        if (!m_reuse_mem) {
            AtomicQueue<T>* ptr = NULL;
            while (m_free_cache->dequeue(ptr)) {
                if (NULL!= ptr){
                    ptr->setFreeT(m_free_T);
                    ptr->clear();
                    m_reuse_cache->enqueue(ptr);
                }
            }
            m_pop_cache->setFreeT(m_free_T);
            m_pop_cache->clear();
        }
        AtomicQueue<T>* p = NULL;
        if (!m_reuse_cache->dequeue(p)) {
            p = new(std::nothrow) AtomicQueue<T>(m_free_T);
        }
        m_free_cache->enqueue(m_delay_cache);
        m_delay_cache = m_delete_cache;
        (void)__sync_lock_test_and_set(&m_delete_cache, p);
    }
private:
    time_t          m_last_time;
protected:
    int             m_delay_seconds;
    bool            m_reuse_mem;
    bool            m_free_T;
    bool            m_is_init;
    bool            m_refresh;
    bool            m_pop_free;
protected:
    AtomicQueue<T>* m_delete_cache;
    AtomicQueue<T>* m_delay_cache;
    AtomicQueue<T>* m_pop_cache;
    AtomicQueue<AtomicQueue<T>*>* m_free_cache;
    AtomicQueue<AtomicQueue<T>*>* m_reuse_cache;
    destroy_func*  m_destroy_func;
    void*          m_destroy_arg;
};


template<typename T>
class AtomicThreadCache : public AtomicCache<T> {
public:
    AtomicThreadCache()
    : AtomicCache<T>()
    , m_delay_seconds(3)
    , m_reuse_mem(true)
    , m_free_T(false)
    , m_is_init(false)
    , m_pop_free(false)
    , m_delete_cache(NULL)
    , m_delay_cache(NULL)
    , m_pop_cache(NULL)
    , m_free_cache(NULL)
    , m_reuse_cache(NULL)
    , m_destroy_func(NULL)
    , m_destroy_arg(NULL) {}
    virtual ~AtomicThreadCache() {
        int ret = pthread_rwlock_trywrlock(AtomicCacheBase::m_rwlock);
        Sleeper sleeper;
        while (ret != 0) {
            sleeper.wait();
            ret = pthread_rwlock_trywrlock(AtomicCacheBase::m_rwlock);
        }
        AtomicCacheBase::m_caches->unsafe_erase(this);
        pthread_rwlock_unlock(AtomicCacheBase::m_rwlock);
        run_free();
        if (m_delete_cache != NULL) {
            delete m_delete_cache;
            m_delete_cache = NULL;
        }
        if (m_delay_cache != NULL) {
            delete m_delay_cache;
            m_delay_cache = NULL;
        }
        if (m_free_cache != NULL) {
            AtomicQueue<T>* ptr;
            while (m_free_cache->dequeue(ptr)) {
                if (NULL != ptr) {
                    delete ptr;
                    ptr = NULL;
                }
            }
            delete m_free_cache;
            m_free_cache = NULL;
        }
        if (m_reuse_cache != NULL) {
            delete m_reuse_cache;
            m_reuse_cache = NULL;
        }
        if (m_pop_cache != NULL) {
            delete m_pop_cache;
            m_pop_cache = NULL;
        }
    };
public:
    virtual void push(T t) {
        m_delete_cache->enqueue(t);
    }
    virtual bool pop(T& t) {
        if (m_pop_cache->dequeue(t)) {
            return true;
        }
        // set lock to dequeue a new
        if (!__sync_bool_compare_and_swap(&m_pop_free, false, true)) {
            return false;
        }
        AtomicQueue<T>* ptr = NULL;
        if (!m_free_cache->dequeue(ptr)) {
            m_pop_free = false;
            return false;
        }
        m_reuse_cache->enqueue(m_pop_cache);
        (void)__sync_lock_test_and_set(&m_pop_cache, ptr);
        m_pop_free = false;
        return m_pop_cache->dequeue(t);
    }
    virtual bool init(size_t delay_seconds = 3, bool free_T = false, bool  reuse_mem = true,  destroy_func* func = NULL, void* destroy_arg = NULL) {
        if (m_is_init) {
            return true;
        }
        int ret = 0;
        Sleeper sleeper;
        m_delay_seconds = delay_seconds;
        m_free_T = free_T;
        m_reuse_mem = reuse_mem;
        if (__sync_bool_compare_and_swap(&AtomicCacheBase::m_delete_thread, NULL, 1)) {
        //if (AtomicCacheBase::m_delete_thread == NULL) {
            AtomicCacheBase::m_delete_thread = new(std::nothrow) pthread_t();
            if (NULL == AtomicCacheBase::m_delete_thread) {
                goto ERROR;
            }
            AtomicCacheBase::m_run_thread = true;
            AtomicCacheBase::m_rwlock = new (std::nothrow) pthread_rwlock_t();
            if (NULL == AtomicCacheBase::m_rwlock) {
                goto ERROR;
            }
            pthread_rwlock_init(AtomicCacheBase::m_rwlock, NULL);
            AtomicCacheBase::m_caches = new (std::nothrow) AtomicQueue<AtomicCacheBase* >(false);
            if (NULL == AtomicCacheBase::m_caches) {
                goto ERROR;
            }
            if (0 != pthread_create(AtomicCacheBase::m_delete_thread, NULL, run_free_cache, NULL)) {
                goto ERROR;
            }
        }
        if (((int)delay_seconds) > AtomicCacheBase::m_max_delay_seconds) {
            AtomicCacheBase::m_max_delay_seconds = delay_seconds;
        }
        m_delete_cache = new (std::nothrow)AtomicQueue<T>();
        m_delay_cache = new (std::nothrow)AtomicQueue<T>();
        m_pop_cache = new (std::nothrow)AtomicQueue<T>();
        m_free_cache = new (std::nothrow)AtomicQueue<AtomicQueue<T>*>();
        m_reuse_cache = new (std::nothrow)AtomicQueue<AtomicQueue<T>*>(true);
        if (m_delete_cache == NULL || m_delay_cache == NULL || m_pop_cache == NULL || m_free_cache == NULL ||m_reuse_cache == NULL) {
            goto FAIL;
        }
        while (AtomicCacheBase::m_caches == NULL) {
            sleeper.wait();
        }
        ret = pthread_rwlock_tryrdlock(AtomicCacheBase::m_rwlock);
        while (ret != 0) {
            sleeper.wait();
            ret = pthread_rwlock_tryrdlock(AtomicCacheBase::m_rwlock);
        }
        AtomicCacheBase::m_caches->enqueue(this);
        pthread_rwlock_unlock(AtomicCacheBase::m_rwlock); 
        m_is_init = true;
        return true;
FAIL:
        if (m_delete_cache != NULL) {
            delete m_delete_cache;
            m_delete_cache = NULL;
        }
        if (m_delay_cache != NULL) {
            delete m_delay_cache;
            m_delay_cache = NULL;
        }
        if (m_free_cache != NULL) {
            delete m_free_cache;
            m_free_cache = NULL;
        }
        if (m_reuse_cache != NULL) {
            delete m_reuse_cache;
            m_reuse_cache = NULL;
        }
        if (m_pop_cache != NULL) {
            delete m_pop_cache;
            m_pop_cache = NULL;
        }
        m_is_init = false;
        return false;
ERROR:
        if (AtomicCacheBase::m_delete_thread != NULL) {
            delete AtomicCacheBase::m_delete_thread;
            AtomicCacheBase::m_delete_thread = NULL;
        }
        AtomicCacheBase::m_run_thread = false;
        if (NULL != AtomicCacheBase::m_caches) {
            delete AtomicCacheBase::m_caches;
            AtomicCacheBase::m_caches = NULL;
        }
        if (NULL != AtomicCacheBase::m_rwlock) {
            delete AtomicCacheBase::m_rwlock;
            AtomicCacheBase::m_rwlock = NULL;
        }
        return false;
    }
    virtual void run_free()
    {
        if (!m_reuse_mem) {
            AtomicQueue<T>* ptr = NULL;
            while (m_free_cache->dequeue(ptr)) {
                if (NULL!= ptr){
                    ptr->setFreeT(m_free_T);
                    ptr->clear();
                    m_reuse_cache->enqueue(ptr);
                }
            }
            m_pop_cache->setFreeT(m_free_T);
            m_pop_cache->clear();
        }
        m_free_cache->enqueue(m_delay_cache);
        AtomicQueue<T>* p = NULL;
        if (!m_reuse_cache->dequeue(p)) {
            p = new(std::nothrow) AtomicQueue<T>(false);
        }
        m_delay_cache = m_delete_cache;
        (void)__sync_lock_test_and_set(&m_delete_cache, p);
       // m_delay_cache->cut(*m_delete_cache);
    }
protected:
    int             m_delay_seconds;
    bool            m_reuse_mem;
    bool            m_free_T;
    bool            m_is_init;
    bool            m_pop_free;
protected:
    AtomicQueue<T>* m_delete_cache;
    AtomicQueue<T>* m_delay_cache;
    AtomicQueue<T>* m_pop_cache;
    AtomicQueue<AtomicQueue<T>*>* m_free_cache;
    AtomicQueue<AtomicQueue<T>*>* m_reuse_cache;
    destroy_func* m_destroy_func;
    void*  m_destroy_arg;
};


#endif

