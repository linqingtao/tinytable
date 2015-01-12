#ifndef _ATOMIC_BLOCK_QUEUE_H_
#define _ATOMIC_BLOCK_QUEUE_H_
#include <pthread.h>
#include <sys/time.h>
#include <stdint.h>

template<typename T>
inline void FreeAtomicBlockQueueItem(T t, bool free_cache) {
}

template<typename T>
inline void FreeAtomicBlockQueueItem(T* t, bool free_cache) {
    if (t != NULL) {
        if (free_cache) {
            delete t;
            t = NULL;
        }
    }
}


template<typename T>
class AtomicBlockQueue
{
private:
	struct node_t;
	struct pointer_t 
	{
        union {
            node_t* ptr;
            struct {
                unsigned long long ptr : 48;
                unsigned int count : 16;
            } inode;
        };
		pointer_t(): ptr(NULL) {}
		pointer_t(node_t* node, const long c )
        : ptr(node) {
            inode.count = c;
        }
		pointer_t(const pointer_t& p)
        : ptr(p.ptr) {}
		pointer_t(const pointer_t* p): ptr(p->ptr) {}

	};
	// node structure
	struct node_t 
	{
		T value;
		pointer_t next;
	};
	pointer_t Head;
    pointer_t Tail;
    bool      free_cache;
    uint32_t       m_count;
    pthread_cond_t m_condition;
    pthread_mutex_t m_mutex;
public:
    // these two functions are not safe in multithread
    struct iterator {
        iterator() : ptr(NULL) {}
        iterator(node_t* node) : ptr(node) {}
        node_t* ptr;
        T operator*() {
            if (ptr == NULL) {
                return (T)(0);
            }
            return ptr->value;
        }
        T* operator->() {
            if (ptr == NULL) {
                return NULL;
            }
            return &(ptr->value);
        }
        iterator& operator++() {
            if (ptr == NULL) {
                return *this;
            }
            ptr = (node_t*)ptr->next.inode.ptr;
            return *this;
        }
        iterator operator++(int) {
            iterator iter(ptr);
            ptr = (node_t*)ptr->next.inode.ptr;
            return iter;
        }
        bool operator==(const iterator& iter) {
            return (ptr == iter.ptr);
        }
        bool operator!=(const iterator& iter) {
            return (ptr != iter.ptr);
        }
    };
    iterator begin() {
        return iterator((node_t*)((node_t*)Head.inode.ptr)->next.inode.ptr);
    }
    iterator end() {
        return iterator(NULL);
    }
public:	
	// default constructor
	AtomicBlockQueue(bool _free = true)
	: free_cache(_free)
    , m_count(0) {
		node_t* pNode = new node_t();
		Head.inode.ptr = Tail.inode.ptr = (unsigned long long)pNode;
        pthread_cond_init(&m_condition, NULL);
        pthread_mutex_init(&m_mutex, NULL);
	}
	~AtomicBlockQueue() {
        clear();
        delete (node_t*)Head.inode.ptr;
        pthread_cond_destroy(&m_condition);
        pthread_mutex_destroy(&m_mutex);
    }
    inline uint32_t count() {return m_count;}
    void setFreeCache(bool _free) {
        free_cache = _free;
    }
    int clear() {
        T t;
        while (try_dequeue(t)) {
            FreeAtomicBlockQueueItem(t, free_cache);
        }
        return 0;
    }

    int cut(AtomicBlockQueue& queue) {
        int num = 0;
        T t;
        while (queue.try_dequeue(t)) {
            enqueue(t);
            ++num;
        }
        return num;
    }

    void enqueue(T t);
    void unsafe_erase(T t);
    void setFreeT(bool free_) {free_cache = free_;}
	bool try_dequeue(T& t);
    bool dequeue(T& t);
};

template <typename T>
void AtomicBlockQueue<T>::unsafe_erase(T t) {
    AtomicBlockQueue<T>::iterator iter = begin();
    if (iter == end()) {
        return;
    }
    if (*iter == t) {
        try_dequeue(t);
        FreeAtomicBlockQueueItem(t, free_cache);
        return;
    }
    AtomicBlockQueue<T>::iterator iter_prev = iter++;
    AtomicBlockQueue<T>::iterator it = end();
    for (; iter != it; iter++) {
        if (*iter == t) {
            node_t* prev_node = iter_prev.ptr;
            node_t* cur_node = iter.ptr;
            prev_node->next = cur_node->next;
            FreeAtomicBlockQueueItem(cur_node->value, free_cache);
            delete cur_node;
            cur_node = NULL;
            return;
        }
        iter_prev = iter;
    }
}


template <typename T>    
void AtomicBlockQueue<T>::enqueue(T t)
{
    node_t* pNode = new node_t(); 
    pNode->value = t;

    bool flag = true;
    do
    {
        pointer_t tail(Tail);
        bool nNullTail = (0==tail.inode.ptr); 
        node_t* tail_ptr = (node_t*)tail.inode.ptr;
        pointer_t next((nNullTail)? 0 : (node_t*)tail_ptr->next.inode.ptr, (nNullTail)? 0 : tail_ptr->next.inode.count);

        if(tail.ptr == Tail.ptr)
        {
            if(0 == next.inode.ptr) {
                pointer_t p(pNode,next.inode.count+1);
                if(next.ptr == __sync_val_compare_and_swap(&((node_t*)tail.inode.ptr)->next.ptr,next.ptr,p.ptr)) {
                    __sync_fetch_and_add(&m_count, 1);
                    flag = false;
                    // signal
                    pthread_mutex_lock(&m_mutex);
                    pthread_cond_signal(&m_condition);
                    pthread_mutex_unlock(&m_mutex);
                }
            } else {
                pointer_t p((node_t*)next.inode.ptr,tail.inode.count+1);
                __sync_bool_compare_and_swap(&Tail.ptr,tail.ptr,p.ptr);
            }
        }
    } while (flag);
}

template<typename T>
bool AtomicBlockQueue<T>::dequeue(T& t) {
    bool lock = false;
    while (!try_dequeue(t)) {
        if (!lock) {
            pthread_mutex_lock(&m_mutex);
        }
        lock = true;
        struct timeval now;
        struct timespec timeout;
        gettimeofday(&now, NULL);
        timeout.tv_sec = now.tv_sec + 5;
        timeout.tv_nsec = now.tv_usec * 1000;
        pthread_cond_timedwait(&m_condition, &m_mutex, &timeout);
    }
    if (lock) {
        pthread_mutex_unlock(&m_mutex);
    }
    return true;
}

template <typename T>
bool AtomicBlockQueue<T>::try_dequeue(T& t)
{
    pointer_t head;
    bool flag = true;
    do
    {
        head = Head;
        pointer_t tail(Tail);
        if(head.inode.ptr == 0) {
            return false;
        }

        pointer_t next(((node_t*)head.inode.ptr)->next);
        if (head.ptr == Head.ptr)
        {
            if(head.inode.ptr == tail.inode.ptr) {
                if(0 == next.inode.ptr) {
                    return false;
                }
                pointer_t p((node_t*)next.inode.ptr,tail.inode.count+1);
                __sync_val_compare_and_swap(&Tail.ptr,tail.ptr,p.ptr);
            } else {
                t = ((node_t*)next.inode.ptr)->value;
                pointer_t p((node_t*)next.inode.ptr,head.inode.count+1);
                if (head.ptr == __sync_val_compare_and_swap(&Head.ptr, head.ptr, p.ptr)) {
                    __sync_fetch_and_sub(&m_count, 1);
                    flag = false;
                }
            }
        }
    } while(flag);
    delete (node_t*)head.inode.ptr;
    return true;
}


#endif

