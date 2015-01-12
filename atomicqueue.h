#ifndef _ATOMIC_QUEUE_H_
#define _ATOMIC_QUEUE_H_
#include <stdint.h>


template<typename T>
inline void FreeAtomicCacheItem(T t, bool free_cache) {
    (void)t;
    (void)free_cache;
}

template<typename T>
inline void FreeAtomicCacheItem(T* t, bool free_cache) {
    if (t != NULL) {
        if (free_cache) {
            delete t;
            t = NULL;
        }
    }
}

template< typename T>
class AtomicQueue
{
private:
    // node struct
	struct node_t;
	struct pointer_t 
	{
        union {
            node_t* ptr;
            struct {
                unsigned long long ptr : 48;
                // the count is used to avoid ABA
                unsigned int count : 16;
            } inode;
        };
		pointer_t(): ptr(NULL){}
		pointer_t(node_t* node, const long c)
        : ptr(node) {
            inode.count = c;
        }
		pointer_t(const pointer_t& p)
        : ptr(p.ptr){}

		pointer_t(const pointer_t* p): ptr(p->ptr) {}
	};

	struct node_t 
	{
		T value;
		pointer_t next;
	};

	pointer_t Head;
    pointer_t Tail;
    bool      free_cache;
    uint32_t       m_count;
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
	AtomicQueue(bool _free = true)
	:free_cache(_free)
    , m_count(0) {
		node_t* pNode = new node_t();
		Head.inode.ptr = Tail.inode.ptr = (unsigned long long)pNode;
	}
	~AtomicQueue() {
        clear();
        delete (node_t*)Head.inode.ptr;
    }
    inline uint32_t count() {return m_count;}
    void setFreeCache(bool _free) {
        free_cache = _free;
    }
    int clear() {
        T t;
        while (dequeue(t)) {
            FreeAtomicCacheItem(t, free_cache);
        }
        return 0;
    }

    int cut(AtomicQueue& queue) {
        int num = 0;
        T t;
        while (queue.dequeue(t)) {
            enqueue(t);
            ++num;
        }
        return num;
    }

    void enqueue(T t);
    void unsafe_erase(T t);
    void setFreeT(bool free_) {
        free_cache = free_;
    }
	bool dequeue(T& t);

};

template <typename T>
void AtomicQueue<T>::unsafe_erase(T t) {
    AtomicQueue<T>::iterator iter = begin();
    if (iter == end()) {
        return;
    }
    if (*iter == t) {
        dequeue(t);
        FreeAtomicCacheItem(t, free_cache);
        return;
    }
    AtomicQueue<T>::iterator iter_prev = iter++;
    AtomicQueue<T>::iterator it = end();
    for (; iter != it; iter++) {
        if (*iter == t) {
            node_t* prev_node = iter_prev.ptr;
            node_t* cur_node = iter.ptr;
            prev_node->next = cur_node->next;
            FreeAtomicCacheItem(cur_node->value, free_cache);
            delete cur_node;
            cur_node = NULL;
            return;
        }
        iter_prev = iter;
    }
}


template <typename T>
void AtomicQueue<T>::enqueue(T t) {
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
                if(next.ptr == __sync_val_compare_and_swap(&((node_t*)tail.inode.ptr)->next.ptr,next.ptr,p.ptr))
                {
                    __sync_fetch_and_add(&m_count, 1);
                    flag = false;
                }
            } else {
                pointer_t p((node_t*)next.inode.ptr,tail.inode.count+1);
                __sync_bool_compare_and_swap(&Tail.ptr,tail.ptr,p.ptr);
            }
        }
    } while (flag);
}

template <typename T>
bool AtomicQueue<T>::dequeue(T& t) {
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
        if (head.ptr == Head.ptr) {
            if(head.inode.ptr == tail.inode.ptr) {
                if(0 == next.inode.ptr) {
                    return false;
                }
                pointer_t p((node_t*)next.inode.ptr,tail.inode.count+1);
                (void)__sync_val_compare_and_swap(&Tail.ptr,tail.ptr,p.ptr);
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

