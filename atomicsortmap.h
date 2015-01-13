#ifndef _ATOMIC_SORT_MAP_H_
#define _ATOMIC_SORT_MAP_H_

#include <stdint.h>
#include <unordered_map>
#include <vector>
#include "atomicskiplist.h"
#include "locks.h"
#include <string.h>

template<typename T>
class AtomicSortmap {
public:
    AtomicSortmap(bool desc = true, bool reuse_mem = true, void* compare = (void*)SkipList<T>::DefaultComparator, void* arg = NULL, T* limit = NULL);
    ~AtomicSortmap();
    typedef typename SkipList<T>::iterator real_iterator;
    typedef typename std::vector<T>::iterator dump_iterator;
    real_iterator real_begin() {
        return sort_list_->begin();
    }
    real_iterator real_end() {
        return sort_list_->end();
    }
    // this iterator need to check begin&&end idx
    // if idx not equal should get them again
    dump_iterator dump_begin(int idx) {
        return sort_vec_[idx]->begin();
    }
    dump_iterator dump_end(int idx) {
        return sort_vec_[idx]->end();
    }
    int getDumpIdx() const {return vec_index_;}
public:
    bool  insert(T t, bool overwrite = false);
    bool  remove(T t);
    void  clear();
public:
    bool exist(T t, bool real = true);
    bool get(int rank, T& t, bool real = true);
    bool get(int begin, int end, std::vector<T>& vec, bool real = true);
    int  size(bool real = true);
    // fix me
    // if this func is called
    // in dump case only integer or pointer can be used
    int  rank(const T t, bool real = true);
private:
    // fix me
    // this func is only  for integer and pointer
    // can not be used for struct or class
    uint64_t key(T t) {
        uint64_t ret = 0;
        if (sizeof(T) <= 8) {
           memcpy(&ret, &t, sizeof(T)); 
        } else {
            throw;
        }
        return ret;
    }
public:
    void  dump();
private:
    SkipList<T>* sort_list_;
    std::vector<T>* sort_vec_[2];
    std::unordered_map<uint64_t, int>* sort_rank_[2];
    int       vec_index_;
    bool      dump_status_;
    time_t    last_refresh_time;
    void*     compare_;
    void*     compare_arg_;
    pthread_rwlock_t _rwlock;
};

template<typename T>
AtomicSortmap<T>::AtomicSortmap(bool desc, bool reuse_mem, void* compare, void* arg, T* limit) 
: vec_index_(0)
, dump_status_(false)
, last_refresh_time(time(NULL))
, compare_(compare)
, compare_arg_(arg) {
    sort_list_ = new SkipList<T>(desc, true, reuse_mem, compare, arg, limit);
    sort_vec_[0] = new std::vector<T>();
    sort_vec_[1] = new std::vector<T>();
    sort_rank_[0] = new std::unordered_map<uint64_t, int>();
    sort_rank_[1] = new std::unordered_map<uint64_t, int>();
    pthread_rwlock_init(&_rwlock, NULL);
}

template<typename T>
AtomicSortmap<T>::~AtomicSortmap() {
    delete sort_vec_[0];
    sort_vec_[0] = NULL;
    delete sort_vec_[1];
    sort_vec_[1] = NULL;
    delete sort_rank_[0];
    sort_rank_[0] = NULL;
    delete sort_rank_[1];
    sort_rank_[1] = NULL;
    delete sort_list_;
    sort_list_ = NULL;
    dump_status_ = false;
    pthread_rwlock_destroy(&_rwlock);
}


template<typename T>
void AtomicSortmap<T>::clear() {
    int ret = pthread_rwlock_trywrlock(&_rwlock);
    Sleeper sleeper;
    while (ret != 0) {
        sleeper.wait();
        ret = pthread_rwlock_trywrlock(&_rwlock);
    }   
    sort_vec_[0]->clear();
    sort_vec_[1]->clear();
    sort_list_->clear();
    sort_rank_[0]->clear();
    sort_rank_[1]->clear();
    pthread_rwlock_unlock(&_rwlock);
}

template<typename T>
void AtomicSortmap<T>::dump() {
    if (dump_status_ == true) {
        return;
    }
    if (!__sync_bool_compare_and_swap(&dump_status_, false, true)) {
        return;
    }
    int ret = pthread_rwlock_trywrlock(&_rwlock);
    Sleeper sleeper;
    while (ret != 0) {
        sleeper.wait();
        ret = pthread_rwlock_trywrlock(&_rwlock);
    }
    last_refresh_time = time(NULL);
    typename SkipList<T>::iterator iter = sort_list_->begin();
    typename SkipList<T>::iterator end = sort_list_->end();
    int idx = vec_index_ == 0 ? 1 : 0;
    sort_vec_[idx]->clear();
    sort_rank_[idx]->clear();
    int rank = 1;
    for (; iter != end; iter++) {
        sort_vec_[idx]->push_back(*iter);
        (*sort_rank_[idx])[key(*iter)] = rank++;
    }
    vec_index_ = idx;
    dump_status_ = false;
    pthread_rwlock_unlock(&_rwlock);
}

template<typename T>
bool  AtomicSortmap<T>::insert(T t, bool overwrite) {
    int res = pthread_rwlock_tryrdlock(&_rwlock);
    Sleeper sleeper;
    while (res != 0) {
        sleeper.wait();
        res = pthread_rwlock_tryrdlock(&_rwlock);
    }
    bool ret = sort_list_->add(t);
    if (overwrite) {
        while (!ret) {
            sort_list_->remove(t);
            ret = sort_list_->add(t);
        }
    }
    // refresh
    /*if (refresh_time_ > 0) {
        time_t now = time(NULL);
        if (now - last_refresh_time >= refresh_time_) {
            dump();
        }
    }*/
    pthread_rwlock_unlock(&_rwlock);
    return ret;
}

template<typename T>
bool  AtomicSortmap<T>::remove(T t) {
    int res = pthread_rwlock_tryrdlock(&_rwlock);
    Sleeper sleeper;
    while (res != 0) {
        sleeper.wait();
        res = pthread_rwlock_tryrdlock(&_rwlock);
    }
    bool ret = sort_list_->remove(t);
    // refresh
    /*if (refresh_time_ > 0) {
        time_t now = time(NULL);
        if (now - last_refresh_time >= refresh_time_) {
            dump();
        }
    }*/
    pthread_rwlock_unlock(&_rwlock);
    return ret;
}
template<typename T>
bool AtomicSortmap<T>::exist(T t, bool real) {
    if (real) {
        return sort_list_->contains(t);
    } else {
        int idx = vec_index_;
        return (sort_rank_[idx]->find(key(t)) != sort_rank_[idx]->end());
    }
}

template<typename T>
bool AtomicSortmap<T>::get(int rank, T& t, bool real) {
    if (rank <= 0) {return false;}
    if (real) {
        typename SkipList<T>::iterator iter = sort_list_->begin();
        typename SkipList<T>::iterator end = sort_list_->end();
        int i = 1;
        bool flag = false;
        for (; iter != end; iter++) {
            if (i++ == rank) {
                flag = true;
                break;
            }
        }
        if (flag) {
            t = *iter;
        }
        return flag;
    } else {
        int idx = vec_index_;
        if (sort_vec_[idx]->size() <rank) {
            return false;
        }
        t = (*sort_vec_[idx])[rank - 1];
        return true;
    }
}

template<typename T>
bool AtomicSortmap<T>::get(int begin, int end, std::vector<T>& vec, bool real) {
    if (begin <=0 || end <= 0 || end < begin) {
        return false;
    }
    if (real) {
        typename SkipList<T>::iterator iter = sort_list_->begin();
        typename SkipList<T>::iterator end = sort_list_->end();
        int i = 1;
        bool flag = false;
        for (; iter != end; iter++) {
            if (i == begin) {
                flag = true;
                break;
            }
            i++;
        }
        if (flag) {
            for (; iter != end; iter++) {
                if (i == end) {break;}
                vec.push_back(*iter);
                i++;
            }
        }
        return true;
    } else {
        int idx = vec_index_;
        if (sort_vec_[idx]->size() < begin) {
            return true;
        }
        end = end > sort_vec_[idx]->size() ? sort_vec_[idx]->size() : end;
        for (int i = begin; i <= end; i++) {
            vec.push_back((*sort_vec_[idx])[i - 1]);
        }
        return true;
    }
}

template<typename T>
int  AtomicSortmap<T>::size(bool real) {
    if (real) {
        return sort_list_->size();
    } else {
        return sort_vec_[vec_index_]->size();
    }
}


// fix me
// if this func is called
// in not real case only integer or pointer can be used
template<typename T>
int  AtomicSortmap<T>::rank(const T t, bool real) {
    if (real) {
        typename SkipList<T>::iterator iter = sort_list_->begin();
        typename SkipList<T>::iterator end = sort_list_->end();
        int i = 1;
        int ret = 0;
        bool flag = false;
        for (; iter != end; iter++) {
            ret = ((int(*)(const T&, const T&, void*))compare_)(t, *iter, compare_arg_);
            if (ret < 0) {
                i++;
                continue;;
            } else if (ret == 0) {
                return i;
            } else {
                return -1;
            }
        }
    } else {
        int idx = vec_index_;
        std::unordered_map<uint64_t, int>::iterator iter = sort_rank_[idx]->find(key(t));
        if (iter == sort_rank_[idx]->end()) {
            return -1;
        }
        return iter->second;
    }
}


#endif

