#ifndef _ATOMIC_SKIPLIST_H_
#define _ATOMIC_SKIPLIST_H_

#include "random.h"
#include <limits>
#include "atomiccache.h"


#define MAX_LEVEL 24 //Should be choosen as log(1/p)(n)

template<typename T>
bool inline CASPTR(T** ptr, T* old, T* value){
    return __sync_bool_compare_and_swap(ptr, old, value);
}


template<typename T>
struct Node {
    T key;
    unsigned short topLevel;
    Node** next;
    
    Node(int level)
    : topLevel(level) {
        next = static_cast<Node**>(calloc(level + 1, sizeof(Node *)));
    }
    ~Node(){
        free(next);    
    }
};


template<typename T>
inline Node<T>* Unmark(Node<T>* node){
    return reinterpret_cast<Node<T>* >(reinterpret_cast<unsigned long long>(node) & (~0l - 1));
}

template<typename T>
inline Node<T>* Mark(Node<T>* node){
    return reinterpret_cast<Node<T>* >(reinterpret_cast<unsigned long long>(node) | 0x1);
}

template<typename T>
inline bool IsMarked(Node<T>* node){
    return reinterpret_cast<unsigned long long>(node) & 0x1;
}

template<typename T>
class SkipList {
    public:
        SkipList(bool desc = true, bool lazy = true, bool reuse_mem = true, void* func = (void*)DefaultComparator, void* compare_arg = NULL, T* limit = NULL);
        ~SkipList();

        bool add(T value);
        bool remove(T value);
        bool contains(T value);
        void clear();
        struct iterator {
        iterator() : ptr(NULL), level(0) {}
        iterator(Node<T>* node, int level_) : ptr(Unmark(node)), level(level_) {}
        Node<T>* ptr;
        int level;
        T operator*() {
            if (ptr == NULL) {
                return (T)(0);
            }
            return ptr->key;
        }
        T* operator->() {
            if (ptr == NULL) {
                return NULL;
            }
            return &(ptr->key);
        }
        iterator& operator++() {
            if (ptr == NULL) {
                return *this;
            }
            ptr = Unmark(ptr->next[level]);
            return *this;
        }
        iterator operator++(int) {
            iterator iter(ptr, level);
            ptr = Unmark(ptr->next[level]);
            return iter;
        }
        bool operator==(const iterator& iter) const {
            return (ptr == iter.ptr && level == iter.level);
        }
        bool operator!=(const iterator& iter) const {
            return (ptr != iter.ptr);
        }
    };
    iterator begin(int level = 0) {
        return iterator(head->next[level], level);
    }
    iterator end(int level = 0) {
        return iterator(tail, level);
    }       
    inline int size() {return size_;}
    void  setCompare(void* func) {
        compare_ = func;
    }
    void  setCompareArg(void* arg) {
        compare_arg_ = arg;
    }
    static int DefaultComparator(const T& a, const T& b, void* arg){
        if (a < b) {
            return -1;
        } else if (a > b) {
            return 1;
        } else {
            return 0;
        }
    }
    private:
        int randomLevel();
        bool find(T key, Node<T>** preds, Node<T>** succs, bool all);
        Node<T>* newNode(T key, int height);
        Node<T>* head;
        Node<T>* tail;
        AtomicCache<Node<T>*>* _cache[MAX_LEVEL + 1];
        Random rnd_;
        void*  compare_;
        void*  compare_arg_;
        int size_;
        int desc_;
};

template<typename T>
Node<T>* SkipList<T>::newNode(T key, int height){
    Node<T>* node = NULL;
    if (!_cache[height]->pop(node)) {
        node = new Node<T>(height);
    }
    node->key = key;
    node->topLevel = height;
    for (int i = 0; i < height + 1; i++) {
        node->next[i] = NULL;
    }
    return node;
}

template<typename T>
SkipList<T>::SkipList(bool desc, bool lazy, bool reuse_mem, void* func, void* arg, T* limit) : rnd_(0xdeadbeef), compare_(func), compare_arg_(arg), desc_(desc ? -1 : 1) {
    // cache
    if (lazy) {
        for (int i = 0; i < MAX_LEVEL + 1; i++) {
            _cache[i] = new AtomicLazyCache<Node<T>* >();
        }
    } else {
        for (int i = 0; i < MAX_LEVEL + 1; i++) {
            _cache[i] = new AtomicThreadCache<Node<T>* >();
        }
    }
    for (int i = 0; i < MAX_LEVEL + 1; i++) {
        _cache[i]->init(3, true, reuse_mem);
    }
    // here has a little queation
    // min or max will not be used int app
    if (desc) {
        head = newNode(std::numeric_limits<T>::max(), MAX_LEVEL);
        if (limit != NULL) {
            tail = newNode(*limit, MAX_LEVEL);
        } else {
            tail = newNode(std::numeric_limits<T>::min(), MAX_LEVEL);
        }
    } else {
        head = newNode(std::numeric_limits<T>::min(), MAX_LEVEL);
        if (limit != NULL) {
            tail = newNode(*limit, MAX_LEVEL);
        } else {
            tail = newNode(std::numeric_limits<T>::max(), MAX_LEVEL);
        }
    }
    for(int i = 0; i < MAX_LEVEL + 1; ++i){
        head->next[i] = tail;
    }
}


template<typename T>
void SkipList<T>::clear() {
    SkipList<T>::iterator iter = begin();
    while (iter != end()) {
        remove(*iter);
        iter++;
    }
}


template<typename T>
SkipList<T>::~SkipList(){
    delete head;
    delete tail;
    head = NULL;
    tail = NULL;
    if (_cache != NULL) {
        for (int i = 0; i < MAX_LEVEL + 1; i++) {
            delete _cache[i];
            _cache[i] = NULL;
        }
    }
}

template<typename T>
int SkipList<T>::randomLevel(){
    static const unsigned int kBranching = 4;
    int height = 0;
    while (height < MAX_LEVEL && ((rnd_.Next() % kBranching) == 0)) {
        height++;
    }  
    return (height >= MAX_LEVEL) ? MAX_LEVEL : height;
}

template<typename T>
bool SkipList<T>::add(T key){
    int topLevel = randomLevel();
    Node<T>* preds[MAX_LEVEL + 1];
    Node<T>* succs[MAX_LEVEL + 1];
    Node<T>* newElement = newNode(key, topLevel);
    while(true){
        if(find(key, preds, succs, false)){
            _cache[topLevel]->push(newElement);
            return false;
        } else {
            for(int level = 0; level <= topLevel; ++level){
                newElement->next[level] = succs[level];
            }
            if(CASPTR(&preds[0]->next[0], succs[0], newElement)){
                for(int level = 1; level <= topLevel; ++level){
                    while(true){
                        if(CASPTR(&preds[level]->next[level], succs[level], newElement)){ 
                            break;
                        } else {
                            find(key, preds, succs, false);
                            // the node may be deleted when the first is inserted
                            // so we should check the mark
                            // the succ also may be changed so we reset the success
                            for(int j = level; j <= topLevel; ++j){
                                newElement->next[j] = succs[j];
                            }
                        }
                    }
                }
                __sync_fetch_and_add(&size_, 1);
                return true;
            }
        }
    }
}

template<typename T>
bool SkipList<T>::remove(T key){
    Node<T>* preds[MAX_LEVEL + 1];
    Node<T>* succs[MAX_LEVEL + 1];
    while(true){
        // only check the level 0 node 
        // in order to make sure list will mark all the level node
        if(!find(key, preds, succs, true)){
            return false;
        } else {
            Node<T>* nodeToRemove = succs[0];
            for(int level = nodeToRemove->topLevel; level > 0; --level){
                Node<T>* succ = NULL;
                do {
                    succ = nodeToRemove->next[level];
                    if(IsMarked(succ)){
                        break;
                    }
                } while (!CASPTR(&nodeToRemove->next[level], succ, Mark(succ)));
            }

            while(true){
                Node<T>* succ = nodeToRemove->next[0];
                if(IsMarked(succ)){
                    return false;
                } else if(CASPTR(&nodeToRemove->next[0], succ, Mark(succ))){
                    find(key, preds, succs, false);
                    _cache[nodeToRemove->topLevel]->push(nodeToRemove);
                    __sync_fetch_and_sub(&size_, 1);
                    return true;
                }
            }
        }
    }
}

template<typename T>
bool SkipList<T>::contains(T key){
    Node<T>* pred = head;
    Node<T>* curr = NULL;
    Node<T>* succ = NULL;

    for(int level = MAX_LEVEL; level >= 0; --level){
        curr = Unmark(pred->next[level]);
        while(true){
            succ = Unmark(curr->next[level]);
            while(IsMarked(succ)){
                curr = Unmark(curr->next[level]);
                succ = curr->next[level]; 
            }
            if (desc_ == 1) {
                if(((int(*)(const T&, const T&, void*))compare_)(curr->key, key, compare_arg_) < 0){
                    pred = curr;
                    curr = succ;
                } else {
                    break;
                }
            } else  {
                if(((int(*)(const T&, const T&, void*))compare_)(curr->key, key, compare_arg_) > 0){
                    pred = curr;
                    curr = succ;
                } else {
                    break;
                }
            }
        }
    }
    bool found = curr->key == key;
    return found;
}

template<typename T>
bool SkipList<T>::find(T key, Node<T>** preds, Node<T>** succs, bool all){
    Node<T>* pred = NULL;
    Node<T>* curr = NULL;
    Node<T>* succ = NULL;
retry:
    pred = head;
    for(int level = MAX_LEVEL; level >= 0; --level){
        curr = pred->next[level];
        while(true){
            if(IsMarked(curr)){
                goto retry;
            }
            succ = curr->next[level];
            // if curr is deleted
            while(IsMarked(succ)){
                if(!CASPTR(&pred->next[level], curr, Unmark(succ))){
                    goto retry;
                }
                curr = pred->next[level];
                if(IsMarked(curr)){
                    goto retry;
                }
                succ = curr->next[level];
            }
            if (desc_ == 1) {
                if(((int(*)(const T&, const T&, void*))compare_)(curr->key, key, compare_arg_) < 0){
                    pred = curr;
                    curr = succ;
                } else {
                    break;
                }
            } else {
                if(((int(*)(const T&, const T&, void*))compare_)(curr->key, key, compare_arg_) > 0){
                    pred = curr;
                    curr = succ;
                } else {
                    break;
                }
            }
        }
        preds[level] = pred;
        succs[level] = curr;
    }
    bool found = curr->key == key;
    if (found && all) {
        for (int j = 1; j <= curr->topLevel; ++j) {
            if (succs[j]->key != key) {
                return false;
            }
        }
    }
    return found;
}


#endif
