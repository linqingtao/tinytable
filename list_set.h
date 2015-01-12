#ifndef _LIST_SET_H_
#define _LIST_SET_H_
#include <set>
#include <iterator>

#define MAX_LIST_SET_NUM 7

template<typename K>
class ListSet {
public:
    ListSet() : _size(0) {}
    ~ListSet() {
    }
    ListSet(std::set<K>& data) {
        typename std::set<K>::iterator iter = data.begin();
        typename std::set<K>::iterator iter_end = data.end();
        _size = 0;
        for (; iter != iter_end; ++iter) {
            ((K*)_item)[_size++] = *iter;
            if (_size >= MAX_LIST_SET_NUM) {break;}
        }
    }
    struct iterator {
        iterator() : val(NULL), cur(0), size(0) {}
        iterator(K* _val, short _cur, short _size) : val(_val), cur(_cur), size(_size) {}
        K* val;
        short cur;
        short size;
        K operator*() {
            return val[cur];
        }
        K* operator->() {
            return &val[cur];
        }
        iterator& operator++() {
            ++cur;
            if (cur > size) {
                cur = size;
            }
            return *this;
        }
        iterator operator++(int) {
            iterator iter(val, cur, size);
            ++cur;
            if (cur > size) {
                cur = size;
            }
            return iter;
        }
        bool operator==(const iterator& iter) {
            return (cur == iter.cur && val == iter.val);
        }
        bool operator!=(const iterator& iter) {
            return (cur != iter.cur);
        }
    };
    iterator begin() const {
        return iterator((K*)_item, 0, _size);
    }
    iterator end() const {
        return iterator((K*)_item, _size, _size);
    }
    iterator find(K k) const {
        K* val = (K*)_item;
        for (short i = 0; i < _size; ++i) {
            if (val[i] == k) {
                return iterator(val, i, _size);
            } else if (val[i] > k) {
                return iterator(val, _size, _size);
            }
        }
        return iterator(val, _size, _size);
    }
public:
    bool operator==(const ListSet<K>& val) {
        if (_size != val.size()) {
            return false;
        }
        if (_size == 0) {
            return true;
        }
        if (memcmp(_item, val._item, sizeof(K) * _size) != 0) {
            return false;
        }
        return true;
    }
    void clear() {
        _size = 0;
    }
    bool insert(K k) {
        if (_size >= MAX_LIST_SET_NUM) {return false;}
        // try insert
        short i = _size - 1;
        K* val = (K*)_item;
        for (; i >= 0; --i) {
            if (val[i] < k) {
                break;
            } else if (val[i] > k) {
                continue;
            } else {
                return false;
            }
        }
        if (i == _size - 1) {
            val[_size] = k;
        } else {
            for (short j = _size; j > i; --j) {
                val[j] = val[j - 1];
            }
            val[i] = k;
        }
        ++_size;
        return true;
    }
    inline int size() const { return _size;}
    K  first() const {
        return *((K*)_item);
    }
private:
    mutable char  _item[MAX_LIST_SET_NUM * 8];
    short _size;
};


#endif

