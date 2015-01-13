#ifndef _LIST_MAP_H_
#define _LIST_MAP_H_

#define MAX_LIST_MAP_NUM 7

template<typename K, typename V>
struct ClassLen
{
    static const int len = 0;
};


template<typename K, typename V>
struct MapPair
{
    K first;
    V second;
    static const int SIZE = sizeof(MapPair);
    MapPair(K& k, V& v)
    : first(k)
    , second(v) {}
};

template<typename K, typename V>
class ListMap {
public:
    ListMap() : _size(0) {}
    ~ListMap() {
        for (int i = 0; i < _size; ++i) {
            ((MapPair<K, V>*)(_item + i * sizeof(MapPair<K, V>)))->~MapPair<K, V>();
        }
    }
    struct iterator {
        typedef MapPair<K, V> val_type;
        iterator() : list_map(NULL), cur(0) {}
        iterator(ListMap<K, V>* _map, short _cur) : list_map(_map), cur(_cur) {}
        ListMap<K, V>* list_map;
        short cur;
        val_type operator*() {
            MapPair<K, V>* val = (MapPair<K, V>*)list_map->_item;
            return val[list_map->_map[cur]];
        }
        val_type* operator->() {
            MapPair<K, V>* val = (MapPair<K, V>*)list_map->_item;
            return &val[list_map->_map[cur]];
        }
        iterator& operator++() {
            ++cur;
            short size = list_map->_size;
            if (cur > size) {
                cur = size;
            }
            return *this;
        }
        iterator operator++(int) {
            iterator iter(list_map, cur);
            short size = list_map->_size;
            ++cur;
            if (cur > size) {
                cur = size;
            }
            return iter;
        }
        bool operator==(const iterator& iter) {
            return (cur == iter.cur && list_map == iter.list_map);
        }
        bool operator!=(const iterator& iter) {
            return (cur != iter.cur);
        }
    };
    iterator begin() {
<<<<<<< HEAD
        return iterator(this, 0);
    }
    iterator end() {
=======
        //return iterator((MapPair<K, V>*)_item, _map, _size, 0);
        return iterator(this, 0);
    }
    iterator end() {
        //return iterator((MapPair<K, V>*)_item, _map, _size, _size);
>>>>>>> memtable
        return iterator(this, _size);
    }
public:
    bool insert(K& k, V& v) {
        if (_size >= MAX_LIST_MAP_NUM) {return false;}
        MapPair<K, V>* _pair = (MapPair<K, V>*)_item;
        // try insert
        short i = 0;    
        for (; i < _size; ++i) {
            short cur = _map[i];
            if (_pair[cur].first < k) {
                continue;
            } else if (_pair[cur].first > k) {
                break;
            } else {
                return false;
            }
        }
        new(_item + _size * sizeof(MapPair<K, V>)) MapPair<K, V>(k ,v);
        if (i == _size) {
            _map[_size] = _size;
        } else {
            for (short j = _size; j > i; --j) {
                _map[j] = _map[j - 1];
            }
            _map[i] = _size;
        }
        ++_size;
        return true;
    }
    inline int size() { return _size;}
    K  firstKey() {
        MapPair<K, V>* val = (MapPair<K, V>*)_item;
        return val[0].first;
    }
    V&  firstVal() {
        MapPair<K, V>* val = (MapPair<K, V>*)_item;
        return val[0].second;
    }
private:
    char  _item[MAX_LIST_MAP_NUM * MapPair<K, V>::SIZE];
    short _map[MAX_LIST_MAP_NUM];
    short _size;
};
#endif

