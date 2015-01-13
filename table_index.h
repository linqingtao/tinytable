#ifndef _TABLE_INDEX_H_
#define _TABLE_INDEX_H_
#include "common.h"
#include "item.h"
#include <set>
#include <vector>
#include "item_array.h"
#include "atomichashmap.h"
#include "atomiccache.h"
#include "atomicqueue.h"
#include "atomicsortmap.h"
#include "list_set.h"

class SingleTable;

extern uint64_t set_hash(const std::set<short>& val);
extern uint64_t set_hash(const ListSet<short>& val);

typedef TableItem_t TinyTableItem;
enum {
    INDEX_NULL,
    INDEX_PRIMARY,
    INDEX_UNIQUE,
    INDEX_CLUSTER,
    INDEX_SORT,
    INDEX_CLUSTER_SORT,
    INDEX_QUEUE,
    INDEX_CLUSTER_QUEUE,
};


class TableBaseIndex {
public:
    // iterator is not allowed to change the value
    typedef AtomicHashmap<uint64_t, TinyTableItem*>::iterator iterator;
    typedef AtomicSortmap<TinyTableItem*>::real_iterator real_iterator;
    typedef AtomicSortmap<TinyTableItem*>::dump_iterator dump_iterator;
    TableBaseIndex(TableConfig* config, ListSet<short>& cols);
    TableBaseIndex(TableConfig* config);
    virtual ~TableBaseIndex();
public:
    inline char getType() const {return _type;}
    bool isIndex(int col) const;
    inline const TableConfig*  getConfig() const {return m_config;}
    uint64_t getColSign() const ;
public:
    virtual bool mod(TinyTableItem* item, int col, ItemValue& new_val) = 0;
    virtual bool del(TinyTableItem* item) = 0;
    virtual bool remove(TinyTableItem* item, int col, ItemValue& new_val) = 0;
    virtual bool add(TinyTableItem* item) = 0;
    virtual void dump();
    virtual iterator begin();
    virtual iterator cluster_begin(RowItemValue* val);
    virtual iterator cluster_end(RowItemValue* val);
    virtual iterator end();
    virtual iterator find1(RowItemValue& key);
    virtual iterator find2(RowItemValue& key1, RowItemValue& key2);
    virtual real_iterator real_begin();
    virtual real_iterator cluster_real_begin(RowItemValue* val);
    virtual real_iterator cluster_real_end(RowItemValue* val);
    virtual dump_iterator dump_begin(int& idx);
    virtual real_iterator real_end();
    virtual dump_iterator dump_end(int& idx);
    virtual int   size();
    virtual void   clear();
    virtual int   size1(RowItemValue& value);
public:
    virtual TinyTableItem* getItem1(RowItemValue& key);
    virtual TinyTableItem* getItem2(RowItemValue& key1, RowItemValue& key2);
public:
    virtual ItemValue get1(RowItemValue& key, int col);
    virtual int rank(TinyTableItem* item);
    virtual ItemValue get2(RowItemValue& input1, RowItemValue& input2, int col);
    // get the item sign according to _cols
    uint64_t sign(TinyTableItem* item);
    uint64_t sign(TinyTableItem* item,int col, ItemValue& new_val);
protected:
    char _type;
    ListSet<short> _cols;
    TableConfig*  m_config;
};

class TableQueueIndex : public TableBaseIndex {
public:
    // iterator is not allowed to change the value
    TableQueueIndex(TableConfig* config, short cnt_col, bool desc = false, int max_size = -1);
    virtual ~TableQueueIndex();
public:
    virtual bool mod(TinyTableItem* item, int col, ItemValue& new_val);
    virtual bool del(TinyTableItem* item);
    virtual bool remove(TinyTableItem* item, int col, ItemValue& val);
    virtual bool add(TinyTableItem* item);
    virtual real_iterator real_begin();
    virtual real_iterator real_end();
public:
    virtual int   size();
    virtual void   clear();
    inline int     maxSize() {return _max_size;}
    int     insert() { return __sync_add_and_fetch(&_cur_begin, 1);}
    int     decHead() { return __sync_sub_and_fetch(&_cur_begin, 1);}
    int     addTail() { return __sync_add_and_fetch(&_cur_end, 1);}
    inline int     head() {return _cur_begin;}
    inline int     tail() {return _cur_end;}
    inline void   setHead(int head) {_cur_begin = head;}
    inline void   setTail(int tail) {_cur_end = tail;}
    short  _cnt_col;
private:
    // use skiplist not queue to store item
    // as skiplist iterator is multithread safe
    AtomicSortmap<TinyTableItem*>* _queue_items;
    friend class SingleTable;
    int    _max_size;
    int    _cur_begin;
    int    _cur_end;
    bool   _desc;
    TinyTableItem* max_item;
    TinyTableItem* min_item;
};

class TableClusterQueueIndex : public TableBaseIndex {
 public:
    // iterator is not allowed to change the value
    TableClusterQueueIndex(TableConfig* config, ListSet<short>& cluster, short cnt_col, bool is_cluster, bool desc = false, int max_size = -1);
    virtual ~TableClusterQueueIndex();
public:
    virtual bool mod(TinyTableItem* item, int col, ItemValue& new_val);
    virtual bool del(TinyTableItem* item);
    virtual bool remove(TinyTableItem* item, int col, ItemValue& val);
    virtual bool add(TinyTableItem* item);
    virtual real_iterator cluster_real_begin(RowItemValue* val);
    virtual real_iterator cluster_real_end(RowItemValue* val);
    TableQueueIndex* getIndex(RowItemValue* val);
    TableQueueIndex* getIndex(uint64_t val);
public:
    virtual int   size1(RowItemValue& value);
    virtual int   size();
    virtual void   clear();
    inline int    maxSize() {return _max_size;}
    inline bool   isCluster() { return _is_cluster;}
    int    head(RowItemValue& val);
    int    tail(RowItemValue& val);
    short  _cnt_col;
private:
    // use hashmap not queue to store item
    // as hashmap iterator is multithread safe
    AtomicHashmap<uint64_t, TableQueueIndex*>* _cluster_queue_items;
    int    _max_size;
    bool   _desc;
    bool   _is_cluster;
    friend class SingleTable;
};

class TableUniqueIndex : public TableBaseIndex {
public:
    // iterator is not allowed to change the value
    TableUniqueIndex(bool primary, TableConfig* config, ListSet<short>& cols);
    virtual ~TableUniqueIndex();
public:
    virtual TinyTableItem* getItem1(RowItemValue& key);
    virtual bool mod(TinyTableItem* item, int col, ItemValue& new_val);
    virtual bool del(TinyTableItem* item);
    virtual bool remove(TinyTableItem* item, int col, ItemValue& val);
    virtual bool add(TinyTableItem* item);
    virtual iterator begin();
    virtual iterator end() ;
    virtual iterator find1(RowItemValue& key);
public:
    virtual ItemValue get1(RowItemValue& key, int col);
    virtual int   size();
    virtual void   clear();
private:
    AtomicHashmap<uint64_t, TinyTableItem*>* _unique_items;
    friend class SingleTable;
};


class TableClusterIndex : public TableBaseIndex {
public:
    // iterator is not allowed to change the value
    TableClusterIndex(TableConfig* config, ListSet<short>& unique, ListSet<short>& cluster);
    virtual ~TableClusterIndex();
public:
    virtual bool mod(TinyTableItem* item, int col, ItemValue& new_val);
    virtual bool del(TinyTableItem* item);
    virtual bool remove(TinyTableItem* item, int col, ItemValue& val);
    virtual bool add(TinyTableItem* item);
public:
    virtual TinyTableItem* getItem2(RowItemValue& key1, RowItemValue& key2);
    virtual iterator cluster_begin(RowItemValue* val);
    virtual iterator cluster_end(RowItemValue* val);
public:
    virtual ItemValue get2(RowItemValue& input1, RowItemValue& input2, int col);
    virtual iterator find2(RowItemValue& key1, RowItemValue& key2);
    virtual int   size1(RowItemValue& value);
    virtual int   size();
    virtual void   clear();
private:
    AtomicHashmap<uint64_t, TableUniqueIndex*>* _cluster_items;
    ListSet<short> _cluster;
    friend class SingleTable;
};


class TableSortIndex : public TableBaseIndex {
public:
    // iterator is not allowed to change the value
    TableSortIndex(bool desc, TableConfig* config, ListSet<short>& unique, ListSet<short>& sort);
    virtual ~TableSortIndex();
public:
    virtual bool mod(TinyTableItem* item, int col, ItemValue& old);
    virtual bool del(TinyTableItem* item);
    virtual bool remove(TinyTableItem* item, int col, ItemValue& val);
    virtual bool add(TinyTableItem* item);
public:
    virtual TableBaseIndex::real_iterator real_begin();
    virtual TableBaseIndex::dump_iterator dump_begin(int& idx);
    virtual TableBaseIndex::real_iterator real_end();
    virtual TableBaseIndex::dump_iterator dump_end(int& idx);
public:
    virtual int rank(TinyTableItem* item);
    virtual int   size();
    virtual void   clear();
public:
    ListSet<short>  unique_keys;
    ListSet<short>  sort_keys;
private:
    AtomicSortmap<TinyTableItem*>* _sort_items;
    TinyTableItem* max_item;
    TinyTableItem* min_item;
    friend class SingleTable;
};



#endif

