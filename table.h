#ifndef _SINGLE_TABLE_H_
#define _SINGLE_TABLE_H_
#include "common.h"
#include "item.h"
#include <set>
#include <vector>
#include "item_array.h"
#include "atomichashmap.h"
#include "atomiccache.h"
#include "atomicqueue.h"
#include "atomicsortmap.h"
#include "table_index.h"

class SingleTable;

typedef bool (itemchanged_callback)(SingleTable*, TinyTableItem*, int col, void*);
typedef bool (itemdeleted_callback)(SingleTable*, TinyTableItem*, void*);
typedef bool (iteminsert_callback)(SingleTable*, TinyTableItem*, void*);
typedef bool (itemdelete_callback)(SingleTable*, TinyTableItem*, void*);

enum {
    RET_TABLE_OK,
    RET_TABLE_ITEM_NULL,
    RET_TABLE_INDEX_NULL,
    RET_TABLE_INDEX_TYPE_ERROR,
    RET_TABLE_COL_PRIMARY,
};

class SingleTable {
public:
    // iterator is not allowed to change the value
    SingleTable();
    ~SingleTable();
    struct const_iterator;
    struct real_iterator;
    struct dump_iterator;

    struct TableItem {
        TableItem()
        : item(NULL)
        , table(NULL) {}
        TableItem(TinyTableItem* item_, SingleTable* table_)
        : item(item_)
        , table(table_) {}
        TableItem(const TableItem& it)
        : item(it.item)
        , table(it.table) {}
        template<typename T>
        T get(int col) const {
            if (item == NULL){
                return T();
            }
            return item->get<T>(table->getConfig(), col);
        }
        bool isDirty() const {
            if (item == NULL) {return false;}
            return item->dirty();
        }
        time_t getLastActiveTime() const {
            if (item == NULL) {
                return 0;
            }
            return item->getLastActiveTime();
        }
        bool isDeleted(bool trigger = false) const {
            // trigger
            if (trigger) {
                table->trigger(item, false);
            }
            return item->isDeleted();
        }
        TinyTableItem* getItem() {return item;}
        private:
        TinyTableItem* item;
        SingleTable* table;
        friend class const_iterator;
        friend class real_iterator;
        friend class dump_iterator;
        friend class SingleTable;
    };
    struct const_iterator {
        AtomicHashmap<uint64_t, TinyTableItem*>::iterator it;
        TableConfig* config;
        SingleTable*  table;
        TableItem item;
        const_iterator(AtomicHashmap<uint64_t, TinyTableItem*>::iterator it_, TableConfig* config_, SingleTable* table_)
        : it(it_)
        , config(config_)
        , table(table_)
        , item(NULL, table_) {
            if (it_ != AtomicHashmap<uint64_t, TinyTableItem*>::iterator(NULL)) {
                item.item = it_->second;
            }
        }
        const_iterator()
        : config(NULL) {}
        const_iterator(const const_iterator& iter)
        : it(iter.it)
        , config(iter.config)
        , item(iter.item) {
        }
        TableItem& operator*() {
            return item;
        }
        TableItem* operator->() {
            return &item;
        }
        const_iterator& operator++() { 
            ++it;
            if (it != AtomicHashmap<uint64_t, TinyTableItem*>::iterator(NULL)) {
                item.item = it->second;
            } else {
                item.item = NULL;
            }
            return *this;
        }
        const_iterator& tnext() { 
            ++it;
            AtomicHashmap<uint64_t, TinyTableItem*>::iterator end(NULL);
            while (it != end && item.isDeleted(true)) {
                ++it;
            } 
            if (it != end) {
                item.item = it->second;  
            } else {
                item.item = NULL;
            }
            return *this;
        }
        const_iterator operator++(int) { 
            const_iterator iter(it, config, table);
            ++it;
            if (it != AtomicHashmap<uint64_t, TinyTableItem*>::iterator(NULL)) {
                item.item = it->second;
            } else {
                item.item = NULL;
            }
            return iter;           
        }
        bool operator==(const const_iterator& iter) {
            return (it == iter.it);
        }
        bool operator!=(const const_iterator& iter) {
            return (it != iter.it);
        }
        bool isDeleted(bool trigger = false) const {
            // trigger
            if (trigger) {
                table->trigger(item.item, false);
            }
            return item.item->isDeleted();
        }
        void trigger() const {
            table->trigger(item.item, false);
        }
    };

    struct real_iterator {
        AtomicSortmap<TinyTableItem*>::real_iterator it;
        TableConfig* config;
        SingleTable* table;
        TableItem item;
        private:
        AtomicSortmap<TinyTableItem*>* _map;
        public:
        real_iterator(AtomicSortmap<TinyTableItem*>::real_iterator it_, AtomicSortmap<TinyTableItem*>* map_, TableConfig* config_, SingleTable* table_)
        : it(it_)
        , config(config_)
        , table(table_)
        , item(NULL, table_)
        , _map(map_) {
            if (map_ != NULL && it_ != map_->real_end()) {
                item.item = *it_;
            }
        }
        real_iterator()
        : config(NULL)
        , _map(NULL) {}
        real_iterator(const real_iterator& iter)
        : it(iter.it)
        , config(iter.config)
        , item(iter.item)
        , _map(iter._map) {}
        TableItem& operator*() {
            return item;
        }
        TableItem* operator->() {
            return &item;
        }
        real_iterator& operator++() { 
            ++it;
            if (it != _map->real_end()) {
                item.item = *it;
            } else {
                item.item = NULL;
            }
            return *this;
        }
        real_iterator operator++(int) { 
            real_iterator iter(it, _map, config, table);
            ++it;
            if (it != _map->real_end()) {
                item.item = *it;
            } else {
                item.item = NULL;
            }
            return iter;           
        }
        bool operator==(const real_iterator& iter) {
            return (it == iter.it);
        }
        bool operator!=(const real_iterator& iter) {
            return (it != iter.it);
        }
        bool isDeleted(bool trigger = false) const {
            // trigger
            if (trigger) {
                table->trigger(item.item, false);
            }
            return item.item->isDeleted();
        }
        void trigger() const {
            table->trigger(item.item, false);
        }
    };


    struct dump_iterator {
        AtomicSortmap<TinyTableItem*>::dump_iterator it;
        TableConfig* config;
        SingleTable* table;
        TableItem item;
        private:
        AtomicSortmap<TinyTableItem*>* _map;
        int _idx;
        public:
        dump_iterator(AtomicSortmap<TinyTableItem*>::dump_iterator it_, int idx, AtomicSortmap<TinyTableItem*>* map_, TableConfig* config_, SingleTable* table_)
        : it(it_)
        , config(config_)
        , table(table_)
        , item(NULL, table)
        , _map(map_)
        , _idx(idx) {
            if (map_ != NULL && it_ != map_->dump_end(idx)) {
                item.item = *it_;
            }
        }
        dump_iterator()
        : config(NULL)
        , _map(NULL)
        , _idx(0) {}
        dump_iterator(const dump_iterator& iter)
        : it(iter.it)
        , config(iter.config)
        , item(iter.item)
        , _map(iter._map)
        , _idx(iter._idx) {}
        TableItem& operator*() {
            return item;
        }
        TableItem* operator->() {
            return &item;
        }
        dump_iterator& operator++() { 
            ++it;
            if (it != _map->dump_end(_idx)) {
                item.item = *it;
            } else {
                item.item = NULL;
            }
            return *this;
        }
        dump_iterator operator++(int) { 
            dump_iterator iter(it, _idx, _map, config, table);
            ++it;
            if (it != _map->dump_end(_idx)) {
                item.item = *it;
            } else {
                item.item = NULL;
            }
            return iter;           
        }
        bool operator==(const dump_iterator& iter) {
            return (it == iter.it && _idx == iter._idx);
        }
        bool operator!=(const dump_iterator& iter) {
            return (it != iter.it || _idx != iter._idx);
        }
        bool isDeleted(bool trigger = false) const {
            // trigger
            if (trigger) {
                table->trigger(item.item, false);
            }
            return item.item->isDeleted();
        }
        void trigger() const {
            table->trigger(item.item, false);
        }
    };

    const_iterator begin();
    const_iterator tbegin();
    const_iterator end();
    // this is only used for cluster
    const_iterator begin(RowItemValue input);
    const_iterator tbegin(RowItemValue input);
    const_iterator begin(RowItemValue* input);
    const_iterator tbegin(RowItemValue* input);
    const_iterator end(RowItemValue input);
    const_iterator end(RowItemValue* input);
    // this is only used for sort
    real_iterator real_begin(TableIndex index); 
    real_iterator real_begin(TableIndex* index); 
    real_iterator real_end(TableIndex index); 
    real_iterator real_end(TableIndex* index); 
    real_iterator real_find(TableIndex index, int rank);
    real_iterator real_find(TableIndex* index, int rank);
    dump_iterator dump_begin(TableIndex index, int idx); 
    dump_iterator dump_begin(TableIndex* index, int idx); 
    dump_iterator dump_end(TableIndex index, int idx); 
    dump_iterator dump_end(TableIndex* index, int idx); 
    int           getDumpIdx(TableIndex index);
    int           getDumpIdx(TableIndex* index);
    // for queue
    real_iterator queue_begin();
    real_iterator queue_end();
    real_iterator queue_begin(RowItemValue input);
    real_iterator queue_begin(RowItemValue* input);
    real_iterator queue_end(RowItemValue input);
    real_iterator queue_end(RowItemValue* input);

    const_iterator find(RowItemValue value);
    const_iterator find(RowItemValue* value);
    const_iterator find(RowItemValue value1, RowItemValue value2);
    const_iterator find(RowItemValue* value1, RowItemValue* value2);
public:
    int   size();
    int size(RowItemValue value);
    int size(RowItemValue* value);
    char getType(RowItemValue value);
    char getType(RowItemValue* value);
public:
    // the insert func
    bool insert(RowItemValue2 value, bool load = false, TinyTableItem** item = NULL);
    bool insertItem(TinyTableItem* item, bool load = false);
    bool insert(RowItemValue2* value, bool load = false, TinyTableItem** item = NULL);
    bool insert(TinyTableItem** item, ...);
    TinyTableItem* getItem(RowItemValue* key1);
    TinyTableItem* getItem(RowItemValue* key1, RowItemValue* key2);
    bool del(TinyTableItem* item, bool expire = false, bool trig = true, bool only_index = false);
public:
    bool del(RowItemValue input);  
    bool del(RowItemValue* input);  
    bool del(RowItemValue input1, RowItemValue input2);   
    bool del(RowItemValue* input1, RowItemValue* input2);   
    // the first version we only privide single search interface
    // if you want to get multicol items use const_iterator
    // afterwards multicol items get interface will be added
    ItemValue get(RowItemValue input, int col);
    ItemValue get(RowItemValue* input, int col);
    ItemValue get(RowItemValue input1, RowItemValue input2, int col);
    ItemValue get(RowItemValue* input1, RowItemValue* input2, int col);

    int set(RowItemValue input, RowItemValue val);
    int set(RowItemValue input1, RowItemValue input2, RowItemValue val);

    int set(RowItemValue input1, RowItemValue input2, int col, ItemValue val, ItemValue* old = NULL);
    int set(RowItemValue input, int col, ItemValue val, ItemValue* old = NULL);
    int set(TinyTableItem* item, int col, ItemValue* val, ItemValue* old = NULL, bool trig = true);
    int notrigger_set(TinyTableItem* item, int col, ItemValue* val, ItemValue* old = NULL, bool trig = true);
public:
    int set(RowItemValue* input, RowItemValue* val);
    int set(RowItemValue* input, RowItemValue2* val);
    int set(RowItemValue* input1, RowItemValue* input2, RowItemValue* val);
    int set(RowItemValue* input1, RowItemValue* input2, int col, ItemValue* val, ItemValue* old = NULL);
    int set(RowItemValue* input, int col, ItemValue* val, ItemValue* old = NULL);
private:
    int setNolock(TinyTableItem* item, int col, ItemValue* val, ItemValue* old = NULL);
    int addNolock(TinyTableItem* item, int col, ItemValue* val, ItemValue* old = NULL);
    int decNolock(TinyTableItem* item, int col, ItemValue* val, ItemValue* old = NULL);
public:
    int add(RowItemValue input1, RowItemValue input2, int col, ItemValue val, ItemValue* cur = NULL);
    int add(RowItemValue input, int col, ItemValue val, ItemValue* cur = NULL);

    int dec(RowItemValue input1, RowItemValue input2, int col, ItemValue val, ItemValue* cur = NULL);
    int dec(RowItemValue input, int col, ItemValue val, ItemValue* cur = NULL);

    int add(RowItemValue* input1, RowItemValue* input2, int col, ItemValue* val, ItemValue* cur = NULL);
    int add(RowItemValue* input, int col, ItemValue* val, ItemValue* cur = NULL);
    int add(TinyTableItem* item, int col, ItemValue* val, ItemValue* cur = NULL);

    int dec(RowItemValue* input1, RowItemValue* input2, int col, ItemValue* val, ItemValue* cur = NULL);
    int dec(RowItemValue* input, int col, ItemValue* val, ItemValue* cur = NULL);
    int dec(TinyTableItem* item, int col, ItemValue* val, ItemValue* cur = NULL);

public:
    bool init(TableConfig* config, bool enable_trigger = true);
    bool dump_sort(TableIndex index);
    bool dump_sort(TableIndex* index);
    int  rank(RowItemValue unique, TableIndex sort);
    int  rank(RowItemValue* unique, TableIndex* sort);
    int  expire();
    void clear();
    void clear(RowItemValue* input);
    void clear(RowItemValue input);
public:
    void setChangedCallbackFunc(itemchanged_callback* func, void* arg = NULL) {_changed_callback_func = func; _changed_callback_arg = arg;}
    void setDeletedCallbackFunc(itemdeleted_callback* func, void* arg = NULL) {_deleted_callback_func = func; _deleted_callback_arg = arg;}
    void setDeleteCallbackFunc(itemdelete_callback* func, void* arg = NULL) {_delete_callback_func = func; _delete_callback_arg = arg;}
    void setInsertCallbackFunc(iteminsert_callback* func, void* arg = NULL) {_insert_callback_func = func; _insert_callback_arg = arg;}
public:
    // notice
    // these funcs are only safe in singlethread
    // so we should do this when create a table
    // while the table is running index are not allowed to be changed
    bool createUnique(bool primary, TableIndex unique, bool config = false);
    bool createCluster(TableIndex unique, TableIndex cluster, bool config = false);
    bool createQueue(TableIndex unique, TableIndex cluster, bool desc = false, int max_size = -1, bool config = false);
    bool createSort(bool desc, TableIndex unique, TableIndex sort, bool config = false);
    TinyTableItem* newItem();
    void freeItem(TinyTableItem* item);
    int  getExpiredTime() const {return _expired_time;}
public:
    // do the trigger ops
    bool trigger(int trigger_id, bool realtime);
    const TableConfig* getConfig() const {return m_config;}
    int   getTableID() const { return m_config->table_id;}
    void setTriggers(bool triggers) {_enable_trigger = triggers;}
    inline AtomicHashmap<uint64_t, int>* getDirtyItems() {return _dirty_items;}
public:
    int  parse_res_data(char* buffer, int len, bool load = true, std::vector<TinyTableItem*>* vec = NULL);
    int  parse_req_data(char* buffer, int len, int& max_num, RowItemValue2& value1, RowItemValue2& value2, bool mod = false);
private:
    ItemValue set(TinyTableItem* item, int col, ItemValue& val);
    ItemValue add(TinyTableItem* item, int col, ItemValue& val);
    ItemValue dec(TinyTableItem* item, int col, ItemValue& val);
    bool trigger(int trigger_id, TinyTableItem* item, bool realtime, bool* isdel = NULL, time_t now = time(NULL), bool load = false);
    bool trigger(TinyTableItem* item, bool realtime, bool* isdel = NULL, time_t now = time(NULL), bool load = false);
private:
    struct ColumnRelatedIndex {
        // index can not be changed while the table is running
        // so vector will be safe here as it is only used for read
        std::vector<TableBaseIndex*> index;
    };
    uint64_t _primary_sign;
    int            _expired_time;
    bool           _enable_trigger;
private:
    // cache is for reuse mem && mutithread safe
    // inside we use delay delete method
    AtomicCache<TinyTableItem*>*  _cache;
private:
    ColumnRelatedIndex* _col_related_index;
    AtomicHashmap<uint64_t, int>* _dirty_items;
private:
    TableBaseIndex* _primary_index;
    AtomicHashmap<uint64_t, TableBaseIndex*>* _index;
    TableQueueIndex* _unique_queue_index;
    AtomicHashmap<uint64_t, TableClusterQueueIndex*>* _cluster_queue_index;
    TableConfig* m_config;
private:
   itemchanged_callback*  _changed_callback_func;
   itemdeleted_callback*  _deleted_callback_func;
   iteminsert_callback*  _insert_callback_func;
   itemdelete_callback*  _delete_callback_func;
   void*    _changed_callback_arg;
   void*    _deleted_callback_arg;
   void*    _insert_callback_arg;
   void*    _delete_callback_arg;
};
#endif

