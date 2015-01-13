#include "table_index.h"
#include "table.h"
#include <sstream>

uint64_t set_hash(const std::set<short>& val) {
    // if the val is single && integer then just return the value
    if (val.size() == 1) {
        return *(val.begin());
    }
    std::set<short>::iterator iter = val.begin();
    std::set<short>::iterator end = val.end();
    std::stringstream buffer;
    for (; iter != end; ++iter) {
        buffer << *iter;
        buffer << ":";
    }
    return get_sign64(buffer.str().c_str(),  buffer.str().length() - 1);
}

uint64_t set_hash(const ListSet<short>& val) {
    // if the val is single && integer then just return the value
    if (val.size() == 1) {
        return val.first();
    }
    ListSet<short>::iterator iter = val.begin();
    ListSet<short>::iterator end = val.end();
    std::stringstream buffer;
    for (; iter != end; ++iter) {
        buffer << *iter;
        buffer << ":";
    }
    return get_sign64(buffer.str().c_str(),  buffer.str().length() - 1);
}

// this is used for sortmap
int CompareTableItem(const TinyTableItem*& item1, const TinyTableItem*& item2, void* arg) {
    TableSortIndex* index = (TableSortIndex*)arg;
    if (index == NULL) {
        TT_FATAL_LOG("compare func is used not in a sort class!!! check your code");
        return 0;
    }
    const TableConfig* config = index->getConfig();
    ListSet<short>::iterator iter = index->sort_keys.begin();
    ListSet<short>::iterator end = index->sort_keys.end();
    for (; iter != end; ++iter) {
        const ItemValue v1 = item1->getItem(config, *iter);
        const ItemValue v2 = item2->getItem(config, *iter);
        if (v1 > v2) {
            return 1;
        } else if (v1 < v2) {
            return -1;
        }
    }
    ListSet<short>::iterator it = index->unique_keys.begin();
    ListSet<short>::iterator it_end = index->unique_keys.end();
    for (; it != it_end; ++it) {
        const ItemValue v1 = item1->getItem(config, *it);
        const ItemValue v2 = item2->getItem(config, *it);
        if (v1 > v2) {
            return 1;
        } else if (v1 < v2) {
            return -1;
        }
    }
    if (item1 > item1) {
        return 1;
    } else if (item1 < item2) {
        return -1;
    }
    return 0;
}

// this is used for queue
int CompareQueueTableItem(const TinyTableItem*& item1, const TinyTableItem*& item2, void* arg) {
    TableQueueIndex* index = (TableQueueIndex*)arg;
    if (index == NULL) {
        TT_FATAL_LOG("compare func is used not in a queue class!!! check your code");
        return 0;
    }
    const TableConfig* config = index->getConfig();
    long long key1 = item1->get<long long>(config, index->_cnt_col);
    long long key2 = item2->get<long long>(config, index->_cnt_col);
    if (key1 > key2) {
        return 1;
    } else if (key1 < key2) {
        return -1;
    }
    return 0;
}


TableBaseIndex::TableBaseIndex(TableConfig* config, ListSet<short>& cols)
: _type(INDEX_NULL)
, _cols(cols)
, m_config(config) {}

TableBaseIndex::TableBaseIndex(TableConfig* config)
: _type(INDEX_NULL)
, m_config(config) {}


TableBaseIndex::~TableBaseIndex() {
}

uint64_t TableBaseIndex::getColSign() const {
    return set_hash(_cols);
}

TableBaseIndex::real_iterator TableBaseIndex::cluster_real_begin(RowItemValue* val) {
    TT_FATAL_LOG("This func can be used in this type %d class! check your code", (int)getType());
    return real_iterator();
}

TableBaseIndex::real_iterator TableBaseIndex::cluster_real_end(RowItemValue* val) {
    TT_FATAL_LOG("This func can be used in this type %d class! check your code", (int)getType());
    return real_iterator();
}

int   TableBaseIndex::size() {
    TT_FATAL_LOG("This func can be used in this type %d class! check your code", (int)getType());
    return -1;
}

void   TableBaseIndex::clear() {
    TT_FATAL_LOG("This func can be used in this type %d class! check your code", (int)getType());
}

int   TableBaseIndex::size1(RowItemValue& value) {
    TT_FATAL_LOG("This func can be used in this type %d class! check your code", (int)getType());
    return -1;
}

void TableBaseIndex::dump() {
    TT_WARN_LOG("this func can not be use in unsort class! please check your code!");
}

TableBaseIndex::iterator TableBaseIndex::begin() {
    TT_FATAL_LOG("This func can be used in this type %d class! check your code", (int)getType());
    return NULL;
}

TableBaseIndex::iterator TableBaseIndex::cluster_begin(RowItemValue* val) {
    TT_FATAL_LOG("This func can be used in this type %d class! check your code", (int)getType());
    return NULL;
}

TableBaseIndex::iterator TableBaseIndex::cluster_end(RowItemValue* val) {
    TT_FATAL_LOG("This func can be used in this type %d class! check your code", (int)getType());
    return NULL;
}

TableBaseIndex::iterator TableBaseIndex::end() {
    TT_FATAL_LOG("This func can be used in this type %d class! check your code", (int)getType());
    return NULL;
}

TableBaseIndex::real_iterator TableBaseIndex::real_begin() {
    TT_FATAL_LOG("This func can be used in this type %d class! check your code", (int)getType());
    return TableBaseIndex::real_iterator();
}

TableBaseIndex::dump_iterator TableBaseIndex::dump_begin(int& idx) {
    TT_FATAL_LOG("This func can be used in this type %d class! check your code", (int)getType());
    return TableBaseIndex::dump_iterator(NULL);
}

TableBaseIndex::real_iterator TableBaseIndex::real_end() {
    TT_FATAL_LOG("This func can be used in this type %d class! check your code", (int)getType());
    return TableBaseIndex::real_iterator();
}

TableBaseIndex::dump_iterator TableBaseIndex::dump_end(int& idx) {
    TT_FATAL_LOG("This func can be used in this type %d class! check your code", (int)getType());
    return TableBaseIndex::dump_iterator(NULL);
}

ItemValue TableBaseIndex::get2(RowItemValue& input1, RowItemValue& input2, int col) {
    TT_FATAL_LOG("This func can not be used in type %d class! check you code!", (int)getType());
    return ItemValue();
}

ItemValue TableBaseIndex::get1(RowItemValue& input1, int col) {
    TT_FATAL_LOG("This func can not be used in type %d class! check you code!", (int)getType());
    return ItemValue();
}

TinyTableItem* TableBaseIndex::getItem1(RowItemValue& key) {
    TT_FATAL_LOG("This func can not be used in type %d class! check you code!", (int)getType());
    return NULL;
}

TinyTableItem* TableBaseIndex::getItem2(RowItemValue& key1, RowItemValue& key2) {
    TT_FATAL_LOG("This func can not be used in type %d class! check you code!", (int)getType());
    return NULL;
}

int TableBaseIndex::rank(TinyTableItem* item) {
    TT_FATAL_LOG("This func can not be used in type %d class at this version! check you code!", (int)getType());
    return -1;
}

TableBaseIndex::iterator TableBaseIndex::find1(RowItemValue& key) {
    TT_FATAL_LOG("This func can not be used in type %d class! check you code!", (int)getType());
    return TableBaseIndex::iterator();
}

TableBaseIndex::iterator TableBaseIndex::find2(RowItemValue& key1, RowItemValue& key2) {
    TT_FATAL_LOG("This func can not be used in type %d class! check you code!", (int)getType());
    return TableBaseIndex::iterator();
}

// get the cal sign of item according to the _cols
uint64_t TableBaseIndex::sign(TinyTableItem* item) {
    if (_cols.size() == 1 && (m_config->types[_cols.first()] < OBJECT_NUMBER)) {
        return item->getItem(m_config, _cols.first()).get<uint64_t>();
    }
    std::stringstream buffer;
    ListSet<short>::iterator iter = _cols.begin();
    ListSet<short>::iterator end = _cols.end();
    for (; iter != end; ++iter)  {
        short col = *iter;
        switch (m_config->types[col])
        {
            case OBJECT_INT32:buffer << item->get<int32_t>(m_config, col);break; 
            case OBJECT_STRING:buffer << item->get<std::string>(m_config, col);break; 
            case OBJECT_INT64:buffer << item->get<int64_t>(m_config, col);break; 
            case OBJECT_UINT64:buffer << item->get<uint64_t>(m_config, col);break; 
            case OBJECT_UINT32:buffer << item->get<uint32_t>(m_config, col);break; 
            case OBJECT_BOOL: buffer << item->get<bool>(m_config, col);break;
            case OBJECT_INT8:buffer << item->get<int8_t>(m_config, col);break; 
            case OBJECT_UINT8:buffer << item->get<uint8_t>(m_config, col);break; 
            case OBJECT_INT16:buffer << item->get<int16_t>(m_config, col);break; 
            case OBJECT_UINT16:buffer << item->get<uint16_t>(m_config, col);break; 
            case OBJECT_FLOAT:buffer << item->get<float>(m_config, col);break; 
            case OBJECT_DOUBLE:buffer << item->get<double>(m_config, col);break; 
        }
        buffer << ":";
    }
    // here we ignore the last ":"
    return get_sign64(buffer.str().c_str(), buffer.str().length() - 1);
}

// get the sign of new
uint64_t TableBaseIndex::sign(TinyTableItem* item, int col, ItemValue& new_val) {
    if (_cols.size() == 1 ) {
        if (*_cols.begin() == col && (m_config->types[col] < OBJECT_NUMBER)) {
            return new_val.get<uint64_t>();
        }
        if (m_config->types[col] < OBJECT_NUMBER) {
            return item->get<uint64_t>(m_config, *_cols.begin());
        }
    } 
    std::stringstream buffer;
    ListSet<short>::iterator iter = _cols.begin();
    ListSet<short>::iterator end = _cols.end();
    for (; iter != end; ++iter)  {
        short col_id = *iter;
        if (col_id == col) {
            switch (m_config->types[col_id])
            {
                case OBJECT_INT32:buffer << new_val.get<int32_t>();break; 
                case OBJECT_STRING:buffer << new_val.get<std::string>();break; 
                case OBJECT_INT64:buffer << new_val.get<int64_t>();break; 
                case OBJECT_UINT64:buffer << new_val.get<uint64_t>();break; 
                case OBJECT_UINT32:buffer << new_val.get<uint32_t>();break; 
                case OBJECT_BOOL: buffer << new_val.get<bool>();break;
                case OBJECT_INT8:buffer << new_val.get<int8_t>();break; 
                case OBJECT_UINT8:buffer << new_val.get<uint8_t>();break; 
                case OBJECT_INT16:buffer << new_val.get<int16_t>();break; 
                case OBJECT_UINT16:buffer << new_val.get<uint16_t>();break; 
                case OBJECT_FLOAT:buffer << new_val.get<float>();break; 
                case OBJECT_DOUBLE:buffer << new_val.get<double>();break; 
            }
        } else {
            switch (m_config->types[col_id])
            {
                case OBJECT_INT32:buffer << item->get<int32_t>(m_config, col_id);break; 
                case OBJECT_STRING:buffer << item->get<std::string>(m_config, col_id);break; 
                case OBJECT_INT64:buffer << item->get<int64_t>(m_config, col_id);break; 
                case OBJECT_UINT64:buffer << item->get<uint64_t>(m_config, col_id);break; 
                case OBJECT_UINT32:buffer << item->get<uint32_t>(m_config, col_id);break; 
                case OBJECT_BOOL: buffer << item->get<bool>(m_config, col_id);break;
                case OBJECT_INT8:buffer << item->get<int8_t>(m_config, col_id);break; 
                case OBJECT_UINT8:buffer << item->get<uint8_t>(m_config, col_id);break; 
                case OBJECT_INT16:buffer << item->get<int16_t>(m_config, col_id);break; 
                case OBJECT_UINT16:buffer << item->get<uint16_t>(m_config, col_id);break; 
                case OBJECT_FLOAT:buffer << item->get<float>(m_config, col_id);break; 
                case OBJECT_DOUBLE:buffer << item->get<double>(m_config, col_id);break; 
            }
        }
        buffer << ":";
    }
    return get_sign64(buffer.str().c_str(), buffer.str().length() - 1);
}


bool TableBaseIndex::isIndex(int col) const {
    return (_cols.find(col) != _cols.end());
}


TableQueueIndex::TableQueueIndex(TableConfig* config, short cnt_col, bool desc, int max_size)
: TableBaseIndex(config)
, _cnt_col(cnt_col)
, _queue_items(NULL)
, _max_size(max_size)
, _cur_begin(0)
, _cur_end(0)
, _desc(desc)
, max_item(NULL)
, min_item(NULL) {
    _type = INDEX_QUEUE;
    max_item = new TinyTableItem(config, TinyTableItem::max(config));
    min_item = new TinyTableItem(config, TinyTableItem::min(config));
    if (desc) {
        _queue_items = new AtomicSortmap<TinyTableItem*>(desc, true, (void*)CompareQueueTableItem, (void*)this, &min_item);
    } else {
        _queue_items = new AtomicSortmap<TinyTableItem*>(desc, true, (void*)CompareQueueTableItem, (void*)this, &max_item);
    }

}

TableQueueIndex::~TableQueueIndex() {
    if (_queue_items != NULL) {
        delete _queue_items;
        _queue_items = NULL;
    }
    if (max_item != NULL) {
        delete max_item;
        max_item = NULL;
    }
    if (min_item != NULL) {
        delete min_item;
        min_item = NULL;
    }
}

TableBaseIndex::real_iterator TableQueueIndex::real_begin() {
    return _queue_items->real_begin();
}

TableBaseIndex::real_iterator TableQueueIndex::real_end() {
    return _queue_items->real_end();
}


bool TableQueueIndex::mod(TinyTableItem* item, int col, ItemValue& new_val) {
    // add new index
    // here we first remove the old item
    _queue_items->remove(item);
    item->setItem(m_config, col, &new_val);
    // then add the new item
    if (!_queue_items->insert(item)) {
        TT_WARN_LOG("add item to sortmap error");
        return false;
    }
    return true;
}

bool TableQueueIndex::del(TinyTableItem* item) {
    if (!_queue_items->remove(item)) {
        TT_WARN_LOG("remove item error: no exist");
        return false;
    }
    return true;
}

bool TableQueueIndex::remove(TinyTableItem* item, int col, ItemValue& new_val) {
    TT_WARN_LOG("this func can not be used int sort class at this version! Please check your code");
    return true;
}

bool TableQueueIndex::add(TinyTableItem* item) {
    if (!_queue_items->insert(item)) {
        TT_WARN_LOG("add item to sort error");
        return false;
    }
    return true;
}


int   TableQueueIndex::size() {
    return _queue_items->size();
}

void TableQueueIndex::clear() {
    _queue_items->clear();
}


TableUniqueIndex::TableUniqueIndex(bool primary, TableConfig* config, ListSet<short>& cols)
: TableBaseIndex(config, cols)
, _unique_items(NULL) {
    if (primary) {
        _type = INDEX_PRIMARY;
        _unique_items = new AtomicHashmap<uint64_t, TinyTableItem*>(false);
    } else {
        _type = INDEX_UNIQUE;
        _unique_items = new AtomicHashmap<uint64_t, TinyTableItem*>(false);
    }
}

TableUniqueIndex::~TableUniqueIndex() {
    if (NULL != _unique_items) {
        delete _unique_items;
        _unique_items = NULL;
    }
}


int   TableUniqueIndex::size() {
    return _unique_items->size();
}

void TableUniqueIndex::clear() {
    _unique_items->clear();
}

TableBaseIndex::iterator TableUniqueIndex::begin() {
    return _unique_items->begin();
}

TableUniqueIndex::iterator TableUniqueIndex::end() {
    return _unique_items->end();
}


bool TableUniqueIndex::mod(TinyTableItem* item, int col, ItemValue& new_val) {
    // add new index
    uint64_t new_sign = sign(item, col, new_val);
    if (!_unique_items->insert(new_sign, item)) {
        TT_WARN_LOG("mod item col %d error: new index %llu exist", col, new_sign);
        return false;
    }
    return true;
}

bool TableUniqueIndex::del(TinyTableItem* item) {
    uint64_t item_sign = sign(item);
    if (!_unique_items->Remove(item_sign)) {
        TT_WARN_LOG("remove item error: no exist");
        return false;
    }
    return true;
}

bool TableUniqueIndex::remove(TinyTableItem* item, int col, ItemValue& new_val) {
    uint64_t item_sign = sign(item, col, new_val);
    if (!_unique_items->Remove(item_sign)) {
        TT_WARN_LOG("remove item error: no exist");
        return false;
    }
    return true;
}

bool TableUniqueIndex::add(TinyTableItem* item) {
    uint64_t new_sign = sign(item);
    if (!_unique_items->insert(new_sign, item)) {
        TT_WARN_LOG("add item error");
        return false;
    }
    return true;
}

TableBaseIndex::iterator TableUniqueIndex::find1(RowItemValue& key) {
    uint64_t key_sign = key.val_sign();
    return _unique_items->find(key_sign);
}

TinyTableItem* TableUniqueIndex::getItem1(RowItemValue& key) {
    // check if is the unique
    if (key.size() != _cols.size()) {
        TT_WARN_LOG("not the unique");
        return NULL;
    }
    /*std::set<short>::iterator iter = _cols.begin();   
    std::set<short>::iterator end = _cols.end();   
    for (; iter != end; ++iter) {
        if (!key.has(*iter)) {
            TT_WARN_LOG("key is not right: has no col %d", *iter);
            return NULL;
        }
    }*/
    uint64_t unique_key = key.val_sign();
    AtomicHashmap<uint64_t, TinyTableItem*>::iterator it = _unique_items->find(unique_key);
    if (it == _unique_items->end()) {
        return NULL;
    }
    return it->second;
}


ItemValue TableUniqueIndex::get1(RowItemValue& key, int col) {
    /*if (col >= m_config->column_num) {
        TT_WARN_LOG("col %d is over flow max %d", col, m_config->column_num);
        return ItemValue();
    }*/
    TinyTableItem* item = getItem1(key);
    if (item == NULL) {
        TT_WARN_LOG("col %d no found item", col);
        return ItemValue();
    }
    return item->getItem(m_config, col);
}


TableClusterIndex::TableClusterIndex(TableConfig* config, ListSet<short>& unique, ListSet<short>& cluster)
: TableBaseIndex(config, unique)
, _cluster_items(NULL)
, _cluster(cluster) {
    _type = INDEX_CLUSTER;
    // free TableUniqueIndex
    _cluster_items = new AtomicHashmap<uint64_t, TableUniqueIndex*>(true);
}

TableClusterIndex::~TableClusterIndex() {
    if (NULL != _cluster_items) {
        delete _cluster_items;
        _cluster_items = NULL;
    }
}

TableBaseIndex::iterator TableClusterIndex::cluster_begin(RowItemValue* val) {
    uint64_t key = val->val_sign();
    AtomicHashmap<uint64_t, TableUniqueIndex*>::iterator iter = _cluster_items->find(key);
    if (iter == _cluster_items->end()) {
        return NULL;
    }
    return iter->second->begin();
}

TableBaseIndex::iterator TableClusterIndex::cluster_end(RowItemValue* val) {
    /*uint64_t key = val->val_sign();
    AtomicHashmap<uint64_t, TableUniqueIndex*>::iterator iter = _cluster_items->find(key);
    if (iter == _cluster_items->end()) {
        return NULL;
    }
    return iter->second->end();*/
    // here we just return NULL
    return NULL;
}

bool TableClusterIndex::mod(TinyTableItem* item, int col, ItemValue& new_val) {
    // if the col is unique index
    TableUniqueIndex* new_unique = NULL;
    uint64_t new_sign = sign(item, col, new_val);
    // lock 
    int lock_index = _cluster_items->Lock(new_sign);
    AtomicHashmap<uint64_t, TableUniqueIndex*>::iterator iter = _cluster_items->find(new_sign);
    // recreate index
    if (iter == _cluster_items->end()) {
        // create a new index
        new_unique = new(std::nothrow) TableUniqueIndex(false, m_config, _cluster);
        if (new_unique == NULL) {
            _cluster_items->UnlockIndex(lock_index);
            TT_WARN_LOG("create new unique error");
            return false;
        }
        if (!_cluster_items->insertNolock(new_sign, new_unique, lock_index)) {
            TT_WARN_LOG("insert new sign unique error");
            delete new_unique;
            new_unique = NULL;
            iter = _cluster_items->find(new_sign);
            if (iter == _cluster_items->end()) {
                _cluster_items->UnlockIndex(lock_index);
                TT_WARN_LOG("get new index error");
                return false;
            }
            new_unique = iter->second;
        }
    } else {
        new_unique = iter->second;
    }
    bool ret = new_unique->mod(item, col, new_val);
    _cluster_items->UnlockIndex(lock_index);
    return ret;
}

bool TableClusterIndex::del(TinyTableItem* item) {
    // del all the index of the clusters
    uint64_t del_sign = sign(item);
    int lock_index = _cluster_items->Lock(del_sign);
    AtomicHashmap<uint64_t, TableUniqueIndex*>::iterator iter = _cluster_items->find(del_sign);
    if (iter == _cluster_items->end()) {
        _cluster_items->UnlockIndex(lock_index);
        TT_WARN_LOG("del error: item not exist");
        return false;
    }    
    if (!iter->second->del(item)) {
        _cluster_items->UnlockIndex(lock_index);
        TT_WARN_LOG("del item from cluster error");
        return false;
    }
    // check the cluster if the cluster size is zero then del the cluster
    if (iter->second->size() <= 0) {
        _cluster_items->RemoveNolock(del_sign);
    }
    _cluster_items->UnlockIndex(lock_index);
    return true;
}

bool TableClusterIndex::remove(TinyTableItem* item, int col, ItemValue& new_val) {
    // del all the index of the clusters
    uint64_t del_sign = sign(item, col, new_val);
    int lock_index = _cluster_items->Lock(del_sign);
    AtomicHashmap<uint64_t, TableUniqueIndex*>::iterator iter = _cluster_items->find(del_sign);
    if (iter == _cluster_items->end()) {
        _cluster_items->UnlockIndex(lock_index);
        TT_WARN_LOG("del error: item not exist");
        return false;
    }    
    if (!iter->second->del(item)) {
        _cluster_items->UnlockIndex(lock_index);
        TT_WARN_LOG("del item from cluster error");
        return false;
    }
    // check the cluster if the cluster size is zero then del the cluster
    if (iter->second->size() <= 0) {
        _cluster_items->RemoveNolock(del_sign);
    }
    _cluster_items->UnlockIndex(lock_index);
    return true;
}

bool TableClusterIndex::add(TinyTableItem* item) {
    // add item to all the clusters
    uint64_t add_sign = sign(item);
    bool new_flag = false;
    TableUniqueIndex* unique_index = NULL;
    int lock_index = _cluster_items->Lock(add_sign);
    AtomicHashmap<uint64_t, TableUniqueIndex*>::iterator iter = _cluster_items->find(add_sign);
    if (iter == _cluster_items->end()) {
        unique_index = new(std::nothrow) TableUniqueIndex(false, m_config, _cluster);
        if (unique_index == NULL) {
            _cluster_items->UnlockIndex(lock_index);
            TT_WARN_LOG("create unique index error");
            return false;
        }
        if (!_cluster_items->insertNolock(add_sign, unique_index, lock_index)) {
            TT_WARN_LOG("insert new sign unique error");
            delete unique_index;
            unique_index = NULL;
            iter = _cluster_items->find(add_sign);
            if (iter == _cluster_items->end()) {
                _cluster_items->UnlockIndex(lock_index);
                TT_WARN_LOG("get new index error");
                return false;
            }
            unique_index = iter->second;
        } else {
            new_flag = true;
        }
    } else {
        unique_index = iter->second;
    }
    bool ret = unique_index->add(item);
    if (!ret && new_flag) {
        _cluster_items->RemoveNolock(add_sign);
        delete unique_index;
        unique_index = NULL;
    }
    _cluster_items->UnlockIndex(lock_index);       
    return ret;
}


TinyTableItem* TableClusterIndex::getItem2(RowItemValue& key1, RowItemValue& key2) {
    uint64_t master_key = key1.val_sign();
    AtomicHashmap<uint64_t, TableUniqueIndex*>::iterator iter = _cluster_items->find(master_key);
    if (iter == _cluster_items->end()) {
        return NULL;
    }
    return iter->second->getItem1(key2);
}


TableBaseIndex::iterator TableClusterIndex::find2(RowItemValue& key1, RowItemValue& key2) {
    uint64_t master_key = key1.val_sign();
    AtomicHashmap<uint64_t, TableUniqueIndex*>::iterator iter = _cluster_items->find(master_key);
    if (iter == _cluster_items->end()) {
        return NULL;
    }
    return iter->second->find1(key2);
}

int TableClusterIndex::size1(RowItemValue& key1) {
    uint64_t master_key = key1.val_sign();
    AtomicHashmap<uint64_t, TableUniqueIndex*>::iterator iter = _cluster_items->find(master_key);
    if (iter == _cluster_items->end()) {
        return -1;
    }
    return iter->second->size();
}

int   TableClusterIndex::size() {
    return _cluster_items->size();
}

void TableClusterIndex::clear() {
    _cluster_items->clear();
}


ItemValue TableClusterIndex::get2(RowItemValue& input1, RowItemValue& input2, int col) {
    /*if (col >= m_config->column_num) {
        TT_WARN_LOG("col %d is over flow max %d", col, m_config->column_num);
        return ItemValue();
    }*/
    TinyTableItem* item = getItem2(input1, input2);
    if (item == NULL) {
        TT_WARN_LOG("input has no cluster item");
        return ItemValue();
    }
    return item->getItem(m_config, col);
}


TableSortIndex::TableSortIndex(bool desc, TableConfig* config, ListSet<short>& unique, ListSet<short>& sort)
: TableBaseIndex(config, unique)
, _sort_items(NULL)
, max_item(NULL)
, min_item(NULL) {
    _type = INDEX_SORT;
    unique_keys = unique;
    sort_keys = sort;
    max_item = new TinyTableItem(config, TinyTableItem::max(config));
    min_item = new TinyTableItem(config, TinyTableItem::min(config));
    ListSet<short>::iterator iter = sort.begin();
    ListSet<short>::iterator end = sort.end();
    for (; iter != end; ++iter) {
        _cols.insert(*iter);
    }
    if (desc) {
        _sort_items = new AtomicSortmap<TinyTableItem*>(desc, true, (void*)CompareTableItem, (void*)this, &min_item);
    } else {
        _sort_items = new AtomicSortmap<TinyTableItem*>(desc, true, (void*)CompareTableItem, (void*)this, &max_item);
    }
}

TableSortIndex::~TableSortIndex() {
    if (NULL != _sort_items) {
        delete _sort_items;
        _sort_items = NULL;
    }
    if (min_item != NULL) {
        delete min_item;
        min_item = NULL;
    }
    if (max_item != NULL) {
        delete max_item;
        max_item = NULL;
    }
}


TableBaseIndex::real_iterator TableSortIndex::real_begin() {
    return _sort_items->real_begin();
}

TableBaseIndex::dump_iterator TableSortIndex::dump_begin(int& idx) {
    return _sort_items->dump_end(idx);
}

TableBaseIndex::real_iterator TableSortIndex::real_end() {
    return _sort_items->real_end();
}

TableBaseIndex::dump_iterator TableSortIndex::dump_end(int& idx) {
    return _sort_items->dump_end(idx);
}

bool TableSortIndex::mod(TinyTableItem* item, int col, ItemValue& new_val) {
    // add new index
    // here we first remove the old item
    _sort_items->remove(item);
    item->setItem(m_config, col, &new_val);
    // then add the new item
    if (!_sort_items->insert(item)) {
        TT_WARN_LOG("add item to sortmap error");
        return false;
    }
    return true;
}

bool TableSortIndex::del(TinyTableItem* item) {
    if (!_sort_items->remove(item)) {
        TT_WARN_LOG("remove item error: no exist");
        return false;
    }
    return true;
}

bool TableSortIndex::remove(TinyTableItem* item, int col, ItemValue& new_val) {
    TT_WARN_LOG("this func can not be used int sort class at this version! Please check your code");
    return true;
}

bool TableSortIndex::add(TinyTableItem* item) {
    if (!_sort_items->insert(item)) {
        TT_WARN_LOG("add item to sort error");
        return false;
    }
    return true;
}


int TableSortIndex::rank(TinyTableItem* item) {
    return _sort_items->rank(item);
}

int   TableSortIndex::size() {
    return _sort_items->size();
}

void TableSortIndex::clear() {
    _sort_items->clear();
}



TableClusterQueueIndex::TableClusterQueueIndex(TableConfig* config, ListSet<short>& cluster, short cnt_col, bool is_cluster, bool desc, int max_size)
: TableBaseIndex(config, cluster)
, _cnt_col(cnt_col)
, _cluster_queue_items(NULL)
, _max_size(max_size)
, _desc(desc) 
, _is_cluster(is_cluster) {
    _type = INDEX_CLUSTER_QUEUE;
    // free TableUniqueIndex
    _cluster_queue_items = new AtomicHashmap<uint64_t, TableQueueIndex*>(true);
}

TableClusterQueueIndex::~TableClusterQueueIndex() {
    if (NULL != _cluster_queue_items) {
        delete _cluster_queue_items;
        _cluster_queue_items = NULL;
    }
}

TableQueueIndex* TableClusterQueueIndex::getIndex(RowItemValue* val) {
    uint64_t key = val->val_sign();
    AtomicHashmap<uint64_t, TableQueueIndex*>::iterator iter = _cluster_queue_items->find(key);
    if (iter == _cluster_queue_items->end()) {
        return NULL;
    }
    return iter->second;   
}

TableQueueIndex* TableClusterQueueIndex::getIndex(uint64_t val) {
    AtomicHashmap<uint64_t, TableQueueIndex*>::iterator iter = _cluster_queue_items->find(val);
    if (iter == _cluster_queue_items->end()) {
        return NULL;
    }
    return iter->second;   
}


TableBaseIndex::real_iterator TableClusterQueueIndex::cluster_real_begin(RowItemValue* val) {
    uint64_t key = val->val_sign();
    AtomicHashmap<uint64_t, TableQueueIndex*>::iterator iter = _cluster_queue_items->find(key);
    if (iter == _cluster_queue_items->end()) {
        return TableBaseIndex::real_iterator();
    }
    return iter->second->real_begin();
}

TableBaseIndex::real_iterator TableClusterQueueIndex::cluster_real_end(RowItemValue* val) {
    uint64_t key = val->val_sign();
    AtomicHashmap<uint64_t, TableQueueIndex*>::iterator iter = _cluster_queue_items->find(key);
    if (iter == _cluster_queue_items->end()) {
        return TableBaseIndex::real_iterator();
    }
    return iter->second->real_end();
}

bool TableClusterQueueIndex::mod(TinyTableItem* item, int col, ItemValue& new_val) {
    // if the col is unique index
    TableQueueIndex* new_unique = NULL;
    uint64_t new_sign = sign(item, col, new_val);
    // lock 
    int lock_index = _cluster_queue_items->Lock(new_sign);
    AtomicHashmap<uint64_t, TableQueueIndex*>::iterator iter = _cluster_queue_items->find(new_sign);
    // recreate index
    if (iter == _cluster_queue_items->end()) {
        // create a new index
        new_unique = new(std::nothrow) TableQueueIndex(m_config, _cnt_col, _desc, _max_size);
        if (new_unique == NULL) {
            _cluster_queue_items->UnlockIndex(lock_index);
            TT_WARN_LOG("create new queue error");
            return false;
        }
        if (!_cluster_queue_items->insertNolock(new_sign, new_unique, lock_index)) {
            TT_WARN_LOG("insert new sign unique error");
            delete new_unique;
            new_unique = NULL;
            iter = _cluster_queue_items->find(new_sign);
            if (iter == _cluster_queue_items->end()) {
                _cluster_queue_items->UnlockIndex(lock_index);
                TT_WARN_LOG("get new index error");
                return false;
            }
            new_unique = iter->second;
        }
        new_unique->insert();
    } else {
        new_unique = iter->second;
    }
    bool ret = new_unique->mod(item, col, new_val);
    _cluster_queue_items->UnlockIndex(lock_index);
    return ret;
}

bool TableClusterQueueIndex::del(TinyTableItem* item) {
    // del all the index of the clusters
    uint64_t del_sign = sign(item);
    int lock_index = _cluster_queue_items->Lock(del_sign);
    AtomicHashmap<uint64_t, TableQueueIndex*>::iterator iter = _cluster_queue_items->find(del_sign);
    if (iter == _cluster_queue_items->end()) {
        _cluster_queue_items->UnlockIndex(lock_index);
        TT_WARN_LOG("del error: item not exist");
        return false;
    }    
    if (!iter->second->del(item)) {
        _cluster_queue_items->UnlockIndex(lock_index);
        TT_WARN_LOG("del item from cluster error");
        return false;
    }
    // check the cluster if the cluster size is zero then del the cluster
    if (iter->second->size() <= 0) {
        _cluster_queue_items->RemoveNolock(del_sign);
    }
    _cluster_queue_items->UnlockIndex(lock_index);
    return true;
}

bool TableClusterQueueIndex::remove(TinyTableItem* item, int col, ItemValue& new_val) {
    // del all the index of the clusters
    uint64_t del_sign = sign(item, col, new_val);
    int lock_index = _cluster_queue_items->Lock(del_sign);
    AtomicHashmap<uint64_t, TableQueueIndex*>::iterator iter = _cluster_queue_items->find(del_sign);
    if (iter == _cluster_queue_items->end()) {
        _cluster_queue_items->UnlockIndex(lock_index);
        TT_WARN_LOG("del error: item not exist");
        return false;
    }    
    if (!iter->second->del(item)) {
        _cluster_queue_items->UnlockIndex(lock_index);
        TT_WARN_LOG("del item from cluster error");
        return false;
    }
    // check the cluster if the cluster size is zero then del the cluster
    if (iter->second->size() <= 0) {
        _cluster_queue_items->RemoveNolock(del_sign);
    }
    _cluster_queue_items->UnlockIndex(lock_index);
    return true;
}

bool TableClusterQueueIndex::add(TinyTableItem* item) {
    // add item to all the clusters
    uint64_t add_sign = sign(item);
    bool new_flag = false;
    TableQueueIndex* unique_index = NULL;
    int lock_index = _cluster_queue_items->Lock(add_sign);
    AtomicHashmap<uint64_t, TableQueueIndex*>::iterator iter = _cluster_queue_items->find(add_sign);
    if (iter == _cluster_queue_items->end()) {
        unique_index = new(std::nothrow) TableQueueIndex(m_config, _cnt_col, _desc, _max_size);
        if (unique_index == NULL) {
            _cluster_queue_items->UnlockIndex(lock_index);
            TT_WARN_LOG("create queue index error");
            return false;
        }
        if (!_cluster_queue_items->insertNolock(add_sign, unique_index, lock_index)) {
            TT_WARN_LOG("insert new sign unique error");
            delete unique_index;
            unique_index = NULL;
            iter = _cluster_queue_items->find(add_sign);
            if (iter == _cluster_queue_items->end()) {
                _cluster_queue_items->UnlockIndex(lock_index);
                TT_WARN_LOG("get new index error");
                return false;
            }
            unique_index = iter->second;
        } else {
            new_flag = true;
        }
        unique_index->insert();
    } else {
        unique_index = iter->second;
    }
    bool ret = unique_index->add(item);
    if (!ret && new_flag) {
        _cluster_queue_items->RemoveNolock(add_sign);
        delete unique_index;
        unique_index = NULL;
    }
    _cluster_queue_items->UnlockIndex(lock_index);       
    return ret;
}


int TableClusterQueueIndex::size1(RowItemValue& key1) {
    uint64_t master_key = key1.val_sign();
    AtomicHashmap<uint64_t, TableQueueIndex*>::iterator iter = _cluster_queue_items->find(master_key);
    if (iter == _cluster_queue_items->end()) {
        return -1;
    }
    return iter->second->size();
}

int TableClusterQueueIndex::head(RowItemValue& key1) {
    uint64_t master_key = key1.val_sign();
    AtomicHashmap<uint64_t, TableQueueIndex*>::iterator iter = _cluster_queue_items->find(master_key);
    if (iter == _cluster_queue_items->end()) {
        return -1;
    }
    return iter->second->head();
}

int TableClusterQueueIndex::tail(RowItemValue& key1) {
    uint64_t master_key = key1.val_sign();
    AtomicHashmap<uint64_t, TableQueueIndex*>::iterator iter = _cluster_queue_items->find(master_key);
    if (iter == _cluster_queue_items->end()) {
        return -1;
    }
    return iter->second->tail();
}


int   TableClusterQueueIndex::size() {
    return _cluster_queue_items->size();
}

void TableClusterQueueIndex::clear() {
    _cluster_queue_items->clear();
}






