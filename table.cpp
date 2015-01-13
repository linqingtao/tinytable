#include "table.h"
#include "serial_table.h"

TinyTableItem* SingleTable::newItem() {
    TinyTableItem* item = NULL;
    if (!_cache->pop(item)) {
        item = new(std::nothrow) TinyTableItem();
        if (0 != item->init(m_config)) {
            delete item;
            item = NULL;
            return NULL;
        }
    } else {
        item->clear(m_config);
    }
    return item;
}
void SingleTable::freeItem(TinyTableItem* item) {
    if (item != NULL) {
        _cache->push(item);
    }
}

char SingleTable::getType(RowItemValue* value) {
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(value->col_sign());
    if (iter == _index->end()) {
        return -1;
    }
    return iter->second->getType();
}

char SingleTable::getType(RowItemValue value) {
    return getType(&value);
}

bool SingleTable::insertItem(TinyTableItem* item, bool load) {
     bool ret = true;
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel, time(NULL), load);
        if (isdel) {
            return false;
        }
    }
    // when ops with single item we lock the item
    item->lock();
    // if the table has serial col then first set the serial col
    if (m_config->serial_col != -1 && !load) {
        SingleTable& serial = Serial::SERIAL();
        SingleTable::const_iterator it = serial.find(IN2(COL_SERIAL_TABLE_NAME, m_config->table_name, COL_SERIAL_COL_NAME, m_config->column_names[m_config->serial_col]));
        bool first = false;
        if (it == serial.end()) {
            if (serial.insert(NULL, m_config->table_name.c_str(), m_config->column_names[m_config->serial_col].c_str(), 1)) {
                first = true;
            } else {
                it = serial.find(IN2(COL_SERIAL_TABLE_NAME, m_config->table_name.c_str(), COL_SERIAL_COL_NAME, m_config->column_names[m_config->serial_col]));
            }
        }
        long long cur = 1;
        if (!first) {
            cur = it->getItem()->add<long long>(serial.getConfig(), COL_SERIAL_SERIAL, 1);
        }
        item->set<long long>(m_config, m_config->serial_col, cur);
    }
    // reset cnt_col val
    if (_unique_queue_index != NULL) {
        if (!load) {
            int newHead = _unique_queue_index->insert();
            item->set(m_config, _unique_queue_index->_cnt_col, newHead);
        }
    }
    if (_cluster_queue_index != NULL) {
        if (!load) {
            AtomicHashmap<uint64_t, TableClusterQueueIndex*>::iterator queue_iter = _cluster_queue_index->begin();
            AtomicHashmap<uint64_t, TableClusterQueueIndex*>::iterator queue_iter_end = _cluster_queue_index->end();
            for (; queue_iter != queue_iter_end; ++queue_iter) {
                uint64_t sign = queue_iter->second->sign(item);
                TableQueueIndex* index = queue_iter->second->getIndex(sign);
                if (index != NULL) {
                    if (!load) {
                        int newHead = index->insert();
                        item->set(m_config, index->_cnt_col, newHead);
                    }
                } else {
                    if (!load) {
                        int col = queue_iter->second->_cnt_col;
                        item->set(m_config, col, 1);
                    }
                }
            }       
        }
    }
    // push to unique get all the entries and insert
    // when some fail we should stop the ops && recover the map
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->begin();
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator it = _index->begin();
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator end = _index->end();
    for (; iter != end; ++iter) {
        if (iter->first == _primary_sign) {continue;}
        if (!iter->second->add(item)) {
            TT_WARN_LOG("index id %llu add the item error!", iter->first);
            ret = false;
            break;
        }
    }
    // if success push to primary index
    if (ret) {
        if (!_primary_index->add(item)) {
            TT_WARN_LOG("item has conflict item exist can not insert");
            ret = false;
        }
    }
    // if prev success then do queue insert
    if (ret) {
        if (_unique_queue_index != NULL) {
            // first add the cur_end
            int newHead = item->get<int>(m_config, _unique_queue_index->_cnt_col);
            int max_size = _unique_queue_index->maxSize();
            // check the size
            int head = _unique_queue_index->head();
            while (head < newHead) {
                if (__sync_bool_compare_and_swap(&_unique_queue_index->_cur_begin, head, newHead)) {
                    break;
                }
                head = _unique_queue_index->head();
            }
            if (head < newHead) {
                head = newHead;
            }
            if (max_size != -1) {
RETRY:
                int tail = _unique_queue_index->tail();
                int size = head - tail;
                if (size >= max_size) {
                    int delCnt = newHead - max_size;
                    // del the old one
                    if (!load) {
                        for (int k = tail + 1; k <= delCnt; ++k) {
                            if (!del(IN1(_unique_queue_index->_cnt_col, delCnt))) {
                                TT_DEBUG_LOG("del queue old %d error", delCnt);
                            }
                        }
                    }
                    if (!__sync_bool_compare_and_swap(&_unique_queue_index->_cur_end, tail, delCnt)) {
                        goto RETRY;
                    }
                }
            }
            if (!_unique_queue_index->add(item)) {
                TT_WARN_LOG("insert unique queue item error");
                ret = false;
            }
        }
        if (_cluster_queue_index != NULL) {
            AtomicHashmap<uint64_t, TableClusterQueueIndex*>::iterator queue_iter = _cluster_queue_index->begin();
            AtomicHashmap<uint64_t, TableClusterQueueIndex*>::iterator queue_iter_end = _cluster_queue_index->end();
            for (; queue_iter != queue_iter_end; ++queue_iter) {
                TableClusterQueueIndex* cluster_queue = queue_iter->second;
                uint64_t sign = queue_iter->second->sign(item);
                TableQueueIndex* index = queue_iter->second->getIndex(sign);
                if (index != NULL) {
                    int max_size = index->maxSize();
                    int newHead = item->get<int>(m_config, index->_cnt_col);
                    int head = index->head();
                    while (head < newHead) {
                        if (__sync_bool_compare_and_swap(&index->_cur_begin, head, newHead)) {
                            break;
                        }
                        head = index->head();
                    }
                    if (head < newHead) {
                        head = newHead;
                    }
                    if (max_size != -1) {

CLUSTER_RETRY:
                        int tail = index->tail();
                        int size = head - tail;
                        if (size >= max_size) {
                            int delCnt = newHead - max_size;
                            if (!load) {
                                RowItemValue key;
                                ListSet<short>::iterator it = cluster_queue->_cols.begin();
                                ListSet<short>::iterator it_end = cluster_queue->_cols.end();
                                for (; it != it_end; ++it) {
                                    key.push(*it, item->getItem(m_config, *it));
                                }
                                if (cluster_queue->isCluster()) {
                                    for (int k = tail + 1; k <= delCnt; ++k) {
                                        RowItemValue key2;
                                        key2.push(index->_cnt_col, delCnt);
                                        if (!del(&key, &key2)) { 
                                            TT_DEBUG_LOG("del cluster queue old %d error", delCnt);
                                        }
                                    }
                                } else {
                                    for (int k = tail + 1; k <= delCnt; ++k) {
                                        RowItemValue key2 = key;
                                        key2.push(index->_cnt_col, delCnt);
                                        if (!del(&key2)) {
                                            TT_DEBUG_LOG("del cluster queue old %d error", delCnt);
                                        }
                                    }
                                }
                            }
                            if (!__sync_bool_compare_and_swap(&index->_cur_end, tail, delCnt)) {
                                goto CLUSTER_RETRY;
                            }
                        }
                    }
                }
                if (!queue_iter->second->add(item)) {
                    TT_WARN_LOG("add cluster queue item error");
                    ret = false;
                }
            }
        }
    }

    // if fail we should recover the map
    if (!ret) {
        for (; it != iter; ++it) {
            if (it->first == _primary_sign) {continue;}
            if (!it->second->del(item)) {
                TT_WARN_LOG("index id %llu recover insert fail error", it->first);
            }
        }
        // recover queue
        if (_unique_queue_index != NULL) {
            _unique_queue_index->del(item);
        }
        if (_cluster_queue_index != NULL) {
             AtomicHashmap<uint64_t, TableClusterQueueIndex*>::iterator queue_iter = _cluster_queue_index->begin();
            AtomicHashmap<uint64_t, TableClusterQueueIndex*>::iterator queue_iter_end = _cluster_queue_index->end();
            for (; queue_iter != queue_iter_end; ++queue_iter) {
                queue_iter->second->del(item);
            }
        }
        freeItem(item);
    } else {
        // push to dirty
        if (m_config->table_type == TABLE_TYPE_DB && m_config->updatetype == 0 && !load) {
            _dirty_items->insert((uint64_t)item, 0);
        }
    }
    item->unlock();
    if (ret) {
        // we put hhe callback outside lock
        if (_insert_callback_func != NULL) {
            (*_insert_callback_func)(this, item, _insert_callback_arg);
        }
    }
    return ret;   
}

bool SingleTable::insert(RowItemValue2 value, bool load, TinyTableItem** item) {
    return insert(&value, load, item);
}

bool SingleTable::insert(RowItemValue2* value, bool load, TinyTableItem** res) {
    if (value->size() != m_config->column_num) {
        TT_WARN_LOG("table has %d column while input has %d cols", m_config->column_num, value->size());
        return false;
    }
    // create item
    TinyTableItem* item = newItem();
    if (item == NULL) {
        TT_WARN_LOG("create item error");
        return false;
    }
    // create item
    if (!item->setItems(m_config, value)) {
        TT_WARN_LOG("init item error");
        freeItem(item);
        return false;
    }
    if (load) {
        item->clear(m_config);
    }
    if (!insertItem(item, load)) {
        TT_WARN_LOG("intsert item error");
        return false;
    }
    if (res != NULL) {
        *res = item;
    }
    return true;
}

TinyTableItem* SingleTable::getItem(RowItemValue* key1) {
    uint64_t key = key1->col_sign();
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(key);
    if (iter == _index->end()) {
        TT_WARN_LOG("key is not unique");
        return NULL;
    }
    if (iter->second->getType() != INDEX_UNIQUE && iter->second->getType() != INDEX_PRIMARY) {
        TT_WARN_LOG("input key is not a unique");
        return NULL;
    }
    return iter->second->getItem1(*key1);
}

TinyTableItem* SingleTable::getItem(RowItemValue* key1, RowItemValue* key2) {
    uint64_t key = key1->col_sign();
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(key);
    if (iter == _index->end()) {
        TT_WARN_LOG("key is not unique");
        return NULL;
    }
    if (iter->second->getType() != INDEX_CLUSTER) {
        TT_WARN_LOG("input key is not a cluster");
        return NULL;
    }
    return iter->second->getItem2(*key1, *key2);
}

bool SingleTable::del(TinyTableItem* item, bool expire, bool trig, bool only_index) {
    // make the status deleted and insert to queue
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->begin();
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator end = _index->end();
    bool flag = true;
    if (_enable_trigger && trig) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return true;
        }
    }
    item->lock();
    if (item->isDeleted()) {
        TT_DEBUG_LOG("item has been deleted");
        item->unlock();
        return true;
    }
    if (!expire && !only_index) {
         if (_delete_callback_func != NULL) {
            (*_delete_callback_func)(this, item, _delete_callback_arg);
        }       
    }
    for (; iter != end; ++iter) {
        if (!iter->second->del(item)) {
            TT_WARN_LOG("del item from index %llu  type %d error", iter->first, (int)iter->second->getType());
            flag = false;
        }
    }
    // del queue
    if (_unique_queue_index != NULL) {
        if (!_unique_queue_index->del(item)) {
            TT_WARN_LOG("del item from unique queue error");
            flag = false;
        }
    }
    if (_cluster_queue_index != NULL) {
        AtomicHashmap<uint64_t, TableClusterQueueIndex*>::iterator queue_iter = _cluster_queue_index->begin();
        AtomicHashmap<uint64_t, TableClusterQueueIndex*>::iterator queue_iter_end = _cluster_queue_index->end();
        for (; queue_iter != queue_iter_end; ++queue_iter) {
            if (!queue_iter->second->del(item)) {
                TT_WARN_LOG("del cluster queue item error");
                flag = false;
            }
        }           
    }
    if (!expire) {
        item->setDeleted();
    }

    item->unlock();
    if (!expire) {
        if (_deleted_callback_func != NULL) {
            (*_deleted_callback_func)(this, item, _deleted_callback_arg);
        }
    }
    _cache->push(item);
    return flag;
}


bool SingleTable::del(RowItemValue* input) {
    // get item
    TinyTableItem* item = getItem(input);
    if (item == NULL) {
        TT_WARN_LOG("del item not exist");
        return false;
    }
    return del(item);
}

bool SingleTable::del(RowItemValue input) {
    return del(&input);
}

void SingleTable::clear() {
    // del primary index
    TableBaseIndex::iterator it = _primary_index->begin();
    TableBaseIndex::iterator it_end = _primary_index->end();
    for (; it != it_end; ++it) {
        del(it->second, false, false, true);
    }
}

void SingleTable::clear(RowItemValue input) {
    clear(&input);
}

void SingleTable::clear(RowItemValue* input) {
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(input->col_sign());
    if (iter == _index->end()) {
        return;
    }
    if (iter->second->getType() != INDEX_CLUSTER) {
        TT_WARN_LOG("input is not a cluster");
        return;
    }
    TableClusterIndex* index = (TableClusterIndex*)(iter->second);
    TableBaseIndex::iterator it = index->cluster_begin(input);
    TableBaseIndex::iterator end = index->cluster_end(input);
    for (; it != end; ++it) {
        del(it->second, false, false, true);
    }
}


bool SingleTable::del(RowItemValue* input1, RowItemValue* input2) {
    // get item
    TinyTableItem* item = getItem(input1, input2);
    if (item == NULL) {
        TT_WARN_LOG("del item not exist");
        return false;
    }
    return del(item);
}

bool SingleTable::del(RowItemValue input1, RowItemValue input2) {
    return del(&input1, &input2);
}

SingleTable::const_iterator SingleTable::begin() {
    return const_iterator(_primary_index->begin(), m_config, this);
}

SingleTable::const_iterator SingleTable::tbegin() {
    const_iterator iter(_primary_index->begin(), m_config, this);
    const_iterator iter_end = end();
    while (iter != iter_end && iter->isDeleted(true)) {
        ++iter;
    }
    return iter;
}

SingleTable::const_iterator SingleTable::end() {
    return const_iterator(_primary_index->end(), m_config, this);
}

// this is only used for cluster
SingleTable::const_iterator SingleTable::begin(RowItemValue input) {
    return begin(&input);
}

SingleTable::const_iterator SingleTable::tbegin(RowItemValue input) {
    return tbegin(&input);
}

SingleTable::const_iterator SingleTable::tbegin(RowItemValue* input) {
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(input->col_sign());
    if (iter == _index->end()) {
        return const_iterator(NULL, m_config, this);
    }
    if (iter->second->getType() != INDEX_CLUSTER) {
        TT_WARN_LOG("input is not a cluster");
        return const_iterator(NULL, m_config, this);
    }
    TableBaseIndex::iterator it = iter->second->cluster_begin(input);
    const_iterator _it(iter->second->cluster_begin(input), m_config, this);
    const_iterator _it_end = end(input);
    while (_it != _it_end && _it->isDeleted(true)) {
        ++_it;
    }
    return _it;
}

SingleTable::const_iterator SingleTable::begin(RowItemValue* input) {
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(input->col_sign());
    if (iter == _index->end()) {
        return const_iterator(NULL, m_config, this);
    }
    if (iter->second->getType() != INDEX_CLUSTER) {
        TT_WARN_LOG("input is not a cluster");
        return const_iterator(NULL, m_config, this);
    }
    TableBaseIndex::iterator it = iter->second->cluster_begin(input);
    
    return const_iterator(iter->second->cluster_begin(input), m_config, this);
}

SingleTable::real_iterator SingleTable::queue_begin() {
    if (_unique_queue_index == NULL) {
        TT_WARN_LOG("queue not exist");
        return SingleTable::real_iterator();
    }
    return real_iterator(_unique_queue_index->real_begin(), _unique_queue_index->_queue_items, m_config, this);
}

SingleTable::real_iterator SingleTable::queue_end() {
    if (_unique_queue_index == NULL) {
        TT_WARN_LOG("queue not exist");
        return SingleTable::real_iterator();
    }
    return  real_iterator(_unique_queue_index->real_end(), _unique_queue_index->_queue_items, m_config, this);
}


SingleTable::real_iterator SingleTable::queue_end(RowItemValue input) {
    return queue_end(&input);
}

SingleTable::real_iterator SingleTable::queue_end(RowItemValue* input) {
    if (_cluster_queue_index == NULL) {
        TT_WARN_LOG("cluster queue not exist");
        return SingleTable::real_iterator();
    }
    AtomicHashmap<uint64_t, TableClusterQueueIndex*>::iterator iter = _cluster_queue_index->find(input->col_sign());
    if (iter == _cluster_queue_index->end()) {
        return real_iterator();
    }
    TableQueueIndex* index = iter->second->getIndex(input);
    if (index == NULL) {
        return real_iterator(TableBaseIndex::real_iterator(), NULL, m_config, this);
    }
    return real_iterator(index->real_end(), index->_queue_items, m_config, this);
}


SingleTable::real_iterator SingleTable::queue_begin(RowItemValue input) {
    return queue_begin(&input);
}

SingleTable::real_iterator SingleTable::queue_begin(RowItemValue* input) {
    if (_cluster_queue_index == NULL) {
        TT_WARN_LOG("cluster queue not exist");
        return SingleTable::real_iterator();
    }
    AtomicHashmap<uint64_t, TableClusterQueueIndex*>::iterator iter = _cluster_queue_index->find(input->col_sign());
    if (iter == _cluster_queue_index->end()) {
        return real_iterator();
    }
    TableQueueIndex* index = iter->second->getIndex(input);
    if (index == NULL) {
        return real_iterator(TableBaseIndex::real_iterator(), NULL, m_config, this);
    }
    return real_iterator(index->real_begin(), index->_queue_items, m_config, this);
}


SingleTable::const_iterator SingleTable::end(RowItemValue input) {
    return end(&input);
}

SingleTable::const_iterator SingleTable::end(RowItemValue* input) {
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(input->col_sign());
    if (iter == _index->end()) {
        return const_iterator(NULL, m_config, this);
    }
    if (iter->second->getType() != INDEX_CLUSTER) {
        TT_WARN_LOG("input is not a cluster");
        return const_iterator(NULL, m_config, this);
    }
    return const_iterator(iter->second->cluster_end(input), m_config, this);
}

SingleTable::const_iterator SingleTable::find(RowItemValue value) {
    return find(&value);
}

SingleTable::const_iterator SingleTable::find(RowItemValue* value) {
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(value->col_sign());
    if (iter == _index->end()) {
        return const_iterator(TableBaseIndex::iterator(), m_config, this);
    }
    char type = iter->second->getType();
    if (type != INDEX_UNIQUE && type != INDEX_PRIMARY) {
        TT_WARN_LOG("input is not a unique");
        return const_iterator(iter->second->end(), m_config, this);
    }
    AtomicHashmap<uint64_t, TinyTableItem*>::iterator it = iter->second->find1(*value);
    if (_enable_trigger) {
        if (it != iter->second->end()) {
            bool isdel = false;
            trigger(it->second, false, &isdel);
            if (isdel) {
                it = iter->second->end();
            }
        }
    }
    return const_iterator(it, m_config, this);
}

SingleTable::const_iterator SingleTable::find(RowItemValue value1, RowItemValue value2) {
    return find(&value1, &value2);
}

SingleTable::const_iterator SingleTable::find(RowItemValue* value1, RowItemValue* value2) {
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(value1->col_sign());
    if (iter == _index->end()) {
        return const_iterator(TableBaseIndex::iterator(), m_config, this);
    }
    if (iter->second->getType() != INDEX_CLUSTER) {
        TT_WARN_LOG("input is not a cluster");
        return const_iterator(iter->second->end(), m_config, this);
    }
    AtomicHashmap<uint64_t, TinyTableItem*>::iterator it = iter->second->find2(*value1, *value2);
    if (_enable_trigger) {
        if (it != iter->second->cluster_end(value1)) {
            bool isdel = false;
            trigger(it->second, false, &isdel);
            if (isdel) {
                it = iter->second->cluster_end(value1);
            }
        }
    }
    return const_iterator(it, m_config, this);
}

// this is only used for sort
SingleTable::real_iterator SingleTable::real_begin(TableIndex index) {
    return real_begin(&index);
}

SingleTable::real_iterator SingleTable::real_begin(TableIndex* index) {
    uint64_t sort_hash = set_hash(index->index);
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(sort_hash);
    if (iter == _index->end()) {
        return real_iterator(TableBaseIndex::real_iterator(), NULL, m_config, this);
    }
    if (iter->second->getType() != INDEX_SORT) {
        TT_WARN_LOG("input is not a sort");
        return real_iterator(iter->second->real_end(), NULL, m_config, this);
    }
    TableSortIndex* sort_index = (TableSortIndex*)iter->second;
    return real_iterator(iter->second->real_begin(), sort_index->_sort_items, m_config, this);
}

SingleTable::real_iterator SingleTable::real_find(TableIndex* index, int rank) {
    uint64_t sort_hash = set_hash(index->index);
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(sort_hash);
    if (iter == _index->end()) {
        return real_iterator(TableBaseIndex::real_iterator(), NULL, m_config, this);
    }
    if (iter->second->getType() != INDEX_SORT) {
        TT_WARN_LOG("input is not a sort");
        return real_iterator(iter->second->real_end(), NULL, m_config, this);
    }
    TableSortIndex* sort_index = (TableSortIndex*)iter->second;
    real_iterator real_iter = real_iterator(iter->second->real_begin(), sort_index->_sort_items, m_config, this);
    real_iterator real_iter_end = real_iterator(iter->second->real_end(), sort_index->_sort_items, m_config, this);
    if (rank <= 0 || rank > size()) {
        return real_iter_end;
    }
    int i = 1;
    for (; real_iter != real_iter_end; ++real_iter, ++i) {
        if (rank == i) {
            return real_iter;
        }
    }
    return real_iter_end;
}

SingleTable::real_iterator SingleTable::real_find(TableIndex index, int rank) {
    return real_find(&index, rank);
}


SingleTable::real_iterator SingleTable::real_end(TableIndex index) {
    return real_end(&index);
}

SingleTable::real_iterator SingleTable::real_end(TableIndex* index) {
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(set_hash(index->index));
    if (iter == _index->end()) {
        return real_iterator(TableBaseIndex::real_iterator(), NULL, m_config, this);
    }
    if (iter->second->getType() != INDEX_SORT) {
        TT_WARN_LOG("input is not a sort");
        return real_iterator(iter->second->real_end(), NULL, m_config, this);
    }
    TableSortIndex* sort_index = (TableSortIndex*)iter->second;
    return real_iterator(iter->second->real_end(), sort_index->_sort_items, m_config, this);
}

int SingleTable::getDumpIdx(TableIndex index) {
    return getDumpIdx(&index);
}

int SingleTable::getDumpIdx(TableIndex* index) {
     AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(set_hash(index->index));
    if (iter == _index->end()) {
        return -1;
    }
    if (iter->second->getType() != INDEX_SORT) {
        TT_WARN_LOG("input is not a sort");
        return -1;
    }
    TableSortIndex* sort_index = (TableSortIndex*)iter->second;
    return sort_index->_sort_items->getDumpIdx();
}

SingleTable::dump_iterator SingleTable::dump_begin(TableIndex index, int idx) {
    return dump_begin(&index, idx);
}

SingleTable::dump_iterator SingleTable::dump_begin(TableIndex* index, int idx) {
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(set_hash(index->index));
    if (iter == _index->end()) {
        return dump_iterator(TableBaseIndex::dump_iterator(), idx, NULL, m_config, this);
    }
    if (iter->second->getType() != INDEX_SORT) {
        TT_WARN_LOG("input is not a sort");
        return dump_iterator(TableBaseIndex::dump_iterator(), idx, NULL, m_config, this);
    }
    TableSortIndex* sort_index = (TableSortIndex*)iter->second;
    return dump_iterator(iter->second->dump_begin(idx), idx, sort_index->_sort_items, m_config, this);
}

SingleTable::dump_iterator SingleTable::dump_end(TableIndex index, int idx) {
    return dump_end(&index, idx);
}

SingleTable::dump_iterator SingleTable::dump_end(TableIndex* index, int idx) {
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(set_hash(index->index));
    if (iter == _index->end()) {
        return dump_iterator(TableBaseIndex::dump_iterator(), idx, NULL, m_config, this);
    }
    if (iter->second->getType() != INDEX_SORT) {
        TT_WARN_LOG("input is not a sort");
        return dump_iterator(TableBaseIndex::dump_iterator(), idx, NULL, m_config, this);
    }
    TableSortIndex* sort_index = (TableSortIndex*)iter->second;
    return dump_iterator(iter->second->dump_end(idx), idx, sort_index->_sort_items, m_config, this);
}

// the first version we only privide single search interface
// if you want to get multicol items use const_iterator
// afterwards multicol items get interface will be added
ItemValue SingleTable::get(RowItemValue* input, int col) {
    TinyTableItem* item = getItem(input);
    if (item == NULL) {
        return ItemValue();
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return ItemValue();
        }
    }
    return item->getItem(m_config, col);
}

ItemValue SingleTable::get(RowItemValue input, int col) {
    return get(&input, col);
}

ItemValue SingleTable::get(RowItemValue* input1, RowItemValue* input2, int col) {
    TinyTableItem* item = getItem(input1, input2);
    if (item == NULL) {
        return ItemValue();
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return ItemValue();
        }
    }
    return item->getItem(m_config, col);
}

ItemValue SingleTable::get(RowItemValue input1, RowItemValue input2, int col) {
    return get(&input1, &input2, col);
}

int SingleTable::set(RowItemValue input1, RowItemValue input2, int col, ItemValue val, ItemValue* old) {
    return set(&input1, &input2, col, &val, old);
}

int SingleTable::set(RowItemValue* input1, RowItemValue* input2, int col, ItemValue* val, ItemValue* old) {
     // col can not be primary key
    if (_primary_index->isIndex(col)) {
        TT_WARN_LOG("primary col(%d) can not be changed!!!", col);
        return RET_TABLE_COL_PRIMARY;
    }
    uint64_t key = input1->col_sign();
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(key);
    if (iter == _index->end()) {
        TT_WARN_LOG("input has no master index");
        return RET_TABLE_INDEX_NULL;
    }
    // the set must be cluster
    if (iter->second->getType() != INDEX_CLUSTER) {
        TT_WARN_LOG("index type is not cluster: %d", (int)iter->second->getType());
        return RET_TABLE_INDEX_TYPE_ERROR;
    }
    TinyTableItem* item = getItem(input1, input2);
    if (item == NULL) {
        TT_WARN_LOG("item not exist");
        return RET_TABLE_ITEM_NULL;
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return RET_TABLE_ITEM_NULL;
        }
    }
    // if same with before then do nothing
    if (*val == item->getItem(m_config, col)) {
        TT_DEBUG_LOG("set col %d is same with before", col);
        if (old != NULL) {
            *old = val;
        }
        return RET_TABLE_OK;
    }   
    // if col has no related index then we just change the value
    if (_col_related_index[col].index.size() == 0) {
        ItemValue res = item->setItem(m_config, col, val);
        if (!item->isDeleted()) {
            if (m_config->table_type == TABLE_TYPE_DB && m_config->updatetype == 0) {
                _dirty_items->insert((uint64_t)item, 0);
            }
        }
        if (old != NULL) {
            *old = res;
        }
        if (_changed_callback_func != NULL) {
            (*_changed_callback_func)(this, item, col, _changed_callback_arg);
        }
        return RET_TABLE_OK;
    }
    // lock
    item->lock();
    if (item->isDeleted()) {
        TT_DEBUG_LOG("item has been deleted");
        item->unlock();
        return -1;
    }
    int ret = setNolock(item, col, val, old);

    item->unlock();
    if (ret == RET_TABLE_OK) {
        if (_changed_callback_func != NULL) {
            (*_changed_callback_func)(this, item, col, _changed_callback_arg);
        }
    }
    return ret;
}


int SingleTable::setNolock(TinyTableItem* item, int col, ItemValue* val, ItemValue* old) {
    // check the related index
    std::vector<TableBaseIndex*>::iterator iter = _col_related_index[col].index.begin();
    std::vector<TableBaseIndex*>::iterator it = _col_related_index[col].index.begin();
    std::vector<TableBaseIndex*>::iterator end = _col_related_index[col].index.end();
    std::vector<TableBaseIndex*> sort_index;
    bool success = true;
    for (; iter != end; ++iter) {
        //if ((*iter)->getType() == INDEX_SORT || (*iter)->getType() == INDEX_QUEUE || (*iter)->getType() == INDEX_CLUSTER_QUEUE) {
        if ((*iter)->getType() == INDEX_SORT) {
            sort_index.push_back(*iter);
            continue;
        }
        // try to add the new item
        if (!(*iter)->mod(item, col ,*val)) {
            TT_WARN_LOG("mod index %d error", (int)(*iter)->getType());
            success = false;
            break;
        }
    }
    // if fail then we should recover the item
    if (!success) {
        for (; it != iter; ++it) {
            if ((*it)->getType() == INDEX_SORT) { continue;}
            (*it)->remove(item, col, *val);
        }
    } else {
        // else ew should remove the old item
        for (; it != iter; ++it) {
            if ((*it)->getType() == INDEX_SORT) { continue;}
            (*it)->del(item);
        }
    }
    if (success && old != NULL) {
        *old = item->getItem(m_config, col);
    }
    // if success we then add the sort
    // here we do not deal with fail
    // remove old sort 
    if (success && sort_index.size() > 0) {
        std::vector<TableBaseIndex*>::iterator it = sort_index.begin();
        std::vector<TableBaseIndex*>::iterator it_end = sort_index.end();
        for (; it != it_end; ++it) {
            if (!(*it)->del(item)) {
                TT_WARN_LOG("sort del old error");
            }
        }
    }   
    // set the val
    if (success) {
        item->setItem(m_config, col, val);
    }
    if (success && sort_index.size() > 0) {
        std::vector<TableBaseIndex*>::iterator it = sort_index.begin();
        std::vector<TableBaseIndex*>::iterator it_end = sort_index.end();
        for (; it != it_end; ++it) {
            if (!(*it)->add(item)) {
                TT_WARN_LOG("sort add error");
            }
        }
    }

    if (m_config->table_type == TABLE_TYPE_DB && m_config->updatetype == 0) {
        _dirty_items->insert((uint64_t)item, 0);
    }

    return RET_TABLE_OK;
}



int SingleTable::set(RowItemValue input, RowItemValue val) {
    return set(&input, &val);
}

int SingleTable::set(RowItemValue* input, RowItemValue2* val) {
    // check primary index
    RowItemValue2::iterator iter = val->begin();
    RowItemValue2::iterator end = val->end();
    bool has_index = false;
    for (; iter != end; ++iter) {
        if (_primary_index->isIndex(iter->first)) {
            TT_WARN_LOG("primary col(%d) can not be changed", iter->first);
            return RET_TABLE_COL_PRIMARY;
        }
        if (_col_related_index[iter->first].index.size() > 0) {
            has_index = true;
        }
    }
    TinyTableItem* item = getItem(input);
    if (item == NULL) {
        TT_DEBUG_LOG("set item not eixst");
        return RET_TABLE_ITEM_NULL;
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return RET_TABLE_ITEM_NULL;
        }
    }
    // check if has index
    if (has_index) {
        item->lock();
        if (item->isDeleted()) {
            TT_DEBUG_LOG("item has been deleted");
            item->unlock();
            return RET_TABLE_ITEM_NULL;
        }
    }
    iter = val->begin();
    for (; iter != end; ++iter) {
        // if same with before then do nothing
        if (iter->second == item->getItem(m_config, iter->first)) {
            TT_DEBUG_LOG("set col %d is same with before", iter->first);
            continue;
        }
        if (0 != setNolock(item, iter->first, &iter->second, NULL)) {
            TT_WARN_LOG("set col %d error", iter->first);
        } else {
            if (has_index) {
                item->unlock();
            }
            if (_changed_callback_func != NULL) {
                (*_changed_callback_func)(this, item, iter->first, _changed_callback_arg);
            }
            if (has_index) {
                item->lock();
                if (item->isDeleted()) {
                    item->unlock();
                    return RET_TABLE_ITEM_NULL;
                }
            }
        }
    }
    if (has_index) {
        item->unlock();
    }
    return RET_TABLE_OK;
}

int SingleTable::set(RowItemValue* input, RowItemValue* val) {
    // check primary index
    RowItemValue::iterator iter = val->begin();
    RowItemValue::iterator end = val->end();
    bool has_index = false;
    for (; iter != end; ++iter) {
        if (_primary_index->isIndex(iter->first)) {
            TT_WARN_LOG("primary col(%d) can not be changed", iter->first);
            return RET_TABLE_COL_PRIMARY;
        }
        if (_col_related_index[iter->first].index.size() > 0) {
            has_index = true;
        }
    }
    TinyTableItem* item = getItem(input);
    if (item == NULL) {
        TT_DEBUG_LOG("set item not eixst");
        return RET_TABLE_ITEM_NULL;
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return RET_TABLE_ITEM_NULL;
        }
    }
    // check if has index
    if (has_index) {
        item->lock();
        if (item->isDeleted()) {
            TT_DEBUG_LOG("item has been deleted");
            item->unlock();
            return RET_TABLE_ITEM_NULL;
        }
    }
    iter = val->begin();
    for (; iter != end; ++iter) {
        // if same with before then do nothing
        if (iter->second == item->getItem(m_config, iter->first)) {
            TT_DEBUG_LOG("set col %d is same with before", iter->first);
            continue;
        }
        if (0 != setNolock(item, iter->first, &iter->second, NULL)) {
            TT_WARN_LOG("set col %d error", iter->first);
        } else {
            if (has_index) {
                item->unlock();
            }
            if (_changed_callback_func != NULL) {
                (*_changed_callback_func)(this, item, iter->first, _changed_callback_arg);
            }
            if (has_index) {
                item->lock();
                if (item->isDeleted()) {
                    item->unlock();
                    return RET_TABLE_ITEM_NULL;
                }
            }
        }
    }
    if (has_index) {
        item->unlock();
    }
    return RET_TABLE_OK;
}

int SingleTable::set(RowItemValue input1, RowItemValue input2, RowItemValue val) {
    return set(&input1, &input2, &val);   
}

int SingleTable::set(RowItemValue* input1, RowItemValue* input2, RowItemValue* val) {
     // col can not be primary key
    RowItemValue::iterator iter = val->begin();
    RowItemValue::iterator end = val->end();
    bool has_index = false;
    for (; iter != end; ++iter) {
        if (_primary_index->isIndex(iter->first)) {
            TT_WARN_LOG("primary col(%d) can not be changed!!!", iter->first);
            return RET_TABLE_COL_PRIMARY;
        }
        if (_col_related_index[iter->first].index.size() > 0) {
            has_index = true;
        }
    }
    TinyTableItem* item = getItem(input1, input2);
    if (item == NULL) {
        TT_WARN_LOG("item not exist");
        return RET_TABLE_ITEM_NULL;
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return RET_TABLE_ITEM_NULL;
        }
    }
    // lock
    if (has_index) {
        item->lock();
        if (item->isDeleted()) {
            TT_DEBUG_LOG("item has been deleted");
            item->unlock();
            return RET_TABLE_ITEM_NULL;
        }
    }
    iter = val->begin();
    for (; iter != end; ++iter) {
        if (0 != setNolock(item, iter->first, &iter->second, NULL)) {
            TT_WARN_LOG("set col %d error", iter->first);    
        } else {
            if (has_index) {
                item->unlock();
            }
            if (_changed_callback_func != NULL) {
                (*_changed_callback_func)(this, item, iter->first, _changed_callback_arg);
            }
            if (has_index) {
                item->lock();
                if (item->isDeleted()) {
                    item->unlock();
                    return RET_TABLE_ITEM_NULL;
                }
            }
        }
    }
    if (has_index) {
        item->unlock();
    }
    return 0;
}


int SingleTable::set(RowItemValue input, int col, ItemValue val, ItemValue* old) {
    return set(&input, col, &val, old);
}

int SingleTable::set(TinyTableItem* item, int col, ItemValue* val, ItemValue* old, bool trig) {
    // col can not be primary key
    if (_primary_index->isIndex(col)) {
        TT_WARN_LOG("primary col(%d) can not be changed!!!", col);
        return RET_TABLE_COL_PRIMARY;
    }
    if (item == NULL) {
        TT_WARN_LOG("item not exist");
        return RET_TABLE_ITEM_NULL;
    }
    if (_enable_trigger && trig) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return RET_TABLE_ITEM_NULL;
        }
    }
    // if same with before then do nothing
    if (*val == item->getItem(m_config, col)) {
        TT_DEBUG_LOG("set col %d is same with before", col);
        return RET_TABLE_OK;
    }
    // if col has no related index then we just change the value
    if (_col_related_index[col].index.size() == 0) {
        ItemValue res = item->setItem(m_config, col, val);
        if (old != NULL) {
            *old = res;
        }
        if (m_config->table_type == TABLE_TYPE_DB && m_config->updatetype == 0) {
            _dirty_items->insert((uint64_t)item, 0);
        }
        if (_changed_callback_func != NULL) {
            (*_changed_callback_func)(this, item, col, _changed_callback_arg);
        }
        return 0;
    }
    item->lock();
    if (item->isDeleted()) {
        TT_DEBUG_LOG("item has been deleted");
        item->unlock();
        return RET_TABLE_ITEM_NULL;
    }
    int ret = setNolock(item, col, val, old);
    item->unlock();
    if (_changed_callback_func != NULL && ret == RET_TABLE_OK) {
        (*_changed_callback_func)(this, item, col, _changed_callback_arg);
    }
    return ret;
}

int SingleTable::set(RowItemValue* input, int col, ItemValue* val, ItemValue* old) {
    // col can not be primary key
    if (_primary_index->isIndex(col)) {
        TT_WARN_LOG("primary col(%d) can not be changed!!!", col);
        return RET_TABLE_COL_PRIMARY;
    }
    uint64_t key = input->col_sign();
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(key);
    if (iter == _index->end()) {
        TT_WARN_LOG("input has no master index");
        return RET_TABLE_INDEX_NULL;
    }
    // the set must be cluster
    if (iter->second->getType() != INDEX_UNIQUE && iter->second->getType() != INDEX_PRIMARY) {
        TT_WARN_LOG("index type is not unique: %d", (int)iter->second->getType());
        return RET_TABLE_INDEX_TYPE_ERROR;
    }
    TinyTableItem* item = getItem(input);
    if (item == NULL) {
        TT_WARN_LOG("item not exist");
        return RET_TABLE_ITEM_NULL;
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return RET_TABLE_ITEM_NULL;
        }
    }
    // if same with before then do nothing
    if (*val == item->getItem(m_config, col)) {
        TT_DEBUG_LOG("set col %d is same with before", col);
        return RET_TABLE_OK;
    }
    // if col has no related index then we just change the value
    if (_col_related_index[col].index.size() == 0) {
        ItemValue res = item->setItem(m_config, col, val);
        if (old != NULL) {
            *old = res;
        }
        if (m_config->table_type == TABLE_TYPE_DB && m_config->updatetype == 0) {
            _dirty_items->insert((uint64_t)item, 0);
        }
        if (_changed_callback_func != NULL) {
            (*_changed_callback_func)(this, item, col, _changed_callback_arg);
        }
        return 0;
    }
    item->lock();
    if (item->isDeleted()) {
        TT_DEBUG_LOG("item has been deleted");
        item->unlock();
        return RET_TABLE_ITEM_NULL;
    }
    int ret = setNolock(item, col, val, old);
    item->unlock();
    if (_changed_callback_func != NULL && ret == RET_TABLE_OK) {
        (*_changed_callback_func)(this, item, col, _changed_callback_arg);
    }
    return ret;
}

int SingleTable::addNolock(TinyTableItem* item, int col, ItemValue* val, ItemValue* old) {
    std::vector<TableBaseIndex*>::iterator iter = _col_related_index[col].index.begin();
    std::vector<TableBaseIndex*>::iterator it = _col_related_index[col].index.begin();
    std::vector<TableBaseIndex*>::iterator end = _col_related_index[col].index.end();
    std::vector<TableBaseIndex*> sort_index;
    ItemValue new_val = item->getItem(m_config, col) + *val;
    bool success = true;
    for (; iter != end; ++iter) {
        char type = (*iter)->getType();
        //if (type == INDEX_SORT || type == INDEX_QUEUE || type == INDEX_CLUSTER_QUEUE) {
        if (type == INDEX_SORT) {
            sort_index.push_back(*iter);
            continue;
        }
        // try to add the new item
        if (!(*iter)->mod(item, col , new_val)) {
            TT_WARN_LOG("mod index %d error", (int)type);
            success = false;
            break;
        }
    }
    // if fail then we should recover the item
    if (!success) {
        for (; it != iter; ++it) {
            if ((*it)->getType() == INDEX_SORT) { continue;}
            (*it)->remove(item, col, new_val);
        }
    } else {
        // else ew should remove the old item
        for (; it != iter; ++it) {
            if ((*it)->getType() == INDEX_SORT) { continue;}
            (*it)->del(item);
        }
    }
    if (success && old != NULL) {
        *old = item->getItem(m_config, col);
    }
    // if success we then add the sort
    // here we do not deal with fail
    if (success && sort_index.size() > 0) {
        std::vector<TableBaseIndex*>::iterator it = sort_index.begin();
        std::vector<TableBaseIndex*>::iterator it_end = sort_index.end();
        for (; it != it_end; ++it) {
            if (!(*it)->del(item)) {
                TT_WARN_LOG("sort del old error");
            }
        }
    }
    // set the val
    if (success) {
        item->setItem(m_config, col, &new_val);
    }
    if (success && sort_index.size() > 0) {
        std::vector<TableBaseIndex*>::iterator it = sort_index.begin();
        std::vector<TableBaseIndex*>::iterator it_end = sort_index.end();
        for (; it != it_end; ++it) {
            if (!(*it)->add(item)) {
                TT_WARN_LOG("sort add error");
            }
        }
    }
    if (m_config->table_type == TABLE_TYPE_DB && m_config->updatetype == 0) {
        _dirty_items->insert((uint64_t)item, 0);
    }

    return 0;
}

int SingleTable::decNolock(TinyTableItem* item, int col, ItemValue* val, ItemValue* old) {
    std::vector<TableBaseIndex*>::iterator iter = _col_related_index[col].index.begin();
    std::vector<TableBaseIndex*>::iterator it = _col_related_index[col].index.begin();
    std::vector<TableBaseIndex*>::iterator end = _col_related_index[col].index.end();
    std::vector<TableBaseIndex*> sort_index;
    ItemValue new_val = item->getItem(m_config, col) - *val;
    bool success = true;
    for (; iter != end; ++iter) {
        char type = (*iter)->getType();
        if (type == INDEX_SORT) {
            sort_index.push_back(*iter);
            continue;
        }
        // try to add the new item
        if (!(*iter)->mod(item, col , new_val)) {
            TT_WARN_LOG("mod index %d error", (int)(*iter)->getType());
            success = false;
            break;
        }
    }
    // if fail then we should recover the item
    if (!success) {
        for (; it != iter; ++it) {
            if ((*it)->getType() == INDEX_SORT) { continue;}
            (*it)->remove(item, col, new_val);
        }
    } else {
        // else ew should remove the old item
        for (; it != iter; ++it) {
            if ((*it)->getType() == INDEX_SORT) { continue;}
            (*it)->del(item);
        }
    }
    if (success && old != NULL) {
        *old = item->getItem(m_config, col);
    }
    // if success we then add the sort
    // here we do not deal with fail
    if (success && sort_index.size() > 0) {
        std::vector<TableBaseIndex*>::iterator it = sort_index.begin();
        std::vector<TableBaseIndex*>::iterator it_end = sort_index.end();
        for (; it != it_end; ++it) {
            if (!(*it)->del(item)) {
                TT_WARN_LOG("sort del old error");
            }
        }
    }
    // set the val
    if (success) {
        item->setItem(m_config, col, &new_val);
    }
    if (success && sort_index.size() > 0) {
        std::vector<TableBaseIndex*>::iterator it = sort_index.begin();
        std::vector<TableBaseIndex*>::iterator it_end = sort_index.end();
        for (; it != it_end; ++it) {
            if (!(*it)->add(item)) {
                TT_WARN_LOG("sort mod error");
            }
        }
    }
    if (m_config->table_type == TABLE_TYPE_DB && m_config->updatetype == 0) {
        _dirty_items->insert((uint64_t)item, 0);
    }

    return 0;
}

int SingleTable::add(RowItemValue input, int col, ItemValue val, ItemValue* old) {
    return add(&input, col, &val, old);
}

int SingleTable::add(RowItemValue input1, RowItemValue input2, int col, ItemValue val, ItemValue* old) {
    return add(&input1, &input2, col, &val, old);
}

int SingleTable::add(RowItemValue* input1, RowItemValue* input2, int col, ItemValue* val, ItemValue* old) {
    // col can not be primary key
    if (_primary_index->isIndex(col)) {
        TT_WARN_LOG("primary col(%d) can not be changed!!!", col);
        return RET_TABLE_COL_PRIMARY;
    }

    TinyTableItem* item = getItem(input1, input2);
    if (item == NULL) {
        TT_WARN_LOG("item not exist");
        return RET_TABLE_ITEM_NULL;
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return RET_TABLE_ITEM_NULL;
        }
    }
    // if no index then add
    if (_col_related_index[col].index.size() == 0) {
        ItemValue res = item->add(m_config, col, val);
        if (old != NULL) {
            *old = res;
        }
        if (m_config->table_type == TABLE_TYPE_DB && m_config->updatetype == 0) {
            _dirty_items->insert((uint64_t)item, 0);
        }
        if (_changed_callback_func != NULL) {
            (*_changed_callback_func)(this, item, col, _changed_callback_arg);
        }
        return RET_TABLE_OK;
    }
    item->lock();
    if (item->isDeleted()) {
        TT_DEBUG_LOG("item has been deleted");
        item->unlock();
        return RET_TABLE_ITEM_NULL;
    }
    int ret = addNolock(item, col, val, old);
    item->unlock();
    if (_changed_callback_func != NULL && ret == RET_TABLE_OK) {
        (*_changed_callback_func)(this, item, col, _changed_callback_arg);
    }
    return ret;
}

int SingleTable::add(TinyTableItem* item, int col, ItemValue* val, ItemValue* old) {
    // col can not be primary key
    if (_primary_index->isIndex(col)) {
        TT_WARN_LOG("primary col(%d) can not be changed!!!", col);
        return RET_TABLE_COL_PRIMARY;
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return RET_TABLE_ITEM_NULL;
        }
    }
    // if no index then add
    if (_col_related_index[col].index.size() == 0) {
        ItemValue res = item->add(m_config, col, val);
        if (old != NULL) {
            *old = res;
        }
        if (m_config->table_type == TABLE_TYPE_DB && m_config->updatetype == 0) {
            _dirty_items->insert((uint64_t)item, 0);
        }
        if (_changed_callback_func != NULL) {
            (*_changed_callback_func)(this, item, col, _changed_callback_arg);
        }
        return RET_TABLE_OK;
    }
    item->lock();
    if (item->isDeleted()) {
        TT_DEBUG_LOG("item has been deleted");
        item->unlock();
        return RET_TABLE_ITEM_NULL;
    }
    int ret = addNolock(item, col, val, old);
    item->unlock();
    if (_changed_callback_func != NULL && ret == RET_TABLE_OK) {
        (*_changed_callback_func)(this, item, col, _changed_callback_arg);
    }
    return ret;
}

int SingleTable::add(RowItemValue* input, int col, ItemValue* val, ItemValue* old) {
    // col can not be primary key
    if (_primary_index->isIndex(col)) {
        TT_WARN_LOG("primary col(%d) can not be changed!!!", col);
        return RET_TABLE_COL_PRIMARY;
    }

    TinyTableItem* item = getItem(input);
    if (item == NULL) {
        TT_WARN_LOG("item NULL");
        return RET_TABLE_ITEM_NULL;
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return RET_TABLE_ITEM_NULL;
        }
    }
    // if no index then add
    if (_col_related_index[col].index.size() == 0) {
        ItemValue res = item->add(m_config, col, val);
        if (old != NULL) {
            *old = res;
        }
        if (m_config->table_type == TABLE_TYPE_DB && m_config->updatetype == 0) {
            _dirty_items->insert((uint64_t)item, 0);
        }
        if (_changed_callback_func != NULL) {
            (*_changed_callback_func)(this, item, col, _changed_callback_arg);
        }
        return RET_TABLE_OK;
    }
    item->lock();
    if (item->isDeleted()) {
        TT_DEBUG_LOG("item has been deleted");
        item->unlock();
        return RET_TABLE_ITEM_NULL;
    }
    int ret = addNolock(item, col, val, old);
    item->unlock();
    if (_changed_callback_func != NULL && ret == RET_TABLE_OK) {
        (*_changed_callback_func)(this, item, col, _changed_callback_arg);
    }
    return ret;
}

int SingleTable::dec(RowItemValue input1, RowItemValue input2, int col, ItemValue val, ItemValue* old) {
    return dec(&input1, &input2, col, &val, old);
}

int SingleTable::dec(RowItemValue input, int col, ItemValue val, ItemValue* old) {
    return dec(&input, col, &val, old);
}

int SingleTable::dec(TinyTableItem* item, int col, ItemValue* val, ItemValue* old) {
    // col can not be primary key
    if (_primary_index->isIndex(col)) {
        TT_WARN_LOG("primary col(%d) can not be changed!!!", col);
        return RET_TABLE_COL_PRIMARY;
    }

    if (item == NULL) {
        TT_WARN_LOG("item not exist");
        return RET_TABLE_ITEM_NULL;
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return RET_TABLE_ITEM_NULL;
        }
    }
    // if no index then add
    if (_col_related_index[col].index.size() == 0) {
        ItemValue res = item->dec(m_config, col, val);
        if (old != NULL) {
            *old = res;
        }
        if (m_config->table_type == TABLE_TYPE_DB && m_config->updatetype == 0) {
            _dirty_items->insert((uint64_t)item, 0);
        }
        if (_changed_callback_func != NULL) {
            (*_changed_callback_func)(this, item, col, _changed_callback_arg);
        }
        return RET_TABLE_OK;
    }
    item->lock();
    if (item->isDeleted()) {
        TT_DEBUG_LOG("item has been deleted");
        item->unlock();
        return RET_TABLE_ITEM_NULL;
    }
    int ret = decNolock(item, col, val, old);
    item->unlock();
    if (_changed_callback_func != NULL && ret == RET_TABLE_OK) {
        (*_changed_callback_func)(this, item, col, _changed_callback_arg);
    }
    return ret;

}

int SingleTable::dec(RowItemValue* input, int col, ItemValue* val, ItemValue* old) {
    // col can not be primary key
    if (_primary_index->isIndex(col)) {
        TT_WARN_LOG("primary col(%d) can not be changed!!!", col);
        return RET_TABLE_COL_PRIMARY;
    }

    TinyTableItem* item = getItem(input);
    if (item == NULL) {
        TT_WARN_LOG("item not exist");
        return RET_TABLE_ITEM_NULL;
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return RET_TABLE_ITEM_NULL;
        }
    }
    // if no index then add
    if (_col_related_index[col].index.size() == 0) {
        ItemValue res = item->dec(m_config, col, val);
        if (old != NULL) {
            *old = res;
        }
        if (m_config->table_type == TABLE_TYPE_DB && m_config->updatetype == 0) {
            _dirty_items->insert((uint64_t)item, 0);
        }
        if (_changed_callback_func != NULL) {
            (*_changed_callback_func)(this, item, col, _changed_callback_arg);
        }
        return RET_TABLE_OK;
    }
    item->lock();
    if (item->isDeleted()) {
        TT_DEBUG_LOG("item has been deleted");
        item->unlock();
        return RET_TABLE_ITEM_NULL;
    }
    int ret = decNolock(item, col, val, old);
    item->unlock();
    if (_changed_callback_func != NULL && ret == RET_TABLE_OK) {
        (*_changed_callback_func)(this, item, col, _changed_callback_arg);
    }
    return ret;

}

int SingleTable::dec(RowItemValue* input1, RowItemValue* input2, int col, ItemValue* val, ItemValue* old) {
    // col can not be primary key
    if (_primary_index->isIndex(col)) {
        TT_WARN_LOG("primary col(%d) can not be changed!!!", col);
        return RET_TABLE_COL_PRIMARY;
    }

    TinyTableItem* item = getItem(input1, input2);
    if (item == NULL) {
        TT_WARN_LOG("item not exist");
        return RET_TABLE_ITEM_NULL;
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return RET_TABLE_ITEM_NULL;
        }
    }
    // if no index then add
    if (_col_related_index[col].index.size() == 0) {
        ItemValue res = item->dec(m_config, col, val);
        if (old != NULL) {
            *old = res;
        }
        if (m_config->table_type == TABLE_TYPE_DB && m_config->updatetype == 0) {
            _dirty_items->insert((uint64_t)item, 0);
        }
        if (_changed_callback_func != NULL) {
            (*_changed_callback_func)(this, item, col, _changed_callback_arg);
        }
        return RET_TABLE_OK;
    }
    item->lock();
    if (item->isDeleted()) {
        TT_DEBUG_LOG("item has been deleted");
        item->unlock();
        return RET_TABLE_ITEM_NULL;
    }
    int ret = decNolock(item, col, val, old);
    item->unlock();
    if (_changed_callback_func != NULL && ret == RET_TABLE_OK) {
        (*_changed_callback_func)(this, item, col, _changed_callback_arg);
    }
    return ret;

}

bool SingleTable::init(TableConfig* config, bool enable_trigger) {
    if (NULL != m_config) {
        TT_WARN_LOG("table has init");
        return false;
    }
    if (config->primary_keys.size() <= 0) {
        TT_WARN_LOG("table must has unique keys");
        return false;
    }
    m_config = new(std::nothrow) TableConfig(*config);
    if (m_config == NULL) {
        TT_WARN_LOG("create table config error");
        return false;
    }
    // create trigger map
    /*std::vector<TableConfig::TimerTrigger>::iterator trigger_iter = config->triggers.begin();
    std::vector<TableConfig::TimerTrigger>::iterator trigger_end = config->triggers.end();
    int trigger_idx = 0;
    for (; trigger_iter != trigger_end; ++trigger_iter, ++trigger_idx) {
        if (trigger_iter->type == 0) {continue;}
        _col_triggers[trigger_idx] = trigger_iter->time_col;
    }*/
    // cache for item
    _cache = new(std::nothrow) AtomicLazyCache<TinyTableItem*>();
    ListSet<short>::iterator iter;
    ListSet<short>::iterator end;
    if (_cache == NULL) {
        TT_WARN_LOG("table init create cache error");
        goto INITERROR;
    }
    if (m_config->expired_time > 0) {
        if (!_cache->init(8, true, false)) {
            TT_WARN_LOG("table init cache error");
            goto INITERROR;
        }
    } else {
        if (!_cache->init(8, true)) {
            TT_WARN_LOG("table init cache error");
            goto INITERROR;

        }
    }

    // create bitmap
    /*_index_bit = new(std::nothrow) AtomicBitmap(config->column_num);
    if (NULL == _index_bit) {
        TT_WARN_LOG("create index bit(%d) error", config->column_num);
        goto INITERROR;
    }*/
    // create dirty items
    _dirty_items = new(std::nothrow) AtomicHashmap<uint64_t, int>();
    if (NULL == _dirty_items) {
        TT_WARN_LOG("create dirty queue error");
        goto INITERROR;
    }
    /*_dirty_failed_items = new(std::nothrow) AtomicQueue<TinyTableItem*>(false);
    if (NULL == _dirty_failed_items) {
        TT_WARN_LOG("create dirty failed queue error");
        goto INITERROR;
    }*/
    // create index
    _index = new(std::nothrow) AtomicHashmap<uint64_t, TableBaseIndex*>(true);
    if (NULL == _index) {
        TT_WARN_LOG("create index error");
        goto INITERROR;
    }
    // col reelated index
    _col_related_index = new(std::nothrow) ColumnRelatedIndex[config->column_num];
    if (NULL == _col_related_index) {
        TT_WARN_LOG("create col related index error");
        goto INITERROR;
    }
    // create primary index
    iter = config->primary_keys.begin();
    end = config->primary_keys.end();
    for (; iter != end; ++iter) {
        //_index_bit->set(*iter);
        _col_related_index[*iter].index.push_back(_primary_index);
    }
    _primary_sign = set_hash(config->primary_keys);
    _primary_index = new(std::nothrow) TableUniqueIndex(true, m_config, config->primary_keys);
    if (NULL == _primary_index) {
        TT_WARN_LOG("create primary index error");
        goto INITERROR;
    }
    if (!_index->insert(_primary_sign, _primary_index)) {
        TT_WARN_LOG("insert primary index to map error");
        delete _primary_index;
        _primary_index = NULL;
        goto INITERROR;
    }
    // create unique index
    if (config->unique_keys.size() > 0) {
        std::vector<TableConfig::UniqueIndex>::iterator iter = config->unique_keys.begin();
        std::vector<TableConfig::UniqueIndex>::iterator end = config->unique_keys.end();
        for (; iter != end; ++iter) {
            if (!createUnique(false, iter->keys, true)) {
                TT_WARN_LOG("create config unique index error");
                goto INITERROR;
            }
        }
    }
    if (config->cluster_keys.size() > 0) {
        std::vector<TableConfig::ClusterIndex>::iterator iter = config->cluster_keys.begin();
        std::vector<TableConfig::ClusterIndex>::iterator end = config->cluster_keys.end();
        for (; iter != end; ++iter) {
            TableIndex unique_index(iter->unique_keys);
            TableIndex cluster_index(iter->cluster_keys);
            if (!createCluster(unique_index, cluster_index)) {
                TT_WARN_LOG("create config cluster index error");
                goto INITERROR;
            }
        }
    }
    if (config->queue_keys.size() > 0) {
        std::vector<TableConfig::QueueIndex>::iterator iter = config->queue_keys.begin();
        std::vector<TableConfig::QueueIndex>::iterator end = config->queue_keys.end();
        for (; iter != end; ++iter) {
            TableIndex unique_index(iter->unique_keys);
            TableIndex queue_index(iter->queue_keys);
            if (!createQueue(unique_index, queue_index, iter->desc, iter->max_size)) {
                TT_WARN_LOG("create config queue index error");
                goto INITERROR;
            }
        }
    }
    // create sort index
    if (config->sort_keys.size() > 0) {
        std::vector<TableConfig::SortIndex>::iterator iter = config->sort_keys.begin();
        std::vector<TableConfig::SortIndex>::iterator end = config->sort_keys.end();
        for (; iter != end; ++iter) {
            TableIndex unique_index(iter->unique_keys);
            TableIndex sort_index(iter->sort_keys);
            if (!createSort(iter->desc, unique_index, sort_index, true)) {
                TT_WARN_LOG("create config sort index error");
                goto INITERROR;
            }
        }
    }
    _expired_time = m_config->expired_time;
    _enable_trigger = enable_trigger;
    return true;
INITERROR:
    if (m_config != NULL) {
        delete m_config;
        m_config = NULL;
    }
    if (_cache != NULL) {
        delete _cache;
        _cache = NULL;
    }
    /*if (_index_bit != NULL) {
        delete _index_bit;
        _index_bit = NULL;
    }*/
    if (_col_related_index != NULL) {
        delete _col_related_index;
        _col_related_index = NULL;
    }
    if (_dirty_items != NULL) {
        delete _dirty_items;
        _dirty_items = NULL;
    }
    /*if (_dirty_failed_items!= NULL) {
        delete _dirty_failed_items;
        _dirty_failed_items = NULL;
    }*/
    if (_index != NULL) {
        delete _index;
        _index = NULL;
    }
    _primary_index = NULL;
    return false;
}

bool SingleTable::createUnique(bool primary, TableIndex unique, bool config) {
    if (m_config == NULL) {
        TT_WARN_LOG("table has not been inited");
        return false;
    }
    // first we push the index to config
    if (!config) {
        std::vector<TableConfig::UniqueIndex>::iterator iter = m_config->unique_keys.begin();    
        std::vector<TableConfig::UniqueIndex>::iterator end = m_config->unique_keys.end();
        for (; iter != end; ++iter) {
            if (iter->keys == unique.index) {
                TT_WARN_LOG("unique index exist before");
                return false;
            }
        }
    }
    // create index 
    TableBaseIndex* index = new(std::nothrow) TableUniqueIndex(primary, m_config, unique.index);
    if (NULL == index) {
        TT_WARN_LOG("create unique index error");
        return false;
    }

    // push index
    uint64_t sign = set_hash(unique.index);
    if (!_index->insert(sign, index)) {
        TT_WARN_LOG("insert unique index to map error");
        delete index;
        index = NULL;
        return false;
    }
    ListSet<short>::iterator iter = unique.index.begin();
    ListSet<short>::iterator end = unique.index.end();
    for (; iter != end; ++iter) {
        _col_related_index[*iter].index.push_back(index);
        //_index_bit->set(*iter);
    }
    if (!config) {
        TableConfig::UniqueIndex unique_index;
        unique_index.keys = unique.index;
        m_config->unique_keys.push_back(unique_index);
    }
    return true;
}

bool SingleTable::createCluster(TableIndex unique, TableIndex cluster, bool config) {
    if (m_config == NULL) {
        TT_WARN_LOG("table has not been inited");
        return false;
    }
    // first we push the index to config
    if (!config) {
        std::vector<TableConfig::ClusterIndex>::iterator iter = m_config->cluster_keys.begin();    
        std::vector<TableConfig::ClusterIndex>::iterator end = m_config->cluster_keys.end();
        for (; iter != end; ++iter) {
            if (iter->unique_keys == unique.index && iter->cluster_keys == cluster.index) {
                TT_WARN_LOG("index exist before");
                return false;
            }
        }
    }
    ListSet<short>::iterator iter;
    ListSet<short>::iterator end;
    uint64_t unique_sign = 0;
    uint64_t cluster_sign = 0;
    //TableUniqueIndex* cluster_index = NULL;
    // create index 
    TableClusterIndex* index = new(std::nothrow) TableClusterIndex(m_config, unique.index, cluster.index);
    if (NULL == index) {
        TT_WARN_LOG("create unique index error");
        goto CLUSTERERROR;
    }
    // push index
    unique_sign = set_hash(unique.index);
    /*cluster_sign = set_hash(cluster.index);
    cluster_index = new(std::nothrow) TableUniqueIndex(false, m_config, cluster.index);
    if (NULL == cluster_index) {
        TT_WARN_LOG("create cluster index error");
        goto CLUSTERERROR;
    }
    if (!index->_cluster_items->insert(cluster_sign, cluster_index)) {
        delete cluster_index;
        cluster_index = NULL;
        TT_WARN_LOG("insert cluster index to unique map error");
        goto CLUSTERERROR;
    }*/
    if (!_index->insert(unique_sign, index)) {
        TT_WARN_LOG("insert cluster index to  map error");
        goto CLUSTERERROR;
    }
    if (!config) {
        TableConfig::ClusterIndex cluster_index;
        cluster_index.unique_keys = unique.index;
        cluster_index.cluster_keys = cluster.index;
        m_config->cluster_keys.push_back(cluster_index);
    }
    // add bitmap
    iter = unique.index.begin();
    end = unique.index.end();
    for (; iter != end; ++iter) {
        //_index_bit->set(*iter);
        _col_related_index[*iter].index.push_back(index);
    }
    iter = cluster.index.begin();
    end = cluster.index.end();
    for (; iter != end; ++iter) {
        //_index_bit->set(*iter);
        _col_related_index[*iter].index.push_back(index);
    }
    return true;
CLUSTERERROR:
    if (index != NULL) {
        delete index;
        index = NULL;
    }
    return false;
}

bool SingleTable::createQueue(TableIndex unique, TableIndex queue, bool desc, int max_size, bool config) {
    if (m_config == NULL) {
        TT_WARN_LOG("table has not been inited");
        return false;
    }
    if (queue.index.size() != 1) {
        TT_WARN_LOG("queue cnt col size %d error", queue.index.size());
        return false;
    }
    short cnt_col = *(queue.index.begin());
    // first we push the index to config
    if (!config) {
        std::vector<TableConfig::QueueIndex>::iterator iter = m_config->queue_keys.begin();    
        std::vector<TableConfig::QueueIndex>::iterator end = m_config->queue_keys.end();
        for (; iter != end; ++iter) {
            if (iter->queue_keys == unique.index) {
                TT_WARN_LOG("index exist before");
                return false;
            }
        }
    }
    // if unique is primary or unique
    // then create the _unique_queue_index
    bool unique_flag = false;
    if (unique.index.size() == 0 || unique.index == m_config->primary_keys) {
        unique_flag = true;
    }
    if (!unique_flag) {
        std::vector<TableConfig::UniqueIndex>::iterator iter = m_config->unique_keys.begin();
        std::vector<TableConfig::UniqueIndex>::iterator end = m_config->unique_keys.begin();
        for (; iter != end; ++iter) {
            if (unique.index == iter->keys) {
                unique_flag = true;
                break;
            }
        }
    }
    if (unique_flag && _unique_queue_index != NULL) {
        TT_WARN_LOG("table has unique queue index");
        return false;
    }
    if (unique_flag) {
        _unique_queue_index = new(std::nothrow) TableQueueIndex(m_config, cnt_col, desc, max_size);
        if (NULL == _unique_queue_index) {
            TT_WARN_LOG("create unique queue error");
            return false;
        }
    } else {
        bool cluster_flag = false;
        std::vector<TableConfig::ClusterIndex>::iterator it = m_config->cluster_keys.begin();
        std::vector<TableConfig::ClusterIndex>::iterator it_end = m_config->cluster_keys.end();
        for (; it != it_end; ++it) {
            if (unique.index == it->unique_keys || unique.index == it->cluster_keys) {
                cluster_flag = true;
                break;
            }
        }
        if (_cluster_queue_index == NULL) {
            _cluster_queue_index = new(std::nothrow) AtomicHashmap<uint64_t, TableClusterQueueIndex*>(true);
            if (_cluster_queue_index == NULL) {
                TT_WARN_LOG("create cluster queue error");
                return false;
            }
        }
        uint64_t sign = set_hash(unique.index);
        if (_cluster_queue_index->find(sign) != _cluster_queue_index->end()) {
            TT_WARN_LOG("cluster queue exist");
            return false;
        }
        ListSet<short>::iterator iter;
        ListSet<short>::iterator end;
        uint64_t unique_sign = 0;
        uint64_t queue_sign = 0;
        TableClusterQueueIndex* cluster_index = NULL;
        // create index 
        cluster_index = new(std::nothrow) TableClusterQueueIndex(m_config, unique.index, cnt_col, cluster_flag, desc, max_size);
        if (NULL == cluster_index) {
            TT_WARN_LOG("create unique index error");
            return false;
        }
        // push index
        if (!_cluster_queue_index->insert(sign, cluster_index)) {
            TT_WARN_LOG("insert cluster index to  map error");
            delete cluster_index;
            cluster_index = NULL;
            return false;
        }
        if (!config) {
            TableConfig::QueueIndex queue_index;
            queue_index.unique_keys = unique.index;
            queue_index.queue_keys = queue.index;
            m_config->queue_keys.push_back(queue_index);
        }
    }
    return true;
}


bool SingleTable::createSort(bool desc, TableIndex unique, TableIndex sort, bool config) {
    if (m_config == NULL) {
        TT_WARN_LOG("table has not been inited");
        return false;
    }
    // first we push the index to config
    if (!config) {
        std::vector<TableConfig::SortIndex>::iterator it = m_config->sort_keys.begin();    
        std::vector<TableConfig::SortIndex>::iterator it_end = m_config->sort_keys.end();
        for (; it != it_end; ++it) {
            if (it->unique_keys == unique.index && it->sort_keys == sort.index) {
                TT_WARN_LOG("index exist before");
                return false;
            }
        }
    }
    ListSet<short>::iterator iter;
    ListSet<short>::iterator end;
    uint64_t sort_sign;
    // create index 
    TableSortIndex* index = new(std::nothrow) TableSortIndex(desc, m_config, unique.index, sort.index);
    if (NULL == index) {
        TT_WARN_LOG("create sort index error");
        goto CLUSTERERROR;
    }
    // push index
    sort_sign = index->getColSign();
    if (!_index->insert(sort_sign, index)) {
        TT_WARN_LOG("insert sort index to  map error");
        goto CLUSTERERROR;
    }
    if (!config) {
        TableConfig::SortIndex sort_index;
        sort_index.unique_keys = unique.index;
        sort_index.sort_keys = sort.index;
        m_config->sort_keys.push_back(sort_index);
    }
    iter = unique.index.begin();
    end = unique.index.end();
    for (; iter != end; ++iter) {
        //_index_bit->set(*iter);
        _col_related_index[*iter].index.push_back(index);
    }
    iter = sort.index.begin();
    end = sort.index.end();
    for (; iter != end; ++iter) {
        //_index_bit->set(*iter);
        _col_related_index[*iter].index.push_back(index);
    }
    return true;
CLUSTERERROR:
    if (index != NULL) {
        delete index;
        index = NULL;
    }
    return false;
}



bool SingleTable::dump_sort(TableIndex* input) {
    uint64_t key = set_hash(input->index);
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter =  _index->find(key);
    if (iter == _index->end()) {
        TT_WARN_LOG("input has no sort index");
        return false;
    }
    // check if it is sort index
    if (iter->second->getType() != INDEX_SORT) {
        TT_WARN_LOG("index is not sort type");
        return false;
    }
    iter->second->dump();
    return true;
}

bool SingleTable::dump_sort(TableIndex input) {
    return dump_sort(&input);
}


bool SingleTable::trigger(int trigger_id, TinyTableItem* item, bool realtime, bool* isdel, time_t now, bool load) {
    /*if (m_config->triggers.size() <= trigger_id) {
        TT_WARN_LOG("trigger %d not exist", trigger_id);
        return false;
    }*/
    if (isdel != NULL) {
        *isdel = false;
    }
    // check lazy
    TableConfig::TimerTrigger& trig = m_config->triggers[trigger_id];
    short col_id = trig.time_col;
    /*if (!realtime && !load) {
        if (trig.type == 0) {
            return false;
        }
    }*/

    int64_t last_expired = item->get<int64_t>(m_config, col_id);

    int trigger_times = 0;
    if (trig.interval_ms > 0) {
        if ((now - last_expired) * 1000ll < trig.interval_ms) {
            return false;
        }
        trigger_times = (now - last_expired)/(trig.interval_ms/1000.0);
    } else {
        // if interval 0
        if (trig.interval == 0) {
            if (now < last_expired) {
                return false;
            }
            trigger_times = 1;
        } else {
            trigger_times = triggerTimes(trig.weekday, trig.hour, trig.min, trig.sec, last_expired, now);
            if (trigger_times <= 0) {
                return false;
            }
        }
    }
    // if lazy and time no expire
    if (!realtime) {
        if (trigger_times <= 0) {
            return false;
        }
    }

    std::vector<TableConfig::TriggerFunc>::iterator iter = m_config->triggers[trigger_id].funcs.begin();
    std::vector<TableConfig::TriggerFunc>::iterator end = m_config->triggers[trigger_id].funcs.end();
    for (; iter != end; ++iter) {
        switch (iter->ops_type)
        {
            case TRIGGER_OPS_SET: 
            {
                    // if same with before then do nothing
                    if (iter->val == item->getItem(m_config, iter->col)) {
                        TT_DEBUG_LOG("set col %d is same with before", iter->col);
                        continue;
                    }
                    int ret = set(item, iter->col, &iter->val, NULL, false);
                    if (0 != ret) {
                        TT_WARN_LOG("table %s trigger col %d set error", m_config->table_name.c_str(), iter->col);
                    }
                break;
            }
             case TRIGGER_OPS_DEL:
                 {
                             bool ret = del(item, false, false);                                                                 
                             if (!ret) {
                                     TT_WARN_LOG("table %s trigger col %d del error", m_config->table_name.c_str(), iter->col);
                                 }
                             if (isdel != NULL) {
                                *isdel = true;
                             }
                             break;
                         } 
             case TRIGGER_OPS_ADD: 
            {
                    
                    ItemValue val = item->getItem(m_config, iter->col);
                    if (trigger_times > 0) {
                        val = val + iter->val * trigger_times;
                        if (!iter->max.isNull() && val > iter->max) {
                            val = iter->max;
                        }
                        if (!iter->min.isNull() && val < iter->min) {
                            val = iter->min;
                        }
                    } else {
                        TT_DEBUG_LOG("set col %d is same with before", iter->col);
                        continue;
                    }
                    int ret = set(item, iter->col, &val, NULL, false);
                    if (0 != ret) {
                        TT_WARN_LOG("table %s trigger col %d add error", m_config->table_name.c_str(), iter->col);
                    }
                break;
            }       
             case TRIGGER_OPS_DEC: 
            {

                    ItemValue val = item->getItem(m_config, iter->col);
                    if (trigger_times > 0) {
                        val = val - iter->val * trigger_times;
                        if (!iter->max.isNull() && val > iter->max) {
                            val = iter->max;
                        }
                        if (!iter->min.isNull() && val < iter->min) {
                            val = iter->min;
                        }
                    } else {
                        TT_DEBUG_LOG("set col %d is same with before", iter->col);
                        continue;
                    }
                    int ret = set(item, iter->col, &val, NULL, false);
                    if (0 != ret) {
                        TT_WARN_LOG("table %s trigger col %d add error", m_config->table_name.c_str(), iter->col);
                    }
                break;
            }   
            default:
            TT_FATAL_LOG("trigger ops type %d is not supported now!", (int)iter->ops_type);
            return false;
        }
    }
    return true;
}

bool SingleTable::trigger(TinyTableItem* item, bool realtime, bool* isdel, time_t now, bool load) {
    int num = m_config->triggers.size();
    if (num <= 0) {
        return true;
    }
    std::vector<int> cols;
    item->trigger_lock();
    for (int i = 0; i < num; ++i) {
        bool ret = trigger(i, item, realtime, isdel, now, load);
        if (isdel != NULL && *isdel) {
            item->trigger_unlock();
            return true;
        }
        if (ret) {
            cols.push_back(i);
        }
    }
    if (cols.size() > 0){
        std::vector<int>::iterator iter = cols.begin();
        std::vector<int>::iterator end = cols.end();
        for (; iter != end; ++iter) {
            TableConfig::TimerTrigger& trig = m_config->triggers[*iter];
            short col_id = trig.time_col;
            // set time
            if (trig.interval_ms <= 0 && trig.interval <= 0) {continue;}
            ItemValue  new_time = (int64_t)now;
            set(item, col_id, &new_time, NULL, false);       
        }
    }
    item->trigger_unlock();
    return true;
}

bool SingleTable::trigger(int trigger_id, bool realtime) {
    if (m_config->triggers.size() <= trigger_id) {
        TT_WARN_LOG("trigger %d not exist", trigger_id);
        return false;
    }
    time_t now = time(NULL);
    AtomicHashmap<uint64_t, TinyTableItem*>::iterator it = _primary_index->begin();
    AtomicHashmap<uint64_t, TinyTableItem*>::iterator it_end = _primary_index->end();
    for (; it != it_end; ++it) {
        // we do not lock as this func is used in singlethread
        trigger(trigger_id, it->second, realtime, NULL, now);
    }
    return true;
}

bool SingleTable::insert(TinyTableItem** res, ...) {
    TinyTableItem* item = newItem();
    if (item == NULL) {
        TT_WARN_LOG("create item error");
        return false;
    }
    va_list argptr;
    RowItemValue val;
    int columns = m_config->column_num;
    va_start(argptr, res);
    for (int j = 0; j < columns; j++) {
         switch (m_config->types[j])
        {
            case OBJECT_INT32:
                {
                    int tmp = va_arg(argptr, int32_t);
                    val.push(j, ItemValue((int32_t)tmp));
                    break;
                }
            case OBJECT_STRING:
                {
                    const char* str = va_arg(argptr, char*);
                    //printf("str %s\n", str);
                    if (m_config->limits != NULL && strlen(str) > m_config->limits[j]) {
                        TT_WARN_LOG("string is too long");
                        freeItem(item);
                        return false;
                    }
                    val.push(j, ItemValue(str));
                    break;
                }
            case OBJECT_INT64:
                {
                    int64_t tmp = va_arg(argptr, long long);
                    val.push(j, ItemValue((int64_t)tmp));
                    break;
                }
            case OBJECT_UINT32:
                {
                    unsigned int tmp = va_arg(argptr, uint32_t);
                    val.push(j, ItemValue((uint32_t)tmp));
                    break;
                }

            case OBJECT_UINT64:
                {
                    uint64_t tmp = va_arg(argptr, unsigned long long);
                    val.push(j, ItemValue((uint64_t)tmp));
                    break;
                }

            case OBJECT_BOOL:
                {
                    char tmp = va_arg(argptr, int);
                    val.push(j, ItemValue((bool)tmp));
                    break;
                }
            case OBJECT_INT8:
                {
                    char tmp = va_arg(argptr, int);
                    val.push(j, ItemValue((int8_t)tmp));
                    break;
                }
            case OBJECT_UINT8:
                {
                    unsigned char tmp = va_arg(argptr, unsigned int);
                    val.push(j, ItemValue((uint8_t)tmp));
                    break;
                }
            case OBJECT_INT16:
                {
                    short tmp  = va_arg(argptr, int);
                    val.push(j, ItemValue((int16_t)tmp));
                    break;
                }
            case OBJECT_UINT16:
                {
                    unsigned short tmp = va_arg(argptr, unsigned int);
                    val.push(j, ItemValue((uint16_t)tmp));
                    break;
                }
            case OBJECT_FLOAT:
                {
                    float tmp = va_arg(argptr, double);
                    val.push(j, ItemValue((float)tmp));
                    break;
                }
            case OBJECT_DOUBLE:
                {
                    double tmp = va_arg(argptr, double);
                    val.push(j, ItemValue((double)tmp));
                    break;
                }

            default: TT_WARN_LOG("error type %d", (int)m_config->types[j]); freeItem(item); return false;
        }
    }
    va_end(argptr);
    // init item
    if (!item->setItems(m_config, &val)) {
        TT_WARN_LOG("create item params error");
        freeItem(item);
        return false;
    }
    // insert item
    /*if (overwrite) {
        del(item);
    }*/
    if (!insertItem(item)) {
        TT_WARN_LOG("insert item error");
        return false;
    }
    if (res != NULL) {
        *res = item;
    }
    return true;
}

SingleTable::SingleTable()
: _primary_sign(0)
//, _index_bit(NULL)
, _expired_time(-1)
, _enable_trigger(true)
, _cache(NULL)
, _col_related_index(NULL)
, _dirty_items(NULL)
//, _dirty_failed_items(NULL)
, _primary_index(NULL)
, _index(NULL)
, _unique_queue_index(NULL)
, _cluster_queue_index(NULL)
, m_config(NULL)
, _changed_callback_func(NULL)
, _deleted_callback_func(NULL)
, _insert_callback_func(NULL)
, _delete_callback_func(NULL)
, _changed_callback_arg(NULL)
, _deleted_callback_arg(NULL)
, _insert_callback_arg(NULL)
, _delete_callback_arg(NULL) {}

SingleTable::~SingleTable() {
    /*if (_index_bit != NULL) {
        delete _index_bit;
        _index_bit = NULL;
    }*/
    //std::set<uint64_t> delete_items;
  /*  if (_dirty_failed_items != NULL) {
        TinyTableItem* item = NULL;
        while (_dirty_failed_items->dequeue(item)) {
            if (item->isDeleted()) {
                if (delete_items.find((uint64_t)item) == delete_items.end()) {
                    delete_items.insert((uint64_t)item);
                    delete item;
                    item = NULL;
                }
            }
        }
        delete _dirty_failed_items;
        _dirty_failed_items = NULL;
    }*/
    if (_dirty_items != NULL) {
        /*TinyTableItem* item = NULL;
        AtomicHashmap<uint64_t, int>::iterator it = _dirty_items->begin();
        AtomicHashmap<uint64_t, int>::iterator it = _dirty_items->end();
        while (_dirty_items->dequeue(item)) {
            if (item->isDeleted()) {
                if (delete_items.find((uint64_t)item) == delete_items.end()) {
                    delete_items.insert((uint64_t)item);
                    delete item;
                    item = NULL;
                }
            }
        }*/
        delete _dirty_items;
        _dirty_items = NULL;
    }

    if (_cache != NULL) {
        delete _cache;
        _cache = NULL;
    }
    if (_col_related_index != NULL) {
        delete _col_related_index;
        _col_related_index = NULL;
    }
    if (_index != NULL) {
        delete _index;
        _index = NULL;
    }
    _primary_index = NULL;

    if (_unique_queue_index != NULL) {
        delete _unique_queue_index;
        _unique_queue_index = NULL;
    }
    if (_cluster_queue_index != NULL) {
        delete _cluster_queue_index;
        _cluster_queue_index = NULL;
    }
    if (m_config != NULL) {
        delete m_config;
        m_config = NULL;
    }
}

int SingleTable::rank(RowItemValue unique, TableIndex sort) {
    return rank(&unique, &sort);
}

int SingleTable::rank(RowItemValue* unique, TableIndex* sort) {
    uint64_t key = unique->col_sign();
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter =  _index->find(key);
    if (iter == _index->end()) {
        TT_WARN_LOG("input has no unique index");
        return -1;
    }
    // check if it is sort index
    if (iter->second->getType() != INDEX_PRIMARY && iter->second->getType() != INDEX_UNIQUE) {
        TT_WARN_LOG("index is not unique type");
        return -1;
    }
    TinyTableItem* item = getItem(unique);
    if (item == NULL) {
        TT_WARN_LOG("unique item is null");
        return -1;
    }
    if (_enable_trigger) {
        bool isdel = false;
        trigger(item, false, &isdel);
        if (isdel) {
            return -1;
        }
    }
    uint64_t sort_key = set_hash(sort->index);
    iter =  _index->find(sort_key);
    if (iter == _index->end()) {
        TT_WARN_LOG("has no such sort index");
        return -1;
    }
    return iter->second->rank(item);
}

int SingleTable::expire() {
    // do not expire
    if (_expired_time <= 0) {
        return 0;
    }
    const_iterator iter = begin();
    const_iterator iter_end = end();
    time_t now = time(NULL);
    for (; iter != iter_end; ++iter) {
        if (iter->isDirty()) {
            continue;
        }
        // check time
        time_t t = iter->getLastActiveTime();
        if (now >= t + _expired_time) {
            //del item
            if (!del(iter->item, true)) {
                TT_WARN_LOG("del item error in expire");
            }
        }
    }
    return 0;
}

/*bool  SingleTable::getDirtyItem(TinyTableItem*& item) {
    if (_dirty_items->dequeue(item)) {
        return true;
    }
    return false;
}

bool  SingleTable::getDirtyFailItem(TinyTableItem*& item) {
    if (_dirty_failed_items->dequeue(item)) {
        return true;
    }
    return false;
}

void  SingleTable::pushDirtyItem(TinyTableItem* item) {
    if (m_config->table_type != TABLE_TYPE_LOCAL && m_config->updatetype == 0) {
        _dirty_items->enqueue(item);
    }
}

void  SingleTable::pushDirtyFailItem(TinyTableItem* item) {
    if (m_config->table_type != TABLE_TYPE_LOCAL && m_config->updatetype == 0) {
        _dirty_failed_items->enqueue(item);
    }
}
*/

int  SingleTable::parse_req_data(char* buffer, int len, int& max_num, RowItemValue2& val1, RowItemValue2& val2, bool mod) {
#define CHECK_REQPOS_DESERIALIZE(offset) \
        if (pos + offset > len) { \
            TT_WARN_LOG("pos %d offset %d len %d\n", pos, offset, len); \
            return -1; \
        }
    int pos = 0;
    const TableConfig* config = getConfig();
        // this first 4 bytes is max_num
        if (!mod) {
            CHECK_REQPOS_DESERIALIZE(4)
            max_num = *(int*)buffer;
            pos += 4;
            CHECK_REQPOS_DESERIALIZE(2)
            int val_num = *(unsigned short*)(buffer + pos);
            pos += 2;
        }
        // parse buffer to RowItemValue
        unsigned short index = 0;
        unsigned char  type = 0;
        while (pos < len) {
            index = *(unsigned short*)(buffer + pos);
            RowItemValue2* val = &val1;
            if (index & 0x80) {
                val = &val2;
                index &= 0x7F;
            }
            if (index >= m_config->column_num) {
                TT_WARN_LOG("index %d error", index);
                return -1;
            }
            pos += sizeof(index);
            type = buffer[pos];
            pos += sizeof(type);
            // check if equal
            if (type != config->types[index]) {
                TT_WARN_LOG("col %d type is %d but the recv type is %d", index, config->types[index], type);
                return -1;
            }
            // switch type
            switch (type)
            {
                case OBJECT_INT32: val->push(index, *(int32_t*)(buffer + pos)); pos += 4; break;
                case OBJECT_STRING: 
                {
                    // get char len
                    int str_len = *(int*)(buffer + pos);
                    pos += 4;
                    if (str_len < 0 || str_len > len - pos) {
                        TT_WARN_LOG("str len %d error", str_len);
                        return -1;
                    }
                    // make the end 0 after set string recover it
                    char tmp = buffer[pos + str_len];
                    buffer[pos + str_len] = 0;
                    /*bsl::AutoBuffer buf;
                    buf << (buffer + pos);*/
                    val->push(index, buffer + pos);
                    buffer[pos + str_len] = tmp;
                    pos += str_len;
                    break;
                }
                case OBJECT_INT64: val->push(index, *(int64_t*)(buffer + pos)); pos += 8; break;
                case OBJECT_UINT64: val->push(index, *(uint64_t*)(buffer + pos)); pos += 8; break;
                case OBJECT_UINT32: val->push(index, *(uint32_t*)(buffer + pos)); pos += 4; break;
                case OBJECT_BOOL: val->push(index, (bool)*(buffer+pos)); pos += 1; break;
                case OBJECT_UINT8: val->push(index, *(uint8_t*)(buffer + pos)); pos += 1; break;
                case OBJECT_INT8: val->push(index, *(int8_t*)(buffer + pos)); pos += 1; break;
                case OBJECT_UINT16: val->push(index, *(uint16_t*)(buffer + pos)); pos += 2; break;
                case OBJECT_INT16: val->push(index, *(int16_t*)(buffer + pos)); pos += 2; break;
                case OBJECT_FLOAT: val->push(index, *(float*)(buffer + pos)); pos += 4; break;
                case OBJECT_DOUBLE: val->push(index, *(double*)(buffer + pos)); pos += 8; break;
                case OBJECT_NULL: break;
            }
        }
#undef CHECK_REQPOS_DESERIALIZE
    return pos;
}

int  SingleTable::parse_res_data(char* buffer, int len, bool load, std::vector<TinyTableItem*>* vec) {
#define CHECK_POS_DESERIALIZE(offset) \
        if (pos + offset > len) { \
            TT_WARN_LOG("pos %d offset %d len %d\n", pos, offset, len); \
            return -1; \
        }
    int pos = 0;
    const TableConfig* config = getConfig();
    while (pos < len) {
        // this first 4 bytes is len
        CHECK_POS_DESERIALIZE(4)
        int item_len = *(int*)(buffer + pos);
        pos += 4;
        CHECK_POS_DESERIALIZE(item_len)
        // parse buffer to RowItemValue
        unsigned short index = 0;
        unsigned char  type = 0;
        int item_end = pos + item_len;
        RowItemValue2 val;
        while (pos < item_end ) {
            index = *(unsigned short*)(buffer + pos);
            index &= 0x7F;
            pos += sizeof(index);
            type = buffer[pos];
            pos += sizeof(type);
            if (index >= m_config->column_num) {
                TT_WARN_LOG("index %d error", index);
                return -1;
            }
            // check if equal
            if (type != config->types[index]) {
                TT_WARN_LOG("col %d type is %d but the recv type is %d", index, config->types[index], type);
                return -1;
            }
            // switch type
            switch (type)
            {
                case OBJECT_INT32: val.push(index, *(int32_t*)(buffer + pos)); pos += 4; break;
                case OBJECT_STRING: 
                {
                    // get char len
                    int str_len = *(int*)(buffer + pos);
                    pos += 4;
                    if (str_len < 0 || str_len > len - pos) {
                        TT_WARN_LOG("str len %d error", str_len);
                        return -1;
                    }
                    // make the end 0 after set string recover it
                    char tmp = buffer[pos + str_len];
                    buffer[pos + str_len] = 0;
                    val.push(index, buffer + pos);
                    buffer[pos + str_len] = tmp;
                    pos += str_len;
                    break;
                }
                case OBJECT_INT64: val.push(index, *(int64_t*)(buffer + pos)); pos += 8; break;
                case OBJECT_UINT64: val.push(index, *(uint64_t*)(buffer + pos)); pos += 8; break;
                case OBJECT_UINT32: val.push(index, *(uint32_t*)(buffer + pos)); pos += 4; break;
                case OBJECT_BOOL: val.push(index, (bool)*(buffer+pos)); pos += 1; break;
                case OBJECT_UINT8: val.push(index, *(uint8_t*)(buffer + pos)); pos += 1; break;
                case OBJECT_INT8: val.push(index, *(int8_t*)(buffer + pos)); pos += 1; break;
                case OBJECT_UINT16: val.push(index, *(uint16_t*)(buffer + pos)); pos += 2; break;
                case OBJECT_INT16: val.push(index, *(int16_t*)(buffer + pos)); pos += 2; break;
                case OBJECT_FLOAT: val.push(index, *(float*)(buffer + pos)); pos += 4; break;
                case OBJECT_DOUBLE: val.push(index, *(double*)(buffer + pos)); pos += 8; break;

                case OBJECT_NULL: break;
            }
        }
        if (pos != item_end) {
            TT_WARN_LOG("res data is valid: pos %d item_end %d", pos, item_end);
            return -1;
        }
        // insert the value to table
        TinyTableItem* item_insert = NULL;
        if (!insert(&val, load, &item_insert)) {
            TT_WARN_LOG("insert val to table error");
        } else {
            if (vec != NULL) {
                vec->push_back(item_insert);
            }
        }
    }
#undef CHECK_POS_DESERIALIZE
    return pos;
}

int SingleTable::size() {
    return _primary_index->size();
}

int SingleTable::size(RowItemValue value) {
    return size(&value);
}

int SingleTable::size(RowItemValue* value) {
    AtomicHashmap<uint64_t, TableBaseIndex*>::iterator iter = _index->find(value->col_sign());
    if (iter == _index->end()) {
        return -1;
    }
    if (iter->second->getType() != INDEX_CLUSTER) {
        TT_WARN_LOG("input is not a cluster");
        return -1;
    }
    return iter->second->size1(*value);
}



