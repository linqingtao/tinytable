#include <dirent.h>
#include "memtable.h"
#include "proto_config.h"
#include "config.h"
#include "tlsbuffer.h"

namespace tinytable {

AtomicTimer* MemTable::global_timer = NULL;


MemTable::MemTable()
: _tables(NULL)
, _id_config(NULL) {
}

MemTable::~MemTable()
{
    if (_tables != NULL) {
        for (int i = 0; i < MAX_TABLE_NUM; ++i) {
            if (_tables[i] != NULL) {
                delete _tables[i];
                _tables[i] = NULL;
            }
        }
        delete _tables;
        _tables = NULL;
    }
    if (_id_config != NULL) {
        delete _id_config;
        _id_config = NULL;
    }
}

SingleTable::real_iterator MemTable::queue_begin(TableName name) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::real_iterator();
    }
    return table->queue_begin();
}

SingleTable::real_iterator MemTable::queue_end(TableName name) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::real_iterator();
    }
    return table->queue_end();
}

SingleTable::real_iterator MemTable::queue_begin(TableName name, RowItemValue input) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::real_iterator();
    }
    return table->queue_begin(&input);
}

SingleTable::real_iterator MemTable::queue_end(TableName name, RowItemValue input) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::real_iterator();
    }
    return table->queue_end(&input);
}

SingleTable::real_iterator MemTable::real_find(TableName name, TableIndex index, int rank) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::real_iterator();
    }   
    return table->real_find(&index, rank);
}

int  MemTable::addTable(SingleTable* table) {
    // check if exist
    const TableConfig* config = table->getConfig();
    if (NULL == config) {
        TT_WARN_LOG("table has not init");
        return -1;
    }
    if (_id_config == NULL) {
        TT_WARN_LOG("tinytable has not init");
        return -1;
    }
    if (_tables[config->table_id] != NULL) {
        TT_WARN_LOG("add table %d error", config->table_id);
        return -1;
    }
    _tables[config->table_id] = table;
    // add ids
    if (!_id_config->addTable(config->table_id, config->table_name)) {
        TT_DEBUG_LOG("table(%d-%s) exist", config->table_id, config->table_name.c_str());
    }
    return 0;
}

int  MemTable::addTable(TableConfig* config) {
    if (_id_config == NULL) {
        TT_WARN_LOG("tinytable has not init");
        return -1;
    }
    SingleTable* table = new(std::nothrow) SingleTable();
    if (NULL == table) {
        TT_WARN_LOG("create table null");
        return -1;
    }
    // init table
    if (!table->init(config)) {
        TT_WARN_LOG("init table error");
        delete table;
        table = NULL;
        return -1;
    }
    int ret = addTable(table);
    if (0 != ret) {
        TT_WARN_LOG("add table error");
        delete table;
        table = NULL;
    }
    return ret;
}

int  MemTable::addTable(const char* name, int column, char* types, std::set<short>& primary, std::string* names, int id) {
    TableConfig config(name, column, types, NULL, names, id);
    config.primary_keys = primary;
    int ret = addTable(&config);
    if (ret != 0) {
        TT_WARN_LOG("add config table name %s column %d error", name, column);
    }
    return ret;
}



int  MemTable::addTable(const char* xml_path, const char* xml_file) {
    if (_id_config == NULL) {
        TT_WARN_LOG("tinytable has not init");
        return -1;
    }
    TableConfig config;
    // load config
    if (!ProtoToConfig::loadTableXmlConfig(xml_path, xml_file, &config, _id_config)) {
        TT_WARN_LOG("load table(%s/%s) error", xml_path, xml_file);
        return -1;
    }
    int ret = addTable(&config);
    if (0 != ret) {
        TT_WARN_LOG("add table (%s/%s) error", xml_path, xml_file);
    }
    return ret;
}



int  MemTable::init(const char* id_path, const char* id_file, const char* table_path, bool enable_trigger) {
    if (_id_config != NULL) {
        TT_WARN_LOG("tinytable has init");
        return -1;
    }     
    _id_config = new(std::nothrow)TableIDConfig();
    if (NULL == _id_config) {
        TT_WARN_LOG("create id config error");
        return -1;
    }
    DIR* dir = NULL;
    struct dirent* ent = NULL;
    _tables = new(std::nothrow) SingleTable*[MAX_TABLE_NUM];
    if (NULL == _tables) {
        TT_WARN_LOG("create tinytable map error");
        goto INITERROR;
    }
    memset(_tables, 0, sizeof(SingleTable*) * MAX_TABLE_NUM);
    if (!ProtoToConfig::loadTableIDConfig(id_path, id_file, _id_config)) {
        TT_WARN_LOG("load id config (%s/%s) error", id_path, id_file);
        goto INITERROR;
    }
    // get dir all the xml files
    dir = opendir(table_path);
    if (dir == NULL) {
        TT_WARN_LOG("%s is not path", table_path);
        goto INITERROR;
    }
    while(NULL != (ent = readdir(dir))) {
        if (NULL != strstr(ent->d_name, ".xml")) {
            if (0 != addTable(table_path, ent->d_name)) {
                closedir(dir);
                TT_WARN_LOG("load file %s error", ent->d_name);
                goto INITERROR;
            }
            TT_DEBUG_LOG("LOAD xml file %s", ent->d_name);
        }
    }
    closedir(dir);
    // init expire && trigger
    for (int k = 0; k < MAX_TABLE_NUM; ++k) {
        if (_tables[k] == NULL) {continue;}
        if (_tables[k]->getConfig()->expired_time <= 0) {continue;}
        uint64_t interval = _tables[k]->getConfig()->expired_time * 1000000ll;
        if (interval <= 0) {continue;}
        int table_id = _tables[k]->getConfig()->table_id;
        if (!MemTable::global_timer->addTimer(interval, boost::bind(&MemTable::expire, this, table_id), interval, TIMER_FOREVER_LOOP)) {
            TT_FATAL_LOG("init expire time error");
            return false;
        }
        TT_DEBUG_LOG("add table %d expire timer", table_id);
    }
    // trigger
    for (int k = 0; k < MAX_TABLE_NUM; ++k) {
        if (_tables[k] == NULL) {continue;}
        _tables[k]->setTriggers(enable_trigger);
    }
    // init timer for trigger
    if (enable_trigger) {
        for (int k = 0; k < MAX_TABLE_NUM; ++k) {
            if (_tables[k] == NULL) {continue;}
            const TableConfig* config = _tables[k]->getConfig();
            // check trigger
            std::vector<TableConfig::TimerTrigger>::const_iterator it = config->triggers.begin();
            std::vector<TableConfig::TimerTrigger>::const_iterator it_end = config->triggers.end();
            int idx = 0;
            for (; it != it_end; ++it) {
                if (it->type == 1) {
                    continue;
                }
                if (it->interval_ms > 0) {
                    if (!MemTable::global_timer->addTimer(it->interval_ms, boost::bind(&SingleTable::trigger, _tables[k], idx, true), it->interval_ms, TIMER_FOREVER_LOOP)) {
                        TT_FATAL_LOG("init table %s timer for trigger %d error", config->table_name.c_str(), idx);
                        return false;
                    }
                } else if (it->interval > 0) {
                    if (!MemTable::global_timer->addFixedTimer(it->weekday, it->hour, it->min, it->sec, boost::bind(&SingleTable::trigger, _tables[k], idx, true), it->interval * 1000000ll, TIMER_FOREVER_LOOP)) {
                        TT_FATAL_LOG("init table %s timer for trigger %d error", config->table_name.c_str(), idx);
                        return false;
                    }
                } else {
                    TT_DEBUG_LOG("interval trigger");
                }
                ++idx;
            }
        }
    }
    return 0;
INITERROR:
    if (NULL != _id_config) {
        delete _id_config;
        _id_config = NULL;
    }
    return -1;
}

// del
int  MemTable::delTable(TableName table) {
    int table_id = table.id;
    if (table_id == -1) {
        table_id = _id_config->getID(table.name);
        if (table_id == -1) {
            TT_WARN_LOG("del table %s not exist", table.name.c_str());
            return -1;
        }
    }
    _id_config->delTable(table_id);
    if (_tables[table_id] != NULL) {
        return -1;
    }
    SingleTable* tmp = _tables[table_id];
    _tables[table_id] = NULL;
    delete tmp;
    tmp = NULL;
    return 0;
}

int  MemTable::getTableID(std::string table) {
    return _id_config->getID(table);
}

bool MemTable::setChangedCallbackFunc(TableName name, itemchanged_callback* func, void* arg) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return false;
    }
    table->setChangedCallbackFunc(func, arg);
    return true;
}

bool MemTable::setDeletedCallbackFunc(TableName name, itemdeleted_callback* func, void* arg) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return false;
    }
    table->setDeletedCallbackFunc(func, arg);
    return true;
}

bool MemTable::setInsertCallbackFunc(TableName name, iteminsert_callback* func, void* arg) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return false;
    }
    table->setInsertCallbackFunc(func, arg);
    return true;
}

// get interface
SingleTable* MemTable::getTable(TableName& name) {
    int table_id = name.id;
    if (table_id == -1) {
        table_id =  _id_config->getID(name.name);
        if (table_id == -1) {
            TT_WARN_LOG("table %s not exist", name.name.c_str());
            return NULL;
        }
    }
    //AtomicHashmap<int, SingleTable*>::iterator iter = _tables->find(table_id);
    if (_tables[table_id] == NULL) {
        TT_WARN_LOG("table %d not exist", table_id);
        return NULL;
    }
    return _tables[table_id];
}

SingleTable* MemTable::getTable(std::string name) {
    int table_id;
    table_id =  _id_config->getID(name);
    if (table_id == -1) {
        TT_WARN_LOG("table %s not exist", name.c_str());
        return NULL;
    }
    //AtomicHashmap<int, SingleTable*>::iterator iter = _tables->find(table_id);
    if (_tables[table_id] == NULL) {
        TT_WARN_LOG("table %d not exist", table_id);
        return NULL;
    }
    return _tables[table_id];
}

SingleTable::const_iterator MemTable::begin(SingleTable* table) {
    return table->begin();
}

SingleTable::const_iterator MemTable::begin(TableName name) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::const_iterator();
    }
    return table->begin();
}

SingleTable::const_iterator MemTable::end(TableName name) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::const_iterator();
    }
    return table->end();
}

SingleTable::const_iterator MemTable::end(SingleTable* table) {
    return table->end();
}

// this is only used for cluster
SingleTable::const_iterator MemTable::begin(TableName name, RowItemValue input) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::const_iterator();
    }
    return table->begin(&input);
}

SingleTable::const_iterator MemTable::begin(SingleTable* table, RowItemValue* input) {
    return table->begin(input);
}

SingleTable::const_iterator MemTable::end(TableName name, RowItemValue input) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::const_iterator();
    }
    return table->end(&input);
}

SingleTable::const_iterator MemTable::end(SingleTable* table, RowItemValue* input) {
    return table->end(input);
}

// this is only used for sort
SingleTable::real_iterator MemTable::real_begin(TableName name, TableIndex index) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::real_iterator();
    }
    return table->real_begin(&index);
}

SingleTable::real_iterator MemTable::real_begin(SingleTable* table, TableIndex* index) {
    return table->real_begin(index);
}

SingleTable::real_iterator MemTable::real_end(TableName name, TableIndex index) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::real_iterator();
    }
    return table->real_end(&index);
}

SingleTable::real_iterator MemTable::real_end(SingleTable* table, TableIndex* index) {
    return table->real_end(index);
}

SingleTable::dump_iterator MemTable::dump_begin(TableName name, TableIndex index, int idx) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::dump_iterator();
    }
    return table->dump_begin(&index, idx);
}

SingleTable::dump_iterator MemTable::dump_begin(SingleTable* table, TableIndex* index, int idx) {
    return table->dump_begin(index, idx);
}

SingleTable::dump_iterator MemTable::dump_end(TableName name, TableIndex index, int idx) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::dump_iterator();
    }
    return table->dump_end(&index, idx);
}

SingleTable::dump_iterator MemTable::dump_end(SingleTable* table, TableIndex* index, int idx) {
    return table->dump_end(index, idx);
}

int    MemTable::getDumpIdx(TableName name, TableIndex index) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    return table->getDumpIdx(&index);
}

SingleTable::const_iterator MemTable::find(TableName name, RowItemValue value, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::const_iterator();
    }
    return find(table, &value, type);
}

SingleTable::const_iterator MemTable::find(TableName* name, RowItemValue* value, char type) {
    SingleTable* table = getTable(*name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name->id, name->name.c_str());
        return SingleTable::const_iterator();
    }
    return find(table, value, type);
}

SingleTable::const_iterator MemTable::find(SingleTable* table, RowItemValue* value, char type) {
    return table->find(value);
}

SingleTable::const_iterator MemTable::find(TableName name, RowItemValue value1, RowItemValue value2, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return SingleTable::const_iterator();
    }
    return find(table, &value1, &value2, type);
}

SingleTable::const_iterator MemTable::find(TableName* name, RowItemValue* value1, RowItemValue* value2, char type) {
    SingleTable* table = getTable(*name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name->id, name->name.c_str());
        return SingleTable::const_iterator();
    }
    return find(table, value1, value2, type);
}

SingleTable::const_iterator MemTable::find(SingleTable* table, RowItemValue* value1, RowItemValue* value2, char type) {
    return table->find(value1, value2);
}

void MemTable::clear(TableName name, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return;
    }
    table->clear();
}

void MemTable::clear(SingleTable* table, char type) {
    table->clear();
}

void MemTable::clear(TableName name, RowItemValue value, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return;
    }
    table->clear(&value);
}

void MemTable::clear(SingleTable* table, RowItemValue* value, char type) {
    table->clear(value);
}


bool MemTable::del(TableName name, RowItemValue input, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return false;
    }
    return table->del(&input);
}

bool MemTable::del(SingleTable* table, RowItemValue* input, char type) {
    return table->del(input);
}

bool MemTable::del(TableName name, RowItemValue input1, RowItemValue input2, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return false;
    }
    return table->del(&input1, &input2);
}

bool MemTable::del(SingleTable* table, RowItemValue* input1, RowItemValue* input2, char type) {
    return table->del(input1, input2);
}

ItemValue MemTable::get(TableName name, RowItemValue input, int col, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return ItemValue();
    }
    return table->get(&input, col);
}

ItemValue MemTable::get(SingleTable* table, RowItemValue* input, int col, char type) {
    return table->get(input, col);
}

ItemValue MemTable::get(TableName name, RowItemValue input1, RowItemValue input2, int col, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return ItemValue();
    }
    return table->get(&input1, &input2, col);
}

ItemValue MemTable::get(SingleTable* table, RowItemValue* input1, RowItemValue* input2, int col, char type) {
    return table->get(input1, input2, col);
}

int MemTable::set(TableName name, RowItemValue input, RowItemValue val, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    return table->set(&input, &val);
}

int MemTable::set(SingleTable* table, RowItemValue* input, RowItemValue* val, char type) {
    return table->set(input, val);
}

int MemTable::set(SingleTable* table, TinyTableItem* item, RowItemValue* val, char type) {
    RowItemValue::iterator iter = val->begin();
    RowItemValue::iterator iter_end = val->end();
    for (; iter != iter_end; ++iter) {
        table->set(item, iter->first, &iter->second, NULL);
    }
    return 0;
}

int MemTable::set(TableName name, RowItemValue input1, RowItemValue input2, RowItemValue val, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    return table->set(&input1, &input2, &val);
}

int MemTable::set(SingleTable* table, RowItemValue* input1, RowItemValue* input2, RowItemValue* val, char type) {
    return table->set(input1, input2, val);
}

int MemTable::set(TableName name, RowItemValue input1, RowItemValue input2, int col, ItemValue val, ItemValue* old, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    return table->set(&input1, &input2, col, &val, old);
}

int MemTable::set(SingleTable* table, RowItemValue* input1, RowItemValue* input2, int col, ItemValue* val, ItemValue* old, char type) {
    return table->set(input1, input2, col, val, old);
}

int MemTable::set(SingleTable* table, TinyTableItem* item, int col, ItemValue* val, ItemValue* old, char type) {
    return table->set(item, col, val, old);
}

int MemTable::set(TableName name, RowItemValue input, int col, ItemValue val, ItemValue* old, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    return table->set(&input, col, &val, old);
}

int MemTable::set(SingleTable* table, RowItemValue* input, int col, ItemValue* val, ItemValue* old, char type) {
    return table->set(input, col, val, old);
}

int MemTable::add(TableName name, RowItemValue input1, RowItemValue input2, int col, ItemValue val, ItemValue* cur, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    return table->add(&input1, &input2, col, &val, cur);
}

int MemTable::add(SingleTable* table, RowItemValue* input1, RowItemValue* input2, int col, ItemValue* val, ItemValue* cur, char type) {
    return table->add(input1, input2, col, val, cur);
}

int MemTable::add(TableName name, RowItemValue input, int col, ItemValue val, ItemValue* cur, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    return table->add(&input, col, &val, cur);
}

int MemTable::add(SingleTable* table, RowItemValue* input, int col, ItemValue* val, ItemValue* cur, char type) {
    return table->add(input, col, val, cur);
}

int MemTable::add(SingleTable* table, TinyTableItem* item, int col, ItemValue* val, ItemValue* cur, char type) {
    return table->add(item, col, val, cur);
}

int MemTable::dec(TableName name, RowItemValue input1, RowItemValue input2, int col, ItemValue val, ItemValue* cur, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    return table->dec(&input1, &input2, col, &val, cur);
}

int MemTable::dec(SingleTable* table, RowItemValue* input1, RowItemValue* input2, int col, ItemValue* val, ItemValue* cur, char type) {
    return table->dec(input1, input2, col, val, cur);
}

int MemTable::dec(TableName name, RowItemValue input, int col, ItemValue val, ItemValue* cur, char type) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    return table->dec(&input, col, &val, cur);
}

int MemTable::dec(SingleTable* table, RowItemValue* input, int col, ItemValue* val, ItemValue* cur, char type) {
    return table->dec(input, col, val, cur);
}

int MemTable::dec(SingleTable* table, TinyTableItem* item, int col, ItemValue* val, ItemValue* cur, char type) {
    return table->dec(item, col, val, cur);
}

bool MemTable::insert(TableName name,  ...) {
    TT_DEBUG_LOG("[INSERT] memtable insert");
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return false;
    }
    const TableConfig* config = table->getConfig();
    va_list argptr;
    RowItemValue2 val;
    int columns = config->column_num;
    va_start(argptr, name);
    for (int j = 0; j < columns; j++) {
         switch (config->types[j])
        {
            case OBJECT_INT32:
                {
                    int tmp = va_arg(argptr, int32_t);
                    val.push(j, (int32_t)tmp);
                    break;
                }
            case OBJECT_STRING:
                {
                    const char* str = va_arg(argptr, char*);
                    //printf("str %s\n", str);
                    if (config->limits != NULL && strlen(str) > config->limits[j]) {
                        TT_WARN_LOG("string is too long");
                        return false;
                    }
                    val.push(j, str);
                    break;
                }
            case OBJECT_INT64:
                {
                    int64_t tmp = va_arg(argptr, long long);
                    val.push(j, (int64_t)tmp);
                    break;
                }
            case OBJECT_UINT64:
                {
                    uint64_t tmp = va_arg(argptr, unsigned long long);
                    val.push(j, (uint64_t)tmp);
                    break;
                }
            case OBJECT_UINT32:
                {
                    unsigned int tmp = va_arg(argptr, uint32_t);
                    val.push(j, (uint32_t)tmp);
                    break;
                }

            case OBJECT_BOOL:
                {
                    char tmp = va_arg(argptr, int);
                    val.push(j, (bool)tmp);
                    break;
                }
            case OBJECT_INT8:
                {
                    char tmp = va_arg(argptr, int);
                    val.push(j, (int8_t)tmp);
                    break;
                }
            case OBJECT_UINT8:
                {
                    unsigned char tmp = va_arg(argptr, unsigned int);
                    val.push(j, (uint8_t)tmp);
                    break;
                }
            case OBJECT_INT16:
                {
                    short tmp  = va_arg(argptr, int);
                    val.push(j, (int16_t)tmp);
                    break;
                }
            case OBJECT_UINT16:
                {
                    unsigned short tmp = va_arg(argptr, unsigned int);
                    val.push(j, (uint16_t)tmp);
                    break;
                }
            case OBJECT_FLOAT:
                {
                    float tmp = va_arg(argptr, double);
                    val.push(j, (float)tmp);
                    break;
                }
            case OBJECT_DOUBLE:
                {
                    double tmp = va_arg(argptr, double);
                    val.push(j, (double)tmp);
                    break;
                }

            default: TT_WARN_LOG("error type %d", (int)config->types[j]); return false;
        }
    }
    va_end(argptr);
    if (!table->insert(&val)) {
        TT_WARN_LOG("insert item error");
        return false;
    }
    return true;
}



long long MemTable::insertSerial(TableName name,  ...) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    const TableConfig* config = table->getConfig();
    va_list argptr;
    RowItemValue2 val;
    int columns = config->column_num;
    va_start(argptr, name);
    for (int j = 0; j < columns; j++) {
         switch (config->types[j])
        {
            case OBJECT_INT32:
                {
                    int tmp = va_arg(argptr, int32_t);
                    val.push(j, (int32_t)tmp);
                    break;
                }
            case OBJECT_STRING:
                {
                    const char* str = va_arg(argptr, char*);
                    //printf("str %s\n", str);
                    if (config->limits != NULL && strlen(str) > config->limits[j]) {
                        TT_WARN_LOG("string is too long");
                        return false;
                    }
                    val.push(j, str);
                    break;
                }
            case OBJECT_INT64:
                {
                    int64_t tmp = va_arg(argptr, long long);
                    val.push(j, (int64_t)tmp);
                    break;
                }
            case OBJECT_UINT64:
                {
                    uint64_t tmp = va_arg(argptr, unsigned long long);
                    val.push(j, (uint64_t)tmp);
                    break;
                }
            case OBJECT_UINT32:
                {
                    unsigned int tmp = va_arg(argptr, uint32_t);
                    val.push(j, (uint32_t)tmp);
                    break;
                }

            case OBJECT_BOOL:
                {
                    char tmp = va_arg(argptr, int);
                    val.push(j, (bool)tmp);
                    break;
                }
            case OBJECT_INT8:
                {
                    char tmp = va_arg(argptr, int);
                    val.push(j, (int8_t)tmp);
                    break;
                }
            case OBJECT_UINT8:
                {
                    unsigned char tmp = va_arg(argptr, unsigned int);
                    val.push(j, (uint8_t)tmp);
                    break;
                }
            case OBJECT_INT16:
                {
                    short tmp  = va_arg(argptr, int);
                    val.push(j, (int16_t)tmp);
                    break;
                }
            case OBJECT_UINT16:
                {
                    unsigned short tmp = va_arg(argptr, unsigned int);
                    val.push(j, (uint16_t)tmp);
                    break;
                }
            case OBJECT_FLOAT:
                {
                    float tmp = va_arg(argptr, double);
                    val.push(j, (float)tmp);
                    break;
                }
            case OBJECT_DOUBLE:
                {
                    double tmp = va_arg(argptr, double);
                    val.push(j, (double)tmp);
                    break;
                }

            default: TT_WARN_LOG("error type %d", (int)config->types[j]); return -1;
        }
    }
    va_end(argptr);
    TinyTableItem* item;
    if (!table->insert(&val, false, &item)) {
        TT_WARN_LOG("insert item error");
        return -1;
    }
    long long ret = 0;
    if (table->getConfig()->serial_col != -1) {
        ret = item->get<long long>(table->getConfig(), table->getConfig()->serial_col);
    }
    return ret;
}

bool MemTable::dump_sort(TableName name, TableIndex index) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return false;
    }
    return table->dump_sort(&index);
}

bool MemTable::dump_sort(SingleTable* table, TableIndex* index) {
    return table->dump_sort(index);
}

int  MemTable::rank(TableName name, RowItemValue unique, TableIndex sort) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    return table->rank(&unique, &sort);
}

int  MemTable::rank(SingleTable* table, RowItemValue* unique, TableIndex* sort) {
    return table->rank(unique, sort);
}

int MemTable::expire(TableName name) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    return table->expire();
}

int MemTable::expire() {
    //AtomicHashmap<int, SingleTable*>::iterator iter = _tables->begin();
    //AtomicHashmap<int, SingleTable*>::iterator end = _tables->end();
    int ret = 0;
    //for (; iter != end; ++iter) {
    int i = 0;
    for (; i < MAX_TABLE_NUM; ++i) {
        if (_tables[i] == NULL) {break;}
        if (0 != _tables[i]->expire()) {
            TT_WARN_LOG("table %d expire error", i);
        } else {
            ++ret;
        }
    }
    for (int j = MAX_TABLE_NUM - 1; j > i; --j) {
        if (_tables[j] == NULL) {break;}
        if (0 != _tables[j]->expire()) {
            TT_WARN_LOG("table %d expire error", j);
        } else {
            ++ret;
        }
    }
    return ret;
}

int   MemTable::size(TableName name) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    return table->size();
}

int   MemTable::size(SingleTable* table) {
    return table->size();
}

int   MemTable::size(TableName name, RowItemValue value) {
    SingleTable* table = getTable(name);
    if (table == NULL) {
        TT_WARN_LOG("name(%d %s) has no table", name.id, name.name.c_str());
        return -1;
    }
    return table->size(&value);
}

int   MemTable::size(SingleTable* table, RowItemValue* value) {
    return table->size(value);
}


bool  MemTable::tinytable_init(bool addlog, int ticks, runfunc* func, void* func_arg) {
    if (MemTable::global_timer != NULL) {
        return true;
    }
    if (!TlsBuffer::tls_init(MAX_BUFFER_LEN)) {
        TT_WARN_LOG("init buffer error");
        return false;
    }
    if (!AtomicTimer::global_init(AtomicTimer::TIMER_LAZY, ticks)) {
        TT_FATAL_LOG("timer galobal init error");
        return false;
    }
    MemTable::global_timer = new(std::nothrow) AtomicTimer();
    if (MemTable::global_timer == NULL) {
        TT_WARN_LOG("create tinytable timer error");
        return false;
    }
    if (!MemTable::global_timer->init(true, func, func_arg, 5)) {
        TT_FATAL_LOG("init timer error");
        delete MemTable::global_timer;
        MemTable::global_timer = NULL;
        return false;
    }
    return true;
}

bool MemTable::tinytable_destroy() {
    if (MemTable::global_timer != NULL) {
        delete MemTable::global_timer;
        MemTable::global_timer = NULL;
    }
    return AtomicTimer::global_destroy();
}

bool MemTable::addTimer(uint64_t us_time, timer_callback func, uint64_t interval, int round, uint64_t key, uint64_t* newKey) {
    return MemTable::global_timer->addTimer(us_time, func, interval, round, key, newKey);
}
bool MemTable::addFixedTimer(int wday, int hour, int min, int sec, timer_callback func, uint64_t interval, int round, uint64_t key, uint64_t* newKey) {
    return MemTable::global_timer->addFixedTimer(wday, hour, min, sec, func, interval, round, key, newKey);
}
bool MemTable::addHourlyTimer(int min, int sec, timer_callback func, uint64_t key, uint64_t* newKey) {
    return MemTable::global_timer->addHourlyTimer(min, sec, func, key, newKey);
}
bool MemTable::addDailyTimer(int hour, int min, int sec, timer_callback func, uint64_t key, uint64_t* newKey) {
    return MemTable::global_timer->addDailyTimer(hour, min, sec, func, key, newKey);
}
bool MemTable::addWeeklyTimer(int wday, int hour, int min, int sec, timer_callback func, uint64_t key, uint64_t* newKey) {
    return MemTable::global_timer->addWeeklyTimer(wday, hour, min, sec, func, key, newKey);
}
bool MemTable::delTimer(uint64_t key) {
    return MemTable::global_timer->delTimer(key);
}


}

