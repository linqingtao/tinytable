#include "config.h"

int TableConfig::global_table_id = MAX_TABLE_NUM;


TableIDConfig::TableIDConfig(TableIDConfig& config) {
    AtomicHashmap<int, std::string>::iterator iter = config.table_id_name_index.begin();
    AtomicHashmap<int, std::string>::iterator iter_end = config.table_id_name_index.end();
    for (; iter != iter_end; ++iter) {
        table_id_name_index.insert(iter->first, iter->second);
    }
    AtomicHashmap<std::string, int>::iterator it = config.table_name_id_index.begin();
    AtomicHashmap<std::string, int>::iterator it_end = config.table_name_id_index.end();
    for (; it != it_end; ++it) {
        table_name_id_index.insert(it->first, it->second);
    }
}

TableIDConfig& TableIDConfig::operator=(TableIDConfig& config) {
    AtomicHashmap<int, std::string>::iterator iter = config.table_id_name_index.begin();
    AtomicHashmap<int, std::string>::iterator iter_end = config.table_id_name_index.end();
    for (; iter != iter_end; ++iter) {
        table_id_name_index.insert(iter->first, iter->second);
    }
    AtomicHashmap<std::string, int>::iterator it = config.table_name_id_index.begin();
    AtomicHashmap<std::string, int>::iterator it_end = config.table_name_id_index.end();
    for (; it != it_end; ++it) {
        table_name_id_index.insert(it->first, it->second);
    }
    return *this;
}

bool TableIDConfig::addTable(int id, std::string name) {
    AtomicHashmap<int, std::string>::iterator iter = table_id_name_index.find(id);
    if (iter != table_id_name_index.end()) {
        TT_DEBUG_LOG("id conflict: id %d table %s conflict with id %d table %s", id, name.c_str(), id, iter->second.c_str());
        return false;
    }
    AtomicHashmap<std::string, int>::iterator it = table_name_id_index.find(name);
    if (it != table_name_id_index.end()) {
        TT_WARN_LOG("name conflict: id %d table %s conflict with id %d table %s", id, name.c_str(), it->second, name.c_str());
        return false;
    }
    if (!table_id_name_index.insert(id, name)) {
        TT_WARN_LOG("add id %d name %s error", id, name.c_str());
        return false;
    }
    if (!table_name_id_index.insert(name, id)) {
        TT_WARN_LOG("add id %d name %s error", id, name.c_str());
        table_id_name_index.Remove(id);
        return false;
    }
    return true;
}

bool TableIDConfig::delTable(int id) {
    AtomicHashmap<int, std::string>::iterator iter = table_id_name_index.find(id);
    if (iter != table_id_name_index.end()) {
        TT_WARN_LOG("del table id %d not exist", id);
        return -1;
    }
    table_name_id_index.Remove(iter->second);
    if (!table_id_name_index.Remove(id)) {
        return -1;
    }
    return 0;
}

std::string TableIDConfig::getName(int id) const {
    AtomicHashmap<int, std::string>::iterator iter = table_id_name_index.find(id);
    if (iter == table_id_name_index.end()) {
        return "";
    }
    return iter->second;
}

int TableIDConfig::getID(std::string name) const {
    AtomicHashmap<std::string, int>::iterator it = table_name_id_index.find(name);
    if (it == table_name_id_index.end()) {
        return -1;
    }
    return it->second;
}


TableConfig::TableConfig()
: table_id(-1)
, index(NULL)
, types(NULL)
, limits(NULL)
, column_num(0)
, expired_time(-1)
, servercol(-1)
, table_name("")
, column_names(NULL)
, table_sign(0)
, column_name_signs(NULL)
, serial_col(-1)
, string_cnt(0)
, column_map(NULL)
, load_type(1)
, firstsource(1)
, table_type(2)
, updatetype(0) {}

int TableConfig::generateID() {
    return __sync_sub_and_fetch(&TableConfig::global_table_id, 1);
}

TableConfig::TableConfig(const TableConfig& config)
: table_id(config.table_id)
, index(NULL)
, types(NULL)
, limits(NULL)
, column_num(config.column_num)
, expired_time(config.expired_time)
, servercol(config.servercol)
, table_name(config.table_name)
, column_names(NULL)
, table_sign(config.table_sign)
, column_name_signs(NULL) {
    if (table_id == -1) {
        table_id = __sync_sub_and_fetch(&TableConfig::global_table_id, 1);
    }
    //types index
    types = new char[column_num];
    index = new short[column_num];
    memcpy(types, config.types, sizeof(char) * column_num);
    memcpy(index, config.index, sizeof(short) * column_num);
    // column map
    column_map = new AtomicHashmap<uint64_t, int>;
    AtomicHashmap<uint64_t, int>::iterator iter = config.column_map->begin();
    AtomicHashmap<uint64_t, int>::iterator end = config.column_map->end();
    for (; iter != end; ++iter) {
        column_map->insert(iter->first, iter->second);
    }
    ListSet<short>::iterator iter_queue = config.queue_index_keys.begin();
    ListSet<short>::iterator iter_queue_end = config.queue_index_keys.begin();
    for (; iter_queue != iter_queue_end; ++iter_queue) {
        queue_index_keys.insert(*iter_queue);
    }
    // names
    if (config.column_names != NULL) {
        column_names = new std::string[column_num];
        column_name_signs = new uint64_t[column_num];
        for (int j = 0; j < column_num; j++) {
            column_names[j]=config.column_names[j];
            column_name_signs[j]=config.column_name_signs[j];
        }
    }
    // limits
    if (config.limits != NULL) {
        limits = new int[column_num];
        memcpy(limits, config.limits, sizeof(int) * column_num);
    }
    // load type
    load_type = config.load_type;
    updatetype = config.updatetype;
    firstsource = config.firstsource;
    table_type = config.table_type;
    primary_keys = config.primary_keys;
    unique_keys = config.unique_keys;
    sort_keys = config.sort_keys;
    queue_keys = config.queue_keys;
    triggers = config.triggers;
    serial_col = config.serial_col;
    string_cnt = config.string_cnt;
}





TableConfig::TableConfig(const char* name, int columns, char* column_types, int* limits_, std::string* col_name, int id)
: table_id(id)
, index(NULL)
, types(NULL)
, limits(NULL)
, column_num(columns)
, table_name(name)
, column_names(NULL)
, column_name_signs(NULL)
, serial_col(-1)
, string_cnt(0) {
    if (id == -1) {
        table_id = __sync_sub_and_fetch(&TableConfig::global_table_id, 1);
    }
    types = new char[columns];
    index = new short[columns];
    table_sign = get_sign64(table_name.c_str(), table_name.length());
    column_map = new AtomicHashmap<uint64_t, int>;
    if (col_name != NULL) {
        column_names = new std::string[columns];
        column_name_signs = new uint64_t[columns];
        for (int j = 0; j < columns; j++) {
            column_names[j]=col_name[j];
            column_name_signs[j] = get_sign64(col_name[j].c_str(), col_name[j].length());
            column_map->insert(column_name_signs[j],j);
        }
    }
    memset(index, 0, sizeof(short) * columns);
    if (limits_ != NULL) {
        limits = new int[columns];
        memcpy(limits, limits_, sizeof(int) * columns);
    }

    for (int i = 0; i < columns; i++) {
        types[i] = column_types[i];
    }
    int prev = 0;
    // make it -6 as when there is no string
    // -6 + 8 = 2 will be the start
    int string_end = -6;
    for (int i = 0; i < columns; i++) {
        if (column_types[i] == OBJECT_STRING) {
            if (string_cnt == 0) {
                index[i] = 2;
            } else {
                index[i] = index[prev] + 8;
            }
            prev = i;
            ++string_cnt;
            string_end = index[i];
        }
    }
    prev = 0;
    int prev_len = 0;
    int cnt = 0;
    for (int i = 0; i < columns; i++) {
        if (column_types[i] == OBJECT_STRING) { continue;}
        if (cnt == 0) {
            index[i] = string_end + 8;
        } else {
            index[i] = index[prev] + prev_len;
        }
        switch (column_types[i])
        {
            case OBJECT_BOOL:
            case OBJECT_UINT8:
            case OBJECT_INT8: prev_len = 1;break;
            case OBJECT_UINT16:
            case OBJECT_INT16: prev_len = 2;break;
            case OBJECT_UINT32:
            case OBJECT_INT32:
            case OBJECT_FLOAT: prev_len = 4;break;
            case OBJECT_UINT64:
            case OBJECT_INT64:
            case OBJECT_DOUBLE:prev_len = 8;break;
            default: prev_len = 8;break;
        }
        prev = i;
        ++cnt;
    }
    load_type = 1;
    updatetype = 0;
    firstsource = 1;
    table_type = 2;
    servercol = -1;
}

const char * TableConfig::columnIDToName(int id) const {
    return (column_names[id].c_str());
}
int  TableConfig::columnNameToID(const char *name) const {
    AtomicHashmap<uint64_t, int>::iterator iter= column_map->find(get_sign64(name,strlen(name)));
    if(iter!=column_map->end())
        return iter->second;
    return -1; //means no column id found	
}
bool TableConfig::isPrimary(int index) const {
    return (primary_keys.find(index) != primary_keys.end());
}
bool TableConfig::getIndexColumnIDs(std::vector<string> * key_string,int * column_ids)
{
    std::vector<string>::iterator iter;
    int i=0,id;
    for(iter = key_string->begin();iter!=key_string->end();iter++)
    {
        id = columnNameToID(iter->c_str());
        if(id == 0 )
            return false;// no column id match this key
        column_ids[i++] = id;	
    }
    return true;
}

TableConfig::~TableConfig() {
    if (NULL != index) {
        delete index;
        index = NULL;
    }
    if (NULL != types) {
        delete types;
        types = NULL;
    }
    if (NULL != column_names) {
        delete[] column_names;
        column_names = NULL;
    }
    if (NULL != column_name_signs) {
        delete column_name_signs;
        column_name_signs = NULL;
    }
}

bool TableConfig::addPrimary(TableIndex primary) {
    if (primary_keys.size() > 0) {
        TT_WARN_LOG("config has primary");
        return false;
    }
    primary_keys = primary.index;
    return true;
}

bool TableConfig::addUnique(TableIndex unique) {
    std::vector<UniqueIndex>::iterator iter = unique_keys.begin();
    std::vector<UniqueIndex>::iterator end = unique_keys.end();
    for (; iter != end; ++iter) {
        if (iter->keys == unique.index) {
            TT_WARN_LOG("unique index exist");
            return false;
        }
    }
    UniqueIndex unique_index;
    unique_index.keys = unique.index;
    unique_keys.push_back(unique_index);
    return true;
}

bool TableConfig::addCluster(TableIndex unique, TableIndex cluster) {
    std::vector<ClusterIndex>::iterator iter = cluster_keys.begin();
    std::vector<ClusterIndex>::iterator end = cluster_keys.end();
    for (; iter != end; ++iter) {
        if (iter->unique_keys == unique.index && iter->cluster_keys == cluster.index) {
            TT_WARN_LOG("cluster index exist");
            return false;
        }
    }
    ClusterIndex cluster_index;
    cluster_index.unique_keys = unique.index;
    cluster_index.cluster_keys = cluster.index;
    cluster_keys.push_back(cluster_index);
    return true;
}

bool TableConfig::addSerialCol(short col) {
    serial_col = col;
    return true;
}

bool TableConfig::addQueue(TableIndex unique, short cnt_col, bool desc, int max_size) {
    std::vector<QueueIndex>::iterator iter = queue_keys.begin();
    std::vector<QueueIndex>::iterator end = queue_keys.end();
    ListSet<short> q_index;
    q_index.insert(cnt_col);
    for (; iter != end; ++iter) {
        if (iter->unique_keys == unique.index && iter->queue_keys == q_index) {
            TT_WARN_LOG("cluster queue index exist");
            return false;
        }
    }
    QueueIndex queue_index;
    queue_index.unique_keys = unique.index;
    queue_index.queue_keys = q_index;
    queue_index.desc = desc;
    queue_index.max_size = max_size;
    queue_keys.push_back(queue_index);
    queue_index_keys.insert(cnt_col);
    return true;
}

bool TableConfig::addSort(bool desc, TableIndex unique, TableIndex sort) {
    std::vector<SortIndex>::iterator iter = sort_keys.begin();
    std::vector<SortIndex>::iterator end = sort_keys.end();
    for (; iter != end; ++iter) {
        if (iter->unique_keys == unique.index && iter->sort_keys == sort.index) {
            TT_WARN_LOG("sort index exist");
            return false;
        }
    }
    bool is_unique = false;
     // check if unique is unique
    if (primary_keys == unique.index) {
        is_unique = true;
    } else {
        std::vector<UniqueIndex>::iterator iter_unique = unique_keys.begin();
        std::vector<UniqueIndex>::iterator end_unique = unique_keys.end();
        for (; iter_unique != end_unique; ++iter_unique) {
            if (iter_unique->keys == unique.index) {
                is_unique = true;
                break;
            }
        }
    }
    if (!is_unique) {
        TT_WARN_LOG("input unique is not unique");
        return false;
    }
    SortIndex sort_index;
    sort_index.desc = desc;
    sort_index.unique_keys = unique.index;
    sort_index.sort_keys = sort.index;
    sort_keys.push_back(sort_index);
    return true;
}

bool TableConfig::isQueue(short col) {
    return (queue_index_keys.find(col) != queue_index_keys.end());
}





