#ifndef _TINYTABLE_CONFIG_H_
#define _TINYTABLE_CONFIG_H_
#include <stdio.h>
#include <stdint.h>
#include <string>
#include <string.h>
#include <stdlib.h>
#include <vector>
#include <set>
#include <limits>
#include "atomiccache.h"
#include "atomicbitmap.h"
#include "sign.h"
#include "common.h"
#include "locks.h"
#include "atomichashmap.h"
#include "tinytable_log_utils.h"
#include "item_array.h"
#include "list_set.h"

#define MAX_TABLE_NUM 5000


struct TableIDConfig {
    mutable AtomicHashmap<int, std::string> table_id_name_index;
    mutable AtomicHashmap<std::string, int> table_name_id_index;
    bool addTable(int id, std::string name);
    bool delTable(int id);
    std::string getName(int id) const;
    int getID(std::string name) const;
    TableIDConfig(TableIDConfig& config);
    TableIDConfig& operator=(TableIDConfig& config);
    TableIDConfig(){}
};

enum {LOAD_ALL, LOAD_WHEN_USED,};

enum {
    TABLE_TYPE_DB,
    TABLE_TYPE_CACHE,
    TABLE_TYPE_LOCAL,
};

enum {
    CONN_TABLE,
    CONN_CACHE,
};

enum {
    TRIGGER_OPS_SET,
    TRIGGER_OPS_ADD,
    TRIGGER_OPS_DEC,
    TRIGGER_OPS_DEL,
};

enum {
    UPDATE_LAZY,
    UPDATE_REALTIME,
    UPDATE_SINGLE,
};

struct TableConfig {
    // table id every table has a unque id
    int    table_id;
    // the index
    short* index;
    char* types;
    int*  limits;
    int   column_num;
    int  expired_time;
    short servercol;
    std::string table_name;
    std::string* column_names;
    uint64_t table_sign;
    uint64_t* column_name_signs;
    short serial_col;
    unsigned short string_cnt;
	//use to get column id from cloumn name
	AtomicHashmap<uint64_t, int>*  column_map;
    // these params are used for string
    // in order to make string lockfree
    // we add three cache to store the deleted string
    // if we need to new string first pop from free cache
   // AtomicCache<std::string*>* free_cache;
    //AtomicCache<TableItem_t*>* free_item;

    /*************************this item is just used as settings********************************/
    // table params
    // same with xml
    char load_type; // 0 load all  1 load when used
    char firstsource; // 0 db 1 cache
    char table_type; // 0 db 1 cache 2 single
    char updatetype; // 0 delay flush 1 realtime
    // primary
    ListSet<short>  primary_keys;      
    // unique
    struct UniqueIndex {
        ListSet<short> keys;
    };
    std::vector<UniqueIndex>  unique_keys;
    struct ClusterIndex {
        ListSet<short> unique_keys;
        ListSet<short> cluster_keys;
    };
    std::vector<ClusterIndex> cluster_keys;
    struct SortIndex {
        bool desc;
        ListSet<short> unique_keys;
        ListSet<short> sort_keys;
    };
    std::vector<SortIndex> sort_keys;
    struct QueueIndex {
        bool desc;
        int max_size;
        ListSet<short> unique_keys;
        ListSet<short> queue_keys;
    };
    std::vector<QueueIndex> queue_keys;
    ListSet<short> queue_index_keys;
    // triggers
    struct TriggerFunc {
        char ops_type;
        short col;
        ItemValue val;
        ItemValue min;
        ItemValue max;
    };
    struct TimerTrigger {
        TimerTrigger()
        : type(0)
        , interval(0) {}
        std::string name;
        // 0 realtime 1 lazy
        int type;
        short time_col;
        int interval_ms;
        int weekday;
        int hour;
        int min;
        int sec;
        int interval;
        std::vector<TriggerFunc> funcs;
    };
    std::vector<TimerTrigger> triggers;
    // end triggers
    /*************************items above are just used as settings********************************/
    static int global_table_id;

    // constructor
    TableConfig(const char* name, int columns, char* column_types, int* limits_ = NULL, std::string* col_name=NULL, int id = -1);
    TableConfig();
    TableConfig(const TableConfig& config);
	const char * columnIDToName(int id) const;
	int  columnNameToID(const char *name) const;
    bool isPrimary(int index) const;
    static int  generateID();
	bool getIndexColumnIDs(std::vector<string> * key_string,int * column_ids);
    bool addPrimary(TableIndex primary);
    bool addUnique(TableIndex unique);
    bool addCluster(TableIndex unique, TableIndex cluster);
    bool addQueue(TableIndex unique, short cnt_col, bool desc = false, int max_size = -1);
    bool addSort(bool desc, TableIndex unique, TableIndex sort);
    bool addSerialCol(short col);
    bool isQueue(short col);
    ~TableConfig();
};








#endif

